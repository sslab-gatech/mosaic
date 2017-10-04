#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "edge-processor.h"
#endif
#pragma once

#include <pthread.h>

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, bool is_weighted>
  EdgeProcessor<APP, TVertexType, is_weighted>::EdgeProcessor(
      const config_edge_processor_t& config)
      : shutdown_(false), config_(config),
        tile_reader_progress_(config_.count_tile_readers),
        fake_block_id_counter_(config_.count_tile_processors) {
    // init barrier for each iteration, this only for selective scheduling
    if (config_.use_selective_scheduling) {
      pthread_barrier_init(&barrier_tile_readers_, NULL,
                           config_.count_tile_readers);
    }
    pthread_barrier_init(&barrier_tile_processors_, NULL,
                         config_.count_tile_processors);
  }

  template <class APP, typename TVertexType, bool is_weighted>
  int EdgeProcessor<APP, TVertexType, is_weighted>::init() {
    // init fd of tiles-file
    std::string tiles_file_name = core::getEdgeTileFileName(config_, 0);
    tiles_fd_ = util::openFileDirectly(tiles_file_name);

    // init tile_stats_
    std::string global_tile_stats_file_name =
        core::getGlobalTileStatsFileName(config_, 0);

    int count_tiles_for_mic =
        core::countTilesPerMic(config_, config_.mic_index);

    tile_stats_ = new tile_stats_t[count_tiles_for_mic];

    size_t size_tile_stats = sizeof(tile_stats_t) * count_tiles_for_mic;
    util::readDataFromFile(global_tile_stats_file_name, size_tile_stats,
                           tile_stats_);

    // pre-calculate the individual tile-offsets
    tile_offsets_ = new size_t[count_tiles_for_mic];
    tile_offsets_[0] = 0;
    for (int i = 1; i < count_tiles_for_mic; ++i) {
      // offset is the offset of the previous block plus the size of the
      // previous block:
      tile_stats_t tile_stats = tile_stats_[i - 1];

      // step 2: calculate required space to fetch the tile
      size_t size_edge_src_block =
          sizeof(local_vertex_id_t) * tile_stats.count_edges;

      size_t size_edge_tgt_block = size_edge_src_block;

      // if using rle, take the tgt-block as the size times the
      // vertex-count-struct
      if (tile_stats.use_rle) {
        size_edge_tgt_block =
            sizeof(vertex_count_t) * tile_stats.count_vertex_tgt;
      }

      // only include weight-block if necessary
      size_t size_edge_weights_block = 0;
      if (is_weighted) {
        size_edge_weights_block = sizeof(float) * tile_stats.count_edges;
      }

      size_t size_edge_block = sizeof(edge_block_t) + size_edge_src_block +
                               size_edge_tgt_block + size_edge_weights_block;

      size_t size_rb_block = int_ceil(size_edge_block, PAGE_SIZE);
      tile_offsets_[i] = tile_offsets_[i - 1] + size_rb_block;
    }

    // create ring buffers
    // - first, create processed ring buffer
    int port = config_.port;
    int rc;
#if defined(MOSAIC_HOST_ONLY)
    rc = ring_buffer_create(config_.processed_rb_size, L1D_CACHELINE_SIZE,
                            RING_BUFFER_BLOCKING, NULL, NULL, &processed_rb_);
    if (rc) {
      scalable_graphs::util::die(1);
    }

    // - second, create tiles data ring buffer
    // 1) the vertex-data associated with a tile
    // 2) the active-array for that tile
    // 3) the degree-information for that tile
    rc = ring_buffer_create(config_.host_tiles_rb_size, L1D_CACHELINE_SIZE,
                            RING_BUFFER_BLOCKING, NULL, NULL, &tiles_rb_);
    if (rc) {
      scalable_graphs::util::die(1);
    }
#else
    rc = ring_buffer_scif_create_master(
        config_.processed_rb_size, L1D_CACHELINE_SIZE, RING_BUFFER_BLOCKING,
        RING_BUFFER_SCIF_PRODUCER, NULL, NULL, &processed_rb_);
    if (rc)
      scalable_graphs::util::die(1);
    //   then, wait for shadow connection asynchronously
    ring_buffer_scif_wait_for_shadow(&processed_rb_, port, 0);
    port += 10;

    // - second, create tiles data ring buffer
    // 1) the vertex-data associated with a tile
    // 2) the active-array for that tile
    // 3) the degree-information for that tile
    rc = ring_buffer_scif_create_master(
        config_.host_tiles_rb_size, L1D_CACHELINE_SIZE, RING_BUFFER_BLOCKING,
        RING_BUFFER_SCIF_CONSUMER, NULL, NULL, &tiles_rb_);
    if (rc)
      scalable_graphs::util::die(1);
    //   then, wait for shadow connection asynchronously
    ring_buffer_scif_wait_for_shadow(&tiles_rb_, port, 0);
#endif

    // - local tiles ring buffer: tile static data, i.e., edge data
    //   change alignsize for better performance - 512 to 4K
    rc = ring_buffer_create(config_.read_tiles_rb_size, PAGE_SIZE,
                            RING_BUFFER_BLOCKING, NULL, NULL, &local_tiles_rb_);
    if (rc)
      scalable_graphs::util::die(1);

    // initialize tile offset table
    tiles_offset_table_.data_info =
        (pointer_offset_t<edge_block_t, tile_data_edge_engine_t>*)mmap(
            NULL,
            sizeof(tiles_offset_table_.data_info[0]) * config_.count_tiles,
            PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (tiles_offset_table_.data_info == MAP_FAILED) {
      scalable_graphs::util::die(1);
    }

    // if in selective-scheduling-mode, also connect to the two ringbuffers,
    // sharing the tiles-active-array as well as the signaling-ring-buffer to
    // wake up all waiting threads
    sg_log("selective scheduling %s \n",
           config_.use_selective_scheduling ? "true" : "false");
    if (config_.use_selective_scheduling) {
      // add +1 to account for uneven splitting up of last few tiles among
      // edge-engines
      size_tile_active_ =
          size_bool_array(((config_.count_tiles / config_.count_edge_processors) + 1));

#if defined(MOSAIC_HOST_ONLY)
      rc = ring_buffer_create(size_tile_active_,
                              L1D_CACHELINE_SIZE,
                              RING_BUFFER_BLOCKING,
                              NULL,
                              NULL,
                              &active_tiles_rb_);
      if (rc) {
        scalable_graphs::util::die(1);
      }
#else
      port += 10;
      rc = ring_buffer_scif_create_master(
          size_tile_active_, L1D_CACHELINE_SIZE, RING_BUFFER_BLOCKING,
          RING_BUFFER_SCIF_CONSUMER, NULL, NULL, &active_tiles_rb_);
      if (rc) {
        scalable_graphs::util::die(1);
      }
      //   then, wait for shadow connection asynchronously
      ring_buffer_scif_wait_for_shadow(&active_tiles_rb_, port, 0);
#endif

      /*get this from vertex processor */
      tile_active_ = new char[size_tile_active_];
    }

    return 0;
  }

  template<class APP, typename TVertexType, bool is_weighted>
  void EdgeProcessor<APP, TVertexType, is_weighted>::initActiveTiles() {
    if (config_.use_selective_scheduling) {
      /*init active tiles */
      updateActiveTiles();
    }
  }

  template <class APP, typename TVertexType, bool is_weighted>
  size_t EdgeProcessor<APP, TVertexType, is_weighted>::updateActiveTiles() {
    sg_log("Waiting update active tiles %s\n", "");
    ring_buffer_req_t req_active_tiles;
    ring_buffer_get_req_init(&req_active_tiles, BLOCKING);
#if defined(MOSAIC_HOST_ONLY)
    ring_buffer_get(active_tiles_rb_, &req_active_tiles);
#else
    ring_buffer_scif_get(&active_tiles_rb_, &req_active_tiles);
#endif
    sg_rb_check(&req_active_tiles);

    active_tiles_t* active_tiles = (active_tiles_t*)req_active_tiles.data;

    // copy from ring-buffer, set done immediately
    memcpy(tile_active_, active_tiles->active_tiles, size_tile_active_);
    size_t count_active_tiles = active_tiles->count_active_tiles;

#if defined(MOSAIC_HOST_ONLY)
    ring_buffer_elm_set_done(active_tiles_rb_, req_active_tiles.data);
#else
    ring_buffer_scif_elm_set_done(&active_tiles_rb_, req_active_tiles.data);
#endif

    sg_print("active tiles info received\n");

    return count_active_tiles;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  EdgeProcessor<APP, TVertexType, is_weighted>::~EdgeProcessor() {
    // destroy tile offset table
    munmap(tiles_offset_table_.data_info,
           sizeof(tiles_offset_table_.data_info[0]) * config_.count_tiles);

    // destroy ring buffers
#if defined(MOSAIC_HOST_ONLY)
    ring_buffer_destroy(processed_rb_);
    ring_buffer_destroy(tiles_rb_);
#else
    ring_buffer_scif_destroy_master(&processed_rb_);
    ring_buffer_scif_destroy_master(&tiles_rb_);
#endif

    ring_buffer_destroy(local_tiles_rb_);

    // destroy arrays
    delete[] tile_stats_;
    delete[] tile_offsets_;

    /*handle selective scheduling stuffs */
    if (config_.use_selective_scheduling) {
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_destroy(active_tiles_rb_);
#else
      ring_buffer_scif_destroy_master(&active_tiles_rb_);
#endif
      delete[] tile_active_;
    }

    close(tiles_fd_);
  }

  template <class APP, typename TVertexType, bool is_weighted>
  int EdgeProcessor<APP, TVertexType, is_weighted>::start() {
    // launch perfmon
    if (config_.do_perfmon) {
      perfmon_.start();
    }

    // Launch PerfEventManager.
    if (config_.enable_perf_event_collection) {
      pe::PerfEventManager::getInstance(config_)->start();
    }

    scalable_graphs::util::cpu_id_t cpu_id(0, 0, 0);
    if (config_.run_on_mic) {
      // When running on the MIC, simply start from the offset CPU:
      cpu_id.socket = 0;
      cpu_id.pcpu = CORE_OFFSET;
      cpu_id.smt = util::cpu_id_t::ANYWHERE;
    } else {
      int socket = config_.mic_index % util::NUM_SOCKET;
      cpu_id.socket = socket;
      cpu_id.pcpu = util::cpu_id_t::ANYWHERE;
      cpu_id.smt = util::cpu_id_t::ANYWHERE;
      // When running on the host, take the vertexprocessor and the other edge
      // engines into account:
      // int socket = config_.mic_index % util::NUM_SOCKET;
      // int threads_vertex_processor_per_edge_engine =
      //     config_.count_index_readers + config_.count_vertex_reducers +
      //     config_.count_vertex_fetchers;
      // int threads_per_edge_engine =
      //     config_.count_tile_readers + config_.count_tile_processors;

      // int edge_engines_per_socket = config_.count_edge_processors / util::NUM_SOCKET;

      // int preceding_threads =
      //     threads_vertex_processor_per_edge_engine * edge_engines_per_socket
      //     +
      //     threads_per_edge_engine * (config_.mic_index / util::NUM_SOCKET);

      // preceding_threads += config_.count_global_reducers / util::NUM_SOCKET;
      // if (config_.local_fetcher_mode == LocalFetcherMode::LFM_GlobalFetcher)
      // {
      //   preceding_threads += config_.count_global_fetchers /
      //   util::NUM_SOCKET;
      // }

      // cpu_id.socket = socket;
      // cpu_id.pcpu = CORE_OFFSET;
      // cpu_id.smt = 0;

      // for (int i = 0; i < preceding_threads; ++i) {
      //   cpu_id.nextNearest(config_.use_smt);
      // }
    }

    // int socket = config_.mic_index % util::NUM_SOCKET;
    // cpu_id.socket = socket;
    // cpu_id.pcpu = util::cpu_id_t::ANYWHERE;
    // cpu_id.smt = util::cpu_id_t::ANYWHERE;

    // launch tile readers
    for (int i = 0; i < config_.count_tile_readers; ++i) {
      thread_index_t thread_index;
      thread_index.count = config_.count_tile_readers;
      thread_index.id = i;

      auto* reader = new core::TileReader<APP, TVertexType, is_weighted>(
          *this, thread_index, &barrier_tile_readers_);

      reader->setAffinity(cpu_id);
      // Only pin on the Xeon Phi for now, not enough CPUs on the host.
      if (config_.run_on_mic) {
        cpu_id.nextNearest(config_.use_smt);
      }

      reader->start();
      reader->setName("Reader_" + std::to_string(config_.mic_index) + "_" +
                      std::to_string(i));
      threads_.push_back(reader);
    }

    // - in the in-memory mode, wait for everything to be loaded first:
    if (config_.in_memory_mode) {
      for (const auto& it : threads_) {
        it->join();
        delete (it);
      }
      sg_log2("Done reading\n");
      threads_.clear();
    }

    // launch tile processor
    for (int i = 0; i < config_.count_tile_processors; ++i) {
      thread_index_t thread_index;
      thread_index.count = config_.count_tile_processors;
      thread_index.id = i;

      auto* processor = new core::TileProcessor<APP, TVertexType, is_weighted>(
          *this, thread_index);

      processor->setAffinity(cpu_id);
      // Only pin on the Xeon Phi for now, not enough CPUs on the host.
      if (config_.run_on_mic) {
        cpu_id.nextNearest(config_.use_smt);
      }

      processor->start();
      processor->setName("Proc_" + std::to_string(config_.mic_index) + "_" +
                         std::to_string(i));
      threads_.push_back(processor);
    }
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void EdgeProcessor<APP, TVertexType, is_weighted>::join() {
    for (const auto& it : threads_) {
      it->join();
      delete (it);
    }

    if (config_.do_perfmon) {
      perfmon_.stop();
      perfmon_.join();
    }

    // Join PerfEventManager.
    if (config_.enable_perf_event_collection) {
      pe::PerfEventManager::getInstance(config_)->stop();
      sg_print("EventManager is exiting\n");
    }
  }
  template <class APP, typename TVertexType, bool is_weighted>
  void EdgeProcessor<APP, TVertexType, is_weighted>::shutdown() {
    shutdown_ = true;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  bool EdgeProcessor<APP, TVertexType, is_weighted>::isShutdown() {
    return shutdown_;
  }

  template<class APP, typename TVertexType, bool is_weighted>
  ringbuffer_config_t
  EdgeProcessor<APP, TVertexType, is_weighted>::getRingbufferConfig() {
    ringbuffer_config_t config;
    config.active_tiles_rb = active_tiles_rb_;
    config.response_rb = processed_rb_;
    config.tiles_data_rb = tiles_rb_;
    return config;
  }
}
}
