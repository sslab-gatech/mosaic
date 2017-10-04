#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "vertex-processor.h"
#endif
#pragma once

#include <sys/mman.h>

#include <util/perf-event/perf-event-manager.h>

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, typename TVertexIdType>
  VertexProcessor<APP, TVertexType, TVertexIdType>::VertexProcessor(
      VertexDomain<APP, TVertexType, TVertexIdType>& vd,
      const config_vertex_domain_t& config, int mic_id, int edge_engine_index)
      : vd_(vd), config_(config), mic_id_(mic_id),
        edge_engine_index_(edge_engine_index),
        fetcher_progress_(config_.count_vertex_fetchers),
        index_reader_progress_(config_.count_index_readers) {
    pthread_barrier_init(&barrier_readers_, NULL,
                         config_.count_vertex_fetchers);
    pthread_barrier_init(&fetchers_barrier_, NULL,
                         config_.count_vertex_fetchers);

  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  VertexProcessor<APP, TVertexType, TVertexIdType>::~VertexProcessor() {
#if defined(MOSAIC_HOST_ONLY)
    // destroy ring buffers
    ring_buffer_destroy(response_rb_);
    ring_buffer_destroy(tiles_data_rb_);
#else
    // destroy ring buffers
    ring_buffer_scif_destroy_shadow(&response_rb_);
    ring_buffer_scif_destroy_shadow(&tiles_data_rb_);
#endif
    ring_buffer_destroy(index_rb_);

    delete[] tile_stats_;
    delete[] tile_offsets_;

    if (config_.use_selective_scheduling) {
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_destroy(active_tiles_rb_);
#else
      ring_buffer_scif_destroy_shadow(&active_tiles_rb_);
#endif
      delete[] tile_active_current_;
      delete[] tile_active_next_;
    }

    close(meta_fd_);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexProcessor<APP, TVertexType, TVertexIdType>::initRingBuffers() {
    int rc;
#if defined(MOSAIC_HOST_ONLY)
    response_rb_ = config_.ringbuffer_configs[edge_engine_index_].response_rb;
    tiles_data_rb_ = config_.ringbuffer_configs[edge_engine_index_].tiles_data_rb;
#else
    // create ring buffers
    // - first, create and connect to response ring buffer
    int port = config_.port;

    // increment ID by 1 to run on mic, use 0 (host) otherwise!
    int node_id = config_.run_on_mic ? mic_id_ + 1 : 0;
    sg_dbg("Connecting to host %d at port %d\n", node_id, port);
    rc = ring_buffer_scif_create_shadow(port + 1, node_id, port, NULL, NULL,
                                        &response_rb_);
    if (rc) {
      scalable_graphs::util::die(1);
    }
    port += 10;

    // - then, create and connect to tiles data ring buffer
    rc = ring_buffer_scif_create_shadow(port + 1, node_id, port, NULL, NULL,
                                        &tiles_data_rb_);
    if (rc) {
      sg_log("Fail to create ring buffer\n [SG-LOG] NodeId:%d, Port : %d\n",
             node_id, port);
      scalable_graphs::util::die(1);
    }
#endif

    // if in selective-scheduling-mode, also connect to the two ringbuffers,
    // sharing the tiles-active-array as well as the signaling-ring-buffer to
    // wake up all waiting threads
    if (config_.use_selective_scheduling) {
#if defined(MOSAIC_HOST_ONLY)
      active_tiles_rb_ = config_.ringbuffer_configs[edge_engine_index_].active_tiles_rb;
#else
      port += 10;

      /*create ring buffer */
      rc = ring_buffer_scif_create_shadow(port + 1, node_id, port, NULL, NULL,
                                          &active_tiles_rb_);

      if (rc) {
        sg_log("Fail to create ring buffer for selective scheduling\n"
               "[SG-LOG] NodeId:%d, Port : %d\n",
               node_id, port);
        scalable_graphs::util::die(1);
      }

      sg_log("Ring buffer for selective scheduling connected\n"
             "[SG-LOG] NodeId:%d, Port : %d\n",
             node_id, port);
#endif
    }
    // create local index-buffer
    rc = ring_buffer_create(index_rb_size_, PAGE_SIZE, RING_BUFFER_BLOCKING,
                            NULL, NULL, &index_rb_);
    if (rc) {
      scalable_graphs::util::die(1);
    }
    sg_log("Connected to edge-engine %d on %d\n", edge_engine_index_, mic_id_);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexProcessor<APP, TVertexType, TVertexIdType>::allocate() {
    // init fd of tiles-file
    std::string meta_file_name =
        core::getEdgeTileIndexFileName(config_, edge_engine_index_);
    meta_fd_ = util::openFileDirectly(meta_file_name);

    // read tile_stats
    std::string global_tile_stats_file_name =
        core::getGlobalTileStatsFileName(config_, edge_engine_index_);
    int count_tiles_for_mic =
        core::countTilesPerMic(config_, edge_engine_index_);

    tile_stats_ = new tile_stats_t[count_tiles_for_mic];

    sg_dbg("On node: %d, countTilesForMic: %d\n", edge_engine_index_,
           count_tiles_for_mic);
    size_t size_tile_stats = sizeof(tile_stats_t) * count_tiles_for_mic;
    util::readDataFromFile(global_tile_stats_file_name, size_tile_stats,
                           tile_stats_);

    tile_offsets_ = new size_t[count_tiles_for_mic];
    tile_offsets_[0] = 0;

    for (int i = 1; i < count_tiles_for_mic; ++i) {
      // offset is the offset of the previous block plus the size of the
      // previous block:
      tile_stats_t tile_stats = tile_stats_[i - 1];

      // step 2: calculate required space to fetch the index blocks
      size_t size_edge_index_tgt_vertex_block =
          sizeof(uint32_t) * tile_stats.count_vertex_tgt;
      size_t size_edge_index_src_vertex_block =
          sizeof(uint32_t) * tile_stats.count_vertex_src;
      size_t size_edge_index_src_extended_block =
          vd_.config_.is_index_32_bits
              ? 0
              : size_bool_array(tile_stats.count_vertex_src);
      size_t size_edge_index_tgt_extended_block =
          vd_.config_.is_index_32_bits
              ? 0
              : size_bool_array(tile_stats.count_vertex_tgt);
      size_t size_index_block = sizeof(edge_block_index_t) +
                                size_edge_index_src_vertex_block +
                                size_edge_index_tgt_vertex_block +
                                size_edge_index_src_extended_block +
                                size_edge_index_tgt_extended_block;
      size_t size_rb_block = int_ceil(size_index_block, PAGE_SIZE);
      tile_offsets_[i] = tile_offsets_[i - 1] + size_rb_block;
    }

    // initialize index offset table
    index_offset_table_.data_info =
        (pointer_offset_t<edge_block_index_t, tile_data_vertex_engine_t>*)mmap(
            NULL,
            sizeof(index_offset_table_.data_info[0]) * config_.count_tiles,
            PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

    if (index_offset_table_.data_info == MAP_FAILED) {
      scalable_graphs::util::die(1);
    }

    // if in selective-scheduling-mode, also connect to the two ringbuffers,
    // sharing the tiles-active-array as well as the signaling-ring-buffer to
    // wake up all waiting threads
    if (config_.use_selective_scheduling) {
      // init arrays:
      // add +1 to account for uneven splitting up of last few tiles among
      // edge-engines
      size_tile_active_ =
          size_bool_array(((config_.count_tiles / config_.count_edge_processors) + 1));

      tile_active_current_ = new char[size_tile_active_];
      tile_active_next_ = new char[size_tile_active_];

      /*set this all inactive */
      memset(tile_active_next_, 0x00, size_tile_active_);
      memset(tile_active_current_, 0x00, size_tile_active_);
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  int VertexProcessor<APP, TVertexType, TVertexIdType>::init() {
    sg_log("Connecting to edge-engine %d on %d\n", edge_engine_index_, mic_id_);

    allocate();

    initRingBuffers();

    if (config_.enable_perf_event_collection) {
      perf_event_ring_buffer_sizes_ = new pe::PerfEventRingbufferSizes(
          pe::PerfEventManager::getInstance(config_)->getRingBuffer(),
          index_rb_, &tiles_data_rb_, &response_rb_, edge_engine_index_,
          &vd_.memory_init_barrier_);
      perf_event_ring_buffer_sizes_->start();
      perf_event_ring_buffer_sizes_->setName(
          "RBSizes_" + std::to_string(edge_engine_index_));
    }

    return 0;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  int VertexProcessor<APP, TVertexType, TVertexIdType>::start() {

    int socket = edge_engine_index_ % util::NUM_SOCKET;
    // int threads_per_edge_engine = config_.count_index_readers +
    //                               config_.count_vertex_reducers +
    //                               config_.count_vertex_fetchers;
    // int preceding_threads =
    //     threads_per_edge_engine * (edge_engine_index_ / util::NUM_SOCKET);

    // preceding_threads += config_.count_global_reducers / util::NUM_SOCKET;
    // if (config_.local_fetcher_mode == LocalFetcherMode::LFM_GlobalFetcher)
    // {
    //   preceding_threads += config_.count_global_fetchers /
    //   util::NUM_SOCKET;
    // }

    // util::cpu_id_t cpu_id(socket, CORE_OFFSET, 0);
    // for (int i = 0; i < preceding_threads; ++i) {
    //   cpu_id.nextNearest(config_.use_smt);
    // }

    util::cpu_id_t cpu_id(socket, util::cpu_id_t::ANYWHERE,
                          util::cpu_id_t::ANYWHERE);

    // launch index reader
    for (int i = 0; i < config_.count_index_readers; ++i) {
      thread_index_t thread_index;
      thread_index.count = config_.count_index_readers;
      thread_index.id = i;

      auto* thread = new core::IndexReader<APP, TVertexType, TVertexIdType>(
          *this, thread_index);

      thread->setAffinity(cpu_id);
      // cpu_id.nextNearest(config_.use_smt);

      thread->start();
      thread->setName("IndexReader_" + std::to_string(edge_engine_index_) +
                      "_" + std::to_string(i));
      index_readers_.push_back(thread);
    }

    // launch vertex updaters
    for (int i = 0; i < config_.count_vertex_fetchers; ++i) {
      thread_index_t thread_index;
      thread_index.count = config_.count_vertex_fetchers;
      thread_index.id = i;

      util::Runnable* thread;
      if (config_.local_fetcher_mode == LocalFetcherMode::LFM_Fake) {
      } else {
        auto vertex_fetcher =
            new core::VertexFetcher<APP, TVertexType, TVertexIdType>(
                *this, vd_.vertices_, tile_stats_, thread_index);
        vertex_fetchers_.push_back(vertex_fetcher);
        thread = vertex_fetcher;
      }

      thread->setAffinity(cpu_id);
      // cpu_id.nextNearest(config_.use_smt);

      thread->start();
      thread->setName("VFetcher_" + std::to_string(edge_engine_index_) + "_" +
                      std::to_string(i));
      threads_.push_back(thread);
    }

    // launch vertex reducers
    for (int i = 0; i < config_.count_vertex_reducers; ++i) {
      thread_index_t thread_index;
      thread_index.count = config_.count_vertex_reducers;
      thread_index.id = i;

      auto* thread = new core::VertexReducer<APP, TVertexType, TVertexIdType>(
          *this, vd_.vertices_, thread_index);

      thread->setAffinity(cpu_id);
      // cpu_id.nextNearest(config_.use_smt);

      thread->start();
      thread->setName("VReducer_" + std::to_string(edge_engine_index_) + "_" +
                      std::to_string(i));
      threads_.push_back(thread);
    }
  }

  template<class APP, typename TVertexType, typename TVertexIdType>
  void VertexProcessor<APP, TVertexType, TVertexIdType>::joinIndexReaders() {
    for (const auto& it : index_readers_) {
      it->join();
      delete (it);
    }
  };

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexProcessor<APP, TVertexType, TVertexIdType>::join() {
    if (!config_.in_memory_mode) {
      joinIndexReaders();
    }

    for (const auto& it : threads_) {
      it->join();
      delete (it);
    }

    if (config_.enable_perf_event_collection) {
      perf_event_ring_buffer_sizes_->join();
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexProcessor<APP, TVertexType, TVertexIdType>::sendActiveTiles(
      size_t count_active_tiles) {
    ring_buffer_req_t req_active_tiles;
    ring_buffer_put_req_init(&req_active_tiles, BLOCKING, size_tile_active_);
#if defined(MOSAIC_HOST_ONLY)
    ring_buffer_put(active_tiles_rb_, &req_active_tiles);
#else
    ring_buffer_scif_put(&active_tiles_rb_, &req_active_tiles);
#endif
    sg_rb_check(&req_active_tiles);

    active_tiles_t* active_tiles = (active_tiles_t*)(req_active_tiles.data);
    active_tiles->count_active_tiles = count_active_tiles;

#if defined(MOSAIC_HOST_ONLY)
    copy_to_ring_buffer(active_tiles_rb_, active_tiles->active_tiles,
                        tile_active_current_, size_tile_active_);

    ring_buffer_elm_set_ready(active_tiles_rb_, req_active_tiles.data);
#else
    copy_to_ring_buffer_scif(&active_tiles_rb_, active_tiles->active_tiles,
                             tile_active_current_, size_tile_active_);

    ring_buffer_scif_elm_set_ready(&active_tiles_rb_, req_active_tiles.data);
#endif
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexProcessor<APP, TVertexType, TVertexIdType>::resetRound(
      size_t count_active_tiles) {
    /*Before resetround, refresh active tiles info */
    if (config_.use_selective_scheduling) {
      // switch pointers and reset next
      char* temp = tile_active_next_;
      tile_active_next_ = tile_active_current_;
      tile_active_current_ = temp;

      sendActiveTiles(count_active_tiles);

      // pushing to remote array:
      // reset for next round:
      memset(tile_active_next_, 0, size_tile_active_);
      sg_log("Sending active tile list done from %d with %lu active tiles\n",
             edge_engine_index_, count_active_tiles);
    }

    sg_dbg("Round reset %d\n", edge_engine_index_);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexProcessor<APP, TVertexType, TVertexIdType>::shutdown() {
    for (auto vertex_fetcher : vertex_fetchers_) {
      vertex_fetcher->shutdown();
    }

    if (config_.enable_perf_event_collection) {
      perf_event_ring_buffer_sizes_->shutdown();
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  bool VertexProcessor<APP, TVertexType, TVertexIdType>::isShutdown() {
    return (vd_.isShutdown());
  }
}
}
