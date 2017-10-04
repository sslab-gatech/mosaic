#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "vertex-domain.h"
#endif
#pragma once

#include <algorithm>

#include <util/arch.h>
#include <assert.h>

namespace scalable_graphs {
namespace core {

  template <typename TVertexType>
  struct flusher_args_t {
    vertex_array_t<TVertexType>* array;
    std::string fault_tolerance_ouput_path;
    int iteration;
    pthread_barrier_t* barrier;
  };

  template <typename TVertexType>
  void* flushVertexArrayToDisk(void* func_args) {
    flusher_args_t<TVertexType>* args = (flusher_args_t<TVertexType>*)func_args;

    std::string vertex_output_filename = core::getVertexOutputFileName(
        args->fault_tolerance_ouput_path, args->iteration);
    size_t size = sizeof(TVertexType) * args->array->count;

    sg_log("Flushing vertices to disk for iteration %d to %s\n",
           args->iteration, vertex_output_filename.c_str());

    util::writeDataToFileSync(vertex_output_filename, args->array->current,
                              size);
    sg_log("Done flushing vertices to disk for iteration %d to %s\n",
           args->iteration, vertex_output_filename.c_str());

    // sync with host
    pthread_barrier_wait(args->barrier);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  VertexDomain<APP, TVertexType, TVertexIdType>::VertexDomain(
      const config_vertex_domain_t& config)
      : shutdown_(false), config_(config), iteration_(0),
        tile_break_point_(INIT_TILE_BREAK_POINT) {
    for (int i = 0; i < config.count_edge_processors; ++i) {
      // adjust the port to be spaced by 100 between different MICs
      config_vertex_domain_t vp_config = config;
      vp_config.port = config.port + i * 100;
      int node_id = 0;
      if (config.run_on_mic) {
        node_id = config.edge_engine_to_mic[i];
      }

      vp_.push_back(new VertexProcessor<APP, TVertexType, TVertexIdType>(
          *this, vp_config, node_id, i));
    }

    gettimeofday(&init_tv_, NULL);

    pthread_mutex_init(&active_tiles_mutex_, NULL);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  VertexDomain<APP, TVertexType, TVertexIdType>::~VertexDomain() {
    // TODO: Cleanup
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  int VertexDomain<APP, TVertexType, TVertexIdType>::init() {
    // only load translation table if needed to generate output
    if (!config_.path_to_log.empty()) {
      std::string id_translation_file_name =
          core::getGlobalToOrigIDFileName(config_);
      core::readMapFromFile<TVertexIdType, int64_t>(id_translation_file_name,
                                                    global_to_orig_);
    }

    // only load vertex-to-tiles indices when running in selective-scheduling
    // mode:
    if (config_.use_selective_scheduling) {
      vertex_to_tiles_offset_ = new size_t[config_.count_vertices];
      vertex_to_tiles_count_ = new uint32_t[config_.count_vertices];

      std::string vertex_to_tile_count_filename =
          core::getVertexToTileCountFileName(config_.path_to_globals);
      size_t size_vertex_to_tile_count =
          config_.count_vertices * sizeof(uint32_t);
      util::readDataFromFile(vertex_to_tile_count_filename,
                             size_vertex_to_tile_count, vertex_to_tiles_count_);

      vertex_to_tiles_offset_[0] = 0;
      size_t global_vertex_to_tiles_count = vertex_to_tiles_count_[0];

      for (size_t i = 1; i < config_.count_vertices; ++i) {
        vertex_to_tiles_offset_[i] =
            vertex_to_tiles_offset_[i - 1] + vertex_to_tiles_count_[i];
        global_vertex_to_tiles_count += vertex_to_tiles_count_[i];
      }

      sg_dbg("global_vertex_to_tiles_counnt %lu \n",
             global_vertex_to_tiles_count);
      vertex_to_tiles_index_ = new uint32_t[global_vertex_to_tiles_count];

      std::string vertex_to_tiles_index_filename =
          core::getVertexToTileIndexFileName(config_.path_to_globals);
      size_t size_vertex_to_tiles_index =
          global_vertex_to_tiles_count * sizeof(uint32_t);
      util::readDataFromFile(vertex_to_tiles_index_filename,
                             size_vertex_to_tiles_index,
                             vertex_to_tiles_index_);
    }

    initVertexArray();

    // For the reduce-barrier we have to wait for all global reducers to arrive,
    // all appliers are already waiting there
    int count_reduce_barrier =
        config_.count_global_reducers + config_.count_vertex_appliers;

    // For the apply-barrier, we have to wait for all appliers to finish as well
    // as all vertex-fetchers for all mics to be at the barrier.
    int count_apply_barrier =
        config_.count_edge_processors * config_.count_vertex_fetchers +
        config_.count_vertex_appliers;

    // in selective scheduling mode,
    // index reader and reducer should wait until
    // it update active tiles correctly
    if (config_.use_selective_scheduling) {
      count_apply_barrier +=
          config_.count_edge_processors *
          config_.count_index_readers; // this for index readers
    }

    int count_memory_init_barrer =
        config_.count_global_reducers +
        config_.count_edge_processors *
            (config_.count_index_readers + config_.count_vertex_reducers +
             config_.count_vertex_fetchers);

    if (config_.enable_perf_event_collection) {
      // In perf event collection is enabled, add the ringbuffer monitors here,
      // one per VertexProcessor/Edge Engine.
      count_memory_init_barrer += config_.count_edge_processors;
    }

    if (config_.local_fetcher_mode == LocalFetcherMode::LFM_GlobalFetcher) {
      count_memory_init_barrer += config_.count_global_fetchers;
    }

    // 1 GlobalReducer + 1 VertexFetcher per Edge Engine.
    int count_init_algorithm_barrier = 1 + config_.count_edge_processors;

    // 1 GlobalReducer + 1 VertexFetcher per Edge Engine.
    int count_init_active_tiles_barrier = 1 + config_.count_edge_processors;

    pthread_barrier_init(&end_reduce_barrier_, NULL, count_reduce_barrier);
    pthread_barrier_init(&end_apply_barrier_, NULL, count_apply_barrier);
    pthread_barrier_init(&local_apply_barrier_, NULL,
                         config_.count_vertex_appliers);

    pthread_barrier_init(&fault_tolerance_barrier_, NULL, 2);

    pthread_barrier_init(&memory_init_barrier_, NULL, count_memory_init_barrer);
    pthread_barrier_init(&memory_init_global_reducer_barrier_, NULL,
                         config_.count_global_reducers);
    pthread_barrier_init(&init_algorithm_barrier_, NULL,
                         count_init_algorithm_barrier);
    pthread_barrier_init(&init_active_tiles_barrier_, NULL,
                         count_init_active_tiles_barrier);

    // In case we are using the locking approach, init the lock table here.
    if (config_.local_reducer_mode == LocalReducerMode::LRM_Locking) {
      vertex_lock_table.count = VERTEX_LOCK_TABLE_SIZE;
      vertex_lock_table.salt = rand32_seedless();
      vertex_lock_table.locks = new pthread_spinlock_t[VERTEX_LOCK_TABLE_SIZE];
      for (int i = 0; i < VERTEX_LOCK_TABLE_SIZE; ++i) {
        pthread_spin_init(&vertex_lock_table.locks[i], PTHREAD_PROCESS_PRIVATE);
      }
    }

    sg_log2("Intialization done\n");
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  int VertexDomain<APP, TVertexType, TVertexIdType>::start() {
    // launch perfmon
    if (config_.do_perfmon) {
      perfmon_.start();
    }

    // Launch PerfEventManager.
    if (config_.enable_perf_event_collection) {
      pe::PerfEventManager::getInstance(config_)->start();
    }

    // launch vertex appliers
    for (int i = 0; i < config_.count_vertex_appliers; ++i) {
      thread_index_t thread_index;
      thread_index.count = config_.count_vertex_appliers;
      thread_index.id = i;

      auto* thread = new core::VertexApplier<APP, TVertexType, TVertexIdType>(
          *this, vertices_, thread_index);
      thread->start();
      thread->setName("VApplier_" + std::to_string(i));
      threads_.push_back(thread);
      vertex_appliers_.push_back(thread);
    }

    // launch global reducers
    global_reducers_ =
        new core::GlobalReducer<APP, TVertexType,
                                TVertexIdType>*[config_.count_global_reducers];
    for (int i = 0; i < config_.count_global_reducers; ++i) {
      thread_index_t thread_index;
      thread_index.count = config_.count_global_reducers;
      thread_index.id = i;

      global_reducers_[i] =
          new core::GlobalReducer<APP, TVertexType, TVertexIdType>(
              *this, vertices_, thread_index);

      int socket = i % util::NUM_SOCKET;
      util::cpu_id_t cpu_id(socket, util::cpu_id_t::ANYWHERE,
                            util::cpu_id_t::ANYWHERE);
      global_reducers_[i]->setAffinity(cpu_id);

      global_reducers_[i]->start();
      global_reducers_[i]->setName("GlobalReducer_" + std::to_string(i));
      threads_.push_back(global_reducers_[i]);
    }

    // Only launch global fetchers if instructed to do so.
    if (config_.local_fetcher_mode == LocalFetcherMode::LFM_GlobalFetcher) {
      // launch global fetchers
      global_fetchers_ = new core::GlobalFetcher<
          APP, TVertexType, TVertexIdType>*[config_.count_global_fetchers];
      for (int i = 0; i < config_.count_global_fetchers; ++i) {
        thread_index_t thread_index;
        thread_index.count = config_.count_global_fetchers;
        thread_index.id = i;

        global_fetchers_[i] =
            new core::GlobalFetcher<APP, TVertexType, TVertexIdType>(
                *this, vertices_, thread_index);

        int socket = i % util::NUM_SOCKET;
        util::cpu_id_t cpu_id(socket, util::cpu_id_t::ANYWHERE,
                              util::cpu_id_t::ANYWHERE);
        global_fetchers_[i]->setAffinity(cpu_id);
        global_fetchers_[i]->start();
        global_fetchers_[i]->setName("GlobalFetcher_" + std::to_string(i));
        threads_.push_back(global_fetchers_[i]);
      }
    }

    for (auto& it : vp_) {
      it->start();
    }

    // - in the in-memory mode, wait for everything to be loaded first:
    if (config_.in_memory_mode) {
      sg_log2("Wait for index readers to be done.\n");
      for (auto& it : vp_) {
        it->joinIndexReaders();
      }
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexDomain<APP, TVertexType, TVertexIdType>::calculateTileBreakPoint(
      const size_t& count_active_tiles) {
    double average_processing_rate =
        global_reducers_[0]->average_processing_rate_;
    size_t max_tile_size = sizeof(TVertexType) * MAX_VERTICES_PER_TILE;
    if (APP::need_degrees_source_block) {
      max_tile_size += sizeof(vertex_degree_t) * MAX_VERTICES_PER_TILE;
    }

    size_t min_count_tiles =
        std::ceil(config_.host_tiles_rb_size / max_tile_size);

    // In case count of tiles is smaller than the number of tiles that can fit
    // in half a buffer, switch to smaller tiles.
    if (count_active_tiles < (min_count_tiles / 2)) {
      tile_break_point_ = MIN_TILE_BREAK_POINT;
    } else {
      double t_mintile = MAX_VERTICES_PER_TILE / average_processing_rate;

      double t_maxtile = t_mintile * (min_count_tiles - 1) /
                         (config_.count_tile_processors - 1);

      size_t max_tile_break_point = t_maxtile * average_processing_rate;
      tile_break_point_ = max_tile_break_point;
    }
    sg_log("New tile breakpoint: %lu, Average time per tile: %f\n",
           tile_break_point_, average_processing_rate);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexDomain<APP, TVertexType, TVertexIdType>::join() {
    for (const auto& it : threads_) {
      it->join();
      sg_dbg("A thread is exiting: %p\n", it);
      delete (it);
    }
    for (auto& it : vp_) {
      it->join();
      sg_dbg("A VP is exiting: %p\n", it);
    }
    if (config_.do_perfmon) {
      perfmon_.stop();
      perfmon_.join();
      sg_print("A perfmon is exiting:\n");
    }

    // Join PerfEventManager.
    if (config_.enable_perf_event_collection) {
      pe::PerfEventManager::getInstance(config_)->stop();
      sg_print("EventManager is exiting\n");
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexDomain<APP, TVertexType, TVertexIdType>::initVertexArray() {
    // allocate vertex array
    vertices_ = new vertex_array_t<TVertexType>;
    size_t size_active_array = size_bool_array(config_.count_vertices);

    vertices_->count = config_.count_vertices;
    vertices_->size_active = size_active_array;

    vertices_->degrees = new vertex_degree_t[config_.count_vertices];

    vertices_->current = new TVertexType[config_.count_vertices];
    vertices_->next = new TVertexType[config_.count_vertices];

    vertices_->active_current = new char[size_active_array];
    vertices_->active_next = new char[size_active_array];

    vertices_->changed = new char[size_active_array];
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexDomain<APP, TVertexType, TVertexIdType>::initAlgorithm() {
    // read degrees into global array
    size_t degree_filesize = sizeof(vertex_degree_t) * config_.count_vertices;
    std::string degree_filename = core::getVertexDegreeFileName(config_);
    util::readDataFromFile(degree_filename, degree_filesize,
                           vertices_->degrees);

    // let algorithm init vertex-array
    // TODO: pass args
    APP::init_vertices(vertices_, NULL);

    // give APP the chance to initialize before the first round as well
    APP::pre_processing_per_round(vertices_, config_, iteration_);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexDomain<APP, TVertexType, TVertexIdType>::initActiveTiles() {
    assert(config_.use_selective_scheduling);
    sg_print("Init active tiles\n");
    // activate tiles for the first round if needed
    // iterate all vertices, set tiles active if needed
    for (size_t vertex_id = 0; vertex_id < config_.count_vertices;
         ++vertex_id) {
      bool active_status =
          eval_bool_array(vertices_->active_current, vertex_id);
      // activate tiles of this vertex
      // at the first round, most vertices not active

      // active_status = true;
      if (active_status) {
        // set all tiles belonging to this vertex to active
        size_t offset = vertex_to_tiles_offset_[vertex_id];
        for (int i = 0; i < vertex_to_tiles_count_[vertex_id]; ++i) {
          size_t global_offset = offset + i;
          uint32_t tile_id = vertex_to_tiles_index_[global_offset];
          // calculate edge-engine of this tile plus the local-tile-id

          if (tile_id > config_.count_tiles) {
            sg_log("tile id %u exceed count_tiles bound\n", tile_id);
            sg_assert(0, "tile id exceed count_tiles bound");
          }

          int edge_engine_index =
              core::getEdgeEngineIndexFromTile(config_, tile_id);

          uint32_t local_tile_id = core::getLocalTileId(config_, tile_id);

          set_bool_array(vp_[edge_engine_index]->tile_active_current_,
                         local_tile_id, true);
        }
      }
    }
    sg_print("Done init active tiles \n");
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  size_t VertexDomain<APP, TVertexType, TVertexIdType>::countActiveVertices() {
    size_t count_active_vertices = 0;

    for (size_t vertex_id = 0; vertex_id < config_.count_vertices;
         ++vertex_id) {
      if (eval_bool_array(vertices_->active_current, vertex_id)) {
        ++count_active_vertices;
      }
    }

    return count_active_vertices;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  size_t VertexDomain<APP, TVertexType, TVertexIdType>::countActiveTiles() {
    size_t count_active_tiles = 0;
    for (size_t tile_id = 0; tile_id < config_.count_tiles; ++tile_id) {
      int edge_engine_index =
          core::getEdgeEngineIndexFromTile(config_, tile_id);

      uint32_t local_tile_id = core::getLocalTileId(config_, tile_id);

      if (eval_bool_array(vp_[edge_engine_index]->tile_active_next_,
                          local_tile_id)) {
        ++count_active_tiles;
      }
    }

    return count_active_tiles;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexDomain<APP, TVertexType, TVertexIdType>::resetRound() {
    // get timing info
    struct timeval current_tv, result_time;

    gettimeofday(&current_tv, NULL);
    timersub(&current_tv, &start_tv_round_, &result_time);

    sg_log("Round Time for iteration %lu %.3fmsec\n", (iteration_ + 1),
           result_time.tv_sec * 1000 + result_time.tv_usec / 1000.0);
    gettimeofday(&start_tv_round_, NULL);
    sg_print("Write output, flip vertices and reset for next round\n");

    // write output
    if (!config_.path_to_log.empty()) {
      core::writeOutput<TVertexType, TVertexIdType, int64_t>(
          global_to_orig_, config_.path_to_log, iteration_, vertices_->count,
          vertices_->next);
    }

    // allow application to vote against switching the current and next fields,
    // i.e. for more than one iteration per super-step:
    bool switchCurrentNext = true;
    // reset internal state
    APP::reset_vertices(vertices_, &switchCurrentNext);

    // in iterations other than the first one, wait for the flusher to finish
    // first:
    if (iteration_ > 0) {
      if (config_.enable_fault_tolerance) {
        sg_log2("Wait for flusher to finish\n");
        pthread_barrier_wait(&fault_tolerance_barrier_);
      }
    }

    if (switchCurrentNext) {
      TVertexType* temp_vertices = vertices_->current;
      vertices_->current = vertices_->next;
      vertices_->next = temp_vertices;

      char* temp_active = vertices_->active_current;
      vertices_->active_current = vertices_->active_next;
      vertices_->active_next = temp_active;
    }

    // Reset changed status.
    memset(vertices_->changed, 0, sizeof(char) * vertices_->size_active);

    size_t count_active_tiles;
    if (config_.use_selective_scheduling) {
      count_active_tiles = countActiveTiles();
    } else {
      count_active_tiles = config_.count_tiles;
    }

    sg_log("Number of active tiles: %lu out of %lu\n", count_active_tiles,
           config_.count_tiles);

    // Give VertexProcessors chance to reset internal stat.
    for (auto& it : vp_) {
      it->resetRound(count_active_tiles);
    }

    // flush to disk, if requested
    if (config_.enable_fault_tolerance) {
      flusher_args_t<TVertexType>* args = new flusher_args_t<TVertexType>;
      args->array = vertices_;
      args->fault_tolerance_ouput_path = config_.fault_tolerance_ouput_path;
      args->iteration = iteration_;
      args->barrier = &fault_tolerance_barrier_;

      pthread_t t;
      pthread_create(&t, NULL, flushVertexArrayToDisk<TVertexType>, args);
    }

    sg_log("Wake up everyone, done for round %lu\n", (iteration_ + 1));
    ++iteration_;
    // Converge either on number of iterations or on all tiles being inactive
    // when using the selective scheduling.
    bool end_condition_selective_scheduling =
        (config_.use_selective_scheduling && count_active_tiles == 0);
    bool end_condition_no_selective_scheduling = false;
    if (!config_.use_selective_scheduling &&
        (config_.algorithm == "bfs" || config_.algorithm == "cc")) {
      size_t count_active_vertices = countActiveVertices();
      sg_log("Count active vertices: %lu out of %lu\n", count_active_vertices,
             config_.count_vertices);
      end_condition_no_selective_scheduling = (count_active_vertices == 0);
    }

    if (iteration_ >= config_.max_iterations ||
        end_condition_selective_scheduling ||
        end_condition_no_selective_scheduling) {
      // wait once more for flushing
      if (config_.enable_fault_tolerance) {
        sg_log2("Wait for flusher to finish\n");
        pthread_barrier_wait(&fault_tolerance_barrier_);
      }
      sg_log("Finished with execution after %lu iterations!\n", iteration_);
      shutdown();
      return;
    }

    // next round starts after this, expose preProcessing to APP:
    APP::pre_processing_per_round(vertices_, config_, iteration_);

    // Calculate the new tile break point.
    calculateTileBreakPoint(count_active_tiles);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexDomain<APP, TVertexType, TVertexIdType>::initTimers() {
    timeval current_tv, result_time;
    gettimeofday(&current_tv, NULL);
    timersub(&current_tv, &init_tv_, &result_time);

    sg_log("Init time: %.3fmsec\n",
           result_time.tv_sec * 1000 + result_time.tv_usec / 1000.0);

    // Start the actual timer.
    gettimeofday(&start_tv_round_, NULL);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexDomain<APP, TVertexType, TVertexIdType>::shutdown() {
    // Used by the VertexApplier, VertexProcessor to determine if its shutdown.
    shutdown_ = true;

    for (auto it : vp_) {
      it->shutdown();
    }

    // Shutdown other components: GlobalReducer, GlobalFetcher.

    // Shutdown GlobalReducer by pushing a shutdown block.
    ring_buffer_req_t request_global_reducer_block;
    for (int i = 0; i < config_.count_global_reducers; ++i) {
      ring_buffer_put_req_init(&request_global_reducer_block, BLOCKING,
                               sizeof(processed_vertex_index_block_t));
      ring_buffer_put(global_reducers_[i]->response_rb_,
                      &request_global_reducer_block);

      sg_rb_check(&request_global_reducer_block);

      processed_vertex_index_block_t* block =
          (processed_vertex_index_block_t*)request_global_reducer_block.data;
      block->shutdown = true;

      ring_buffer_elm_set_ready(global_reducers_[i]->response_rb_,
                                request_global_reducer_block.data);
    }

    if (config_.local_fetcher_mode == LocalFetcherMode::LFM_GlobalFetcher) {
      // Shutdown global fetchers.
      ring_buffer_req_t put_fetch_req;
      for (int i = 0; i < config_.count_global_fetchers; ++i) {
        ring_buffer_put_req_init(&put_fetch_req, BLOCKING,
                                 sizeof(fetch_vertices_request_t));
        ring_buffer_put(global_fetchers_[i]->request_rb_, &put_fetch_req);
        sg_rb_check(&put_fetch_req);

        fetch_vertices_request_t* block =
            (fetch_vertices_request_t*)put_fetch_req.data;

        block->shutdown = true;

        ring_buffer_elm_set_ready(global_fetchers_[i]->request_rb_,
                                  put_fetch_req.data);
      }
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  bool VertexDomain<APP, TVertexType, TVertexIdType>::isShutdown() {
    return shutdown_;
  }
}
}
