#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "global-reducer.h"
#endif
#pragma once

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <cmath>
#include <pthread.h>
#include <sys/stat.h>

#include <core/datatypes.h>
#include <core/util.h>
#include <util/arch.h>
#include <util/util.h>
#include <util/perf-event/perf-event-manager.h>
#include <util/perf-event/perf-event-scoped.h>

using namespace scalable_graphs::util::perf_event;

namespace scalable_graphs {
namespace core {

  template <class APP, typename TVertexType, typename TVertexIdType>
  GlobalReducer<APP, TVertexType, TVertexIdType>::GlobalReducer(
      VertexDomain<APP, TVertexType, TVertexIdType>& ctx,
      vertex_array_t<TVertexType>* vertices, const thread_index_t& thread_index)
      : config_(ctx.config_), ctx_(ctx), vertices_(vertices),
        thread_index_(thread_index), average_processing_rate_(1),
        count_processing_times_(0) {
    int rc =
        ring_buffer_create(processed_rb_size_, PAGE_SIZE, RING_BUFFER_BLOCKING,
                           NULL, NULL, &response_rb_);
    if (rc) {
      scalable_graphs::util::die(1);
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  GlobalReducer<APP, TVertexType, TVertexIdType>::~GlobalReducer() {
    ring_buffer_destroy(response_rb_);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void GlobalReducer<APP, TVertexType, TVertexIdType>::init_memory() {
    // Init ringbuffer.
    ring_buffer_init(response_rb_);

    size_t count_vertex_pages = std::ceil(
        config_.count_vertices / (double)VERTICES_PER_PARTITION_STRIPE);
    // Init the vertex array, touch the parts of the vertex and degree array
    // that will be serviced by this GlobalReducer.
    for (size_t i = 0; i < count_vertex_pages; ++i) {
      // Check if this thread owns the page.
      size_t offset = i * VERTICES_PER_PARTITION_STRIPE;
      size_t length = VERTICES_PER_PARTITION_STRIPE;
      // At the last page, only memset as many vertices as there are left:
      if (i == count_vertex_pages - 1) {
        length = vertices_->count - offset;
      }

      if (core::getPartitionOfVertex(offset, config_.count_global_reducers) ==
          thread_index_.id) {
        // Init the stripe by setting it to 0.
        uint8_t* offset_current =
            (uint8_t*)vertices_->current + sizeof(TVertexType) * offset;
        uint8_t* offset_next =
            (uint8_t*)vertices_->next + sizeof(TVertexType) * offset;

        memset(offset_current, 0, sizeof(TVertexType) * length);
        memset(offset_next, 0, sizeof(TVertexType) * length);

        offset_current = (uint8_t*)vertices_->active_current +
                         sizeof(char) * (size_t)size_bool_array(offset);
        offset_next = (uint8_t*)vertices_->active_next +
                      sizeof(char) * (size_t)size_bool_array(offset);

        memset(offset_current, 0, sizeof(char) * size_bool_array(length));
        memset(offset_next, 0, sizeof(char) * size_bool_array(length));

        uint8_t* offset_changed =
            (uint8_t*)vertices_->changed +
            sizeof(char) * (size_t)size_bool_array(offset);
        memset(offset_changed, 0, sizeof(char) * size_bool_array(length));

        uint8_t* offset_degree =
            (uint8_t*)vertices_->degrees + sizeof(vertex_degree_t) * offset;
        memset(offset_degree, 0, sizeof(vertex_degree_t) * length);
      }
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void GlobalReducer<APP, TVertexType, TVertexIdType>::receive_reduce_block() {
    // wait for responses
    ring_buffer_req_t request_processed;
    sg_print("Waiting for aggregated responses\n");
    ring_buffer_get_req_init(&request_processed, BLOCKING);
    ring_buffer_get(response_rb_, &request_processed);

    sg_rb_check(&request_processed);
    reduce_block_ = (processed_vertex_index_block_t*)request_processed.data;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void
  GlobalReducer<APP, TVertexType, TVertexIdType>::process_source_vertices() {
    // if using the src-indices for the active-array, do a second loop
    // to only update these, using the cached src-index-block
    for (uint32_t i = 0; i < reduce_block_->count_src_vertex_block; ++i) {
      // update active-status for next round

      // vertex is active either if it was active before or should be
      // active from now on
      TVertexIdType id_src = src_index_[i];
      bool old_active_status = eval_bool_array(vertices_->active_next, id_src);
      bool new_active_status =
          old_active_status || eval_bool_array(active_vertices_src_next_, i);

      set_bool_array(vertices_->active_next, id_src, new_active_status);

      if (old_active_status != new_active_status) {
        // set all tiles belonging to this vertex to active
        size_t offset = ctx_.vertex_to_tiles_offset_[id_src];
        for (int i = 0; i < ctx_.vertex_to_tiles_count_[id_src]; ++i) {
          size_t global_offset = offset + i;
          uint32_t tile_id = ctx_.vertex_to_tiles_index_[global_offset];
          // calculate edge-engine of this tile plus the local-tile-id
          int edge_engine_index =
              core::getEdgeEngineIndexFromTile(config_, tile_id);

          uint32_t local_tile_id = core::getLocalTileId(config_, tile_id);

          set_bool_array(ctx_.vp_[edge_engine_index]->tile_active_next_,
                         local_tile_id, true);
        }
      }
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void
  GlobalReducer<APP, TVertexType, TVertexIdType>::process_target_vertices() {
    for (uint32_t i = 0; i < reduce_block_->count_tgt_vertex_block; ++i) {
      TVertexIdType id_tgt = tgt_index_[i];

      if (APP::need_active_target_block) {
        // first update active-status for next round
        bool old_active_status =
            eval_bool_array(vertices_->active_next, id_tgt);
        bool new_active_status =
            old_active_status || eval_bool_array(active_vertices_tgt_next_, i);
        set_bool_array(vertices_->active_next, id_tgt, new_active_status);
      }

      // Apply reduce function to temporary value, if it changes the overall
      // value, swap it.
      APP::reduceVertex(
          vertices_->next[id_tgt], tgt_vertices_[i], vertices_->next[id_tgt],
          id_tgt, vertices_->degrees[id_tgt], vertices_->active_next, config_);
      // TVertexType new_value;
      // APP::reduceVertex(new_value, tgt_vertices_[i],
      // vertices_->next[id_tgt],
      //                   id_tgt, vertices_->degrees[id_tgt],
      //                   vertices_->active_next, config_);
      // bool changed = (new_value == vertices_->next[id_tgt]);
      // if (changed) {
      //   vertices_->next[id_tgt] = new_value;
      //   set_bool_array(vertices_->changed, id_tgt, true);
      // }
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void
  GlobalReducer<APP, TVertexType, TVertexIdType>::parse_arrays_from_response() {
    active_vertices_src_next_ =
        APP::need_active_source_block
            ? get_array(char*, reduce_block_,
                        reduce_block_->offset_active_vertices_src)
            : NULL;
    active_vertices_tgt_next_ =
        APP::need_active_target_block
            ? get_array(char*, reduce_block_,
                        reduce_block_->offset_active_vertices_tgt)
            : NULL;
    tgt_vertices_ =
        get_array(TVertexType*, reduce_block_, reduce_block_->offset_vertices);

    tgt_index_ = get_array(TVertexIdType*, reduce_block_,
                           reduce_block_->offset_tgt_indices);

    src_index_ = get_array(TVertexIdType*, reduce_block_,
                           reduce_block_->offset_src_indices);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void
  GlobalReducer<APP, TVertexType, TVertexIdType>::aggregateProcessingTime() {
    average_processing_rate_ = average_processing_rate_ /
                               (count_processing_times_ + 1) *
                               count_processing_times_;
    average_processing_rate_ += reduce_block_->count_edges /
                                (double)reduce_block_->processing_time_nano /
                                ((double)count_processing_times_ + 1);
    count_processing_times_++;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void GlobalReducer<APP, TVertexType, TVertexIdType>::run() {
    // Elevate priority of this thread.
    setHighestPriority();

    // Init the memory that shall be owned by this thread.
    init_memory();

    // Let the algorithm init the datastructure as well, after touching the
    // memory for the first time, then active the necessary tiles.
    int barrier_rc =
        pthread_barrier_wait(&ctx_.memory_init_global_reducer_barrier_);
    if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
      ctx_.initAlgorithm();

      // Wait until all VertexProcessors are initialized.
      pthread_barrier_wait(&ctx_.init_algorithm_barrier_);

      if (config_.use_selective_scheduling) {
        // Now initialize the active tiles.
        ctx_.initActiveTiles();
      }

      // Join the init_active_tiles barrier to signal completion of initializing
      // the active tiles, this allows the VertexProcessor to send out the set
      // of active tiles to the Edge Engines.
      pthread_barrier_wait(&ctx_.init_active_tiles_barrier_);
    }

    // Then wait for all memory inits to be done.
    barrier_rc = pthread_barrier_wait(&ctx_.memory_init_barrier_);

    if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
      ctx_.initTimers();
    }

    bool shutdown = false;
    int iteration = 0;

    while (true) {
      int responses_received = 0;

      // make sure exactly $count_tiles many response-processors are waiting
      // on response per round
      while (responses_received < config_.count_tiles) {
        receive_reduce_block();

        PerfEventScoped perf_event(
            PerfEventManager::getInstance(config_)->getRingBuffer(), "tile",
            ComponentType::CT_GlobalReducer, 0, thread_index_.id,
            reduce_block_->count_tgt_vertex_block, reduce_block_->block_id,
            config_.enable_perf_event_collection);

        if (reduce_block_->shutdown) {
          // Set the shutdown flag, escape from the inner while block and exit
          // the outer while loop afterwards.
          shutdown = true;
          break;
        }

        if (reduce_block_->completed) {
          ++responses_received;
        }

        // Skip if just a dummy block.
        if (reduce_block_->dummy) {
          ring_buffer_elm_set_done(response_rb_, reduce_block_);
          continue;
        }

        sg_dbg("Got aggregated response for block %lu\n",
               reduce_block_->block_id);

        parse_arrays_from_response();
        if (APP::need_active_source_block) {
          process_source_vertices();
        }
        process_target_vertices();

        if (thread_index_.id == 0 && reduce_block_->sample_execution_time) {
          aggregateProcessingTime();
        }

        // done with processing response, let it be reclaimed
        sg_dbg("Global Reducer Done processing response for block %lu\n",
               reduce_block_->block_id);
        ring_buffer_elm_set_done(response_rb_, reduce_block_);
      }

      ++iteration;
      if (shutdown) {
        break;
      }

      sg_dbg("GlobalReducer %lu: Done for round %d\n", thread_index_.id,
             iteration);

      pthread_barrier_wait(&ctx_.end_reduce_barrier_);
    }

    sg_log("Shutdown GlobalReducer %lu\n", thread_index_.id);
  }
}
}
