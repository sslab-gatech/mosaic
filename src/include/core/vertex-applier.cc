#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "vertex-applier.h"
#endif
#pragma once

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <cmath>
#include <pthread.h>
#include <sys/stat.h>
#include <util/arch.h>
#include <core/datatypes.h>
#include <core/util.h>
#include <util/perf-event/perf-event-manager.h>
#include <util/perf-event/perf-event-scoped.h>

using namespace scalable_graphs::util::perf_event;

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, typename TVertexIdType>
  VertexApplier<APP, TVertexType, TVertexIdType>::VertexApplier(
      VertexDomain<APP, TVertexType, TVertexIdType>& ctx,
      vertex_array_t<TVertexType>* vertices, const thread_index_t& thread_index)
      : config_(ctx.config_), ctx_(ctx), vertices_(vertices),
        thread_index_(thread_index) {
    // do nothing
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  VertexApplier<APP, TVertexType, TVertexIdType>::~VertexApplier() {
    free(local_active_tiles_);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexApplier<APP, TVertexType, TVertexIdType>::reduceActiveTiles() {
    pthread_mutex_lock(&ctx_.active_tiles_mutex_);
    for (int tile_id = 0; tile_id < config_.count_tiles; ++tile_id) {
      if (eval_bool_array(local_active_tiles_, tile_id)) {
        // calculate edge-engine of this tile plus the local-tile-id
        int edge_engine_index =
            core::getEdgeEngineIndexFromTile(ctx_.config_, tile_id);

        if (tile_id > ctx_.config_.count_tiles) {
          sg_log("tile id %u exceed count_tiles bound\n", tile_id);
          sg_assert(0, "tile id exceed count_tiles bound");
        }
        uint32_t local_tile_id = core::getLocalTileId(ctx_.config_, tile_id);

        set_bool_array(ctx_.vp_[edge_engine_index]->tile_active_next_,
                       local_tile_id, true);
      }
    }

    pthread_mutex_unlock(&ctx_.active_tiles_mutex_);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexApplier<APP, TVertexType, TVertexIdType>::initLocalActiveTiles() {
    size_t size_active_tiles = size_bool_array(ctx_.config_.count_tiles);
    memset(local_active_tiles_, 0, size_active_tiles);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexApplier<APP, TVertexType, TVertexIdType>::allocate() {
    size_t size_active_tiles = size_bool_array(ctx_.config_.count_tiles);
    local_active_tiles_ = (char*)malloc(size_active_tiles);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void
  VertexApplier<APP, TVertexType, TVertexIdType>::apply(const size_t offset,
                                                        const size_t end) {
    // execute apply-function on all vertices assigned to this processor:
    for (uint64_t i = offset; i < end; ++i) {
      // only execute apply-function if vertex is active currently or in the
      // next iterations
      APP::apply(vertices_, i, ctx_.config_, ctx_.iteration_);

      if (config_.use_selective_scheduling) {
        // Check if outgoing edges active the outgoing vertices/tiles.
        if (eval_bool_array(vertices_->active_next, i)) {
          // set all tiles belonging to this vertex to active
          size_t offset = ctx_.vertex_to_tiles_offset_[i];
          for (int i = 0; i < ctx_.vertex_to_tiles_count_[i]; ++i) {
            size_t global_offset = offset + i;
            uint32_t tile_id = ctx_.vertex_to_tiles_index_[global_offset];
            set_bool_array(local_active_tiles_, tile_id, true);
          }
        }
      }
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexApplier<APP, TVertexType, TVertexIdType>::run() {
    allocate();

    size_t share_per_thread =
        std::ceil(ctx_.config_.count_vertices / (double)thread_index_.count);

    size_t offset = thread_index_.id * share_per_thread;
    size_t end = offset + share_per_thread;

    end = std::min(end, ctx_.config_.count_vertices);

    int count_iteration = 0;

    while (true) {
      // first wait for all responses to be collected
      pthread_barrier_wait(&ctx_.end_reduce_barrier_);
      {
        PerfEventScoped perf_event(
            PerfEventManager::getInstance(ctx_.config_)->getRingBuffer(),
            "apply", ComponentType::CT_VertexApplier, 0, thread_index_.id,
            count_iteration, ctx_.config_.enable_perf_event_collection);

        sg_dbg("Applying the round %d\n", count_iteration);
        initLocalActiveTiles();

        apply(offset, end);

        reduceActiveTiles();

        sg_dbg("Done applying for round %d\n", count_iteration);
      }
      int barrier_rc = pthread_barrier_wait(&ctx_.local_apply_barrier_);

      // if last thread, reset everything via the vertex-domain
      if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
        ctx_.resetRound();
      }
      pthread_barrier_wait(&ctx_.local_apply_barrier_);

      pthread_barrier_wait(&ctx_.end_apply_barrier_);

      // End the endless loop on shutdown, done.
      if (ctx_.isShutdown()) {
        break;
      }
      ++count_iteration;
    }
    sg_log("Shutdown VertexApplier %lu\n", thread_index_.id);
  }
}
}
