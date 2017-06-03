#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "tile-reader.h"
#endif
#pragma once

#include <arch.h>
#include <assert.h>
#include <util/read-context.h>

#include <util/perf-event/perf-event-manager.h>
#include <util/perf-event/perf-event-scoped.h>

using namespace scalable_graphs::util::perf_event;

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, bool is_weighted>
  TileReader<APP, TVertexType, is_weighted>::TileReader(
      EdgeProcessor<APP, TVertexType, is_weighted>& ctx,
      const thread_index_t& thread_index, pthread_barrier_t* barrier)
      : ReaderBase(thread_index), config_(ctx.config_), ctx_(ctx),
        barrier_(barrier) {
    tile_offsets_ = ctx_.tile_offsets_;

    tiles_offset_table_ = ctx_.tiles_offset_table_;

    rb_ = ctx_.local_tiles_rb_;

    fd_ = ctx_.tiles_fd_;

    count_tiles_for_current_mic_ =
        core::countTilesPerMic(config_, config_.mic_index);

    reader_progress_ = &ctx_.tile_reader_progress_;

    // According to selective scheduling mode, change batch size and batch per
    // iter.
    if (config_.use_selective_scheduling) {
      tile_batch_size_ = 1;
    } else {
      tile_batch_size_ = 1;
    }

    num_batch_per_iter_ =
        ceil(double(count_tiles_for_current_mic_) / double(tile_batch_size_));
  }

  template <class APP, typename TVertexType, bool is_weighted>
  TileReader<APP, TVertexType, is_weighted>::~TileReader() {
    // do nothing
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileReader<APP, TVertexType, is_weighted>::on_before_publish_data(
      edge_block_t* data, const size_t tile_id) {
    data->block_id = tile_id;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  size_t
  TileReader<APP, TVertexType, is_weighted>::get_tile_size(size_t tile_id) {
    // find the current file status
    tile_stats_t tile_stats = ctx_.tile_stats_[tile_id];

    // calculate required space to fetch the tile
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

    return size_rb_block;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileReader<APP, TVertexType, is_weighted>::run() {
    size_t prev_iter = 0; /*round control*/
    size_t count_active_tiles = 0;
    size_t count_inactive_tiles = 0;
    size_t iteration = 0;

    sg_dbg("TilesPerMic %d: count: %lu\n", config_.mic_index,
           count_tiles_for_current_mic_);

    // step 0: if selective-scheduling is enabled, check if the file even
    // should be read
    while (true) {
      size_t tile_id = grab_a_tile(iteration);

      PerfEventScoped perf_event(
          PerfEventManager::getInstance(config_)->getRingBuffer(), "tile",
          ComponentType::CT_TileReader, config_.mic_index, thread_index_.id,
          tile_id, config_.enable_perf_event_collection);

      if (config_.use_selective_scheduling) {
        if (iteration != prev_iter) {
          // iteration control - need to update active tile list in each round
          // done with all tiles, wait for next round:
          sg_log("Tile Reader Done with round %lu\n", prev_iter);

          /*update tile stat */
          ctx_.perfmon_.updateTileStat(count_active_tiles,
                                       count_inactive_tiles);

          // did we run all iterations?
          if (iteration >= config_.max_iterations) {
            sg_log("Finish Tile Reader %lu \n", thread_index_.id);
            break;
          }

          /*wait all tile reader before update active tile list */
          int res = pthread_barrier_wait(barrier_);

          if (res == PTHREAD_BARRIER_SERIAL_THREAD) {
            /*update active tiles and stats */
            size_t count_active_tiles = ctx_.updateActiveTiles();
            // Count active tiles, shut down if converged (iff count == 0).
            if (count_active_tiles == 0) {
              ctx_.shutdown();
            }

            ctx_.perfmon_.resetTileStat();
            count_active_tiles = 0;
            count_inactive_tiles = 0;

          } else if (res != 0) {
            scalable_graphs::util::die(1);
          }

          pthread_barrier_wait(barrier_);

          // Check if shutdown, break and exit if so.
          if (ctx_.isShutdown()) {
            break;
          }
        }
        prev_iter = iteration;
      }

      // in the in-memory-mode, this thread is not needed anymore, just end:
      if (config_.in_memory_mode && iteration > 0) {
        sg_log("Tile reader %lu finish job\n", thread_index_.id);
        break;
      }

      // did we run all iterations?
      if (iteration >= config_.max_iterations) {
        break;
      }

      if (config_.use_selective_scheduling) {
        if (!eval_bool_array(ctx_.tile_active_, tile_id)) {
          count_inactive_tiles++;
          // skip this tile
          continue;
        }

        // add active tiles
        count_active_tiles++;
        sg_dbg("Tile reader %lu, active tile id %lu\n", thread_index_.id,
               tile_id);
      }

      // read a batch of tiles
      size_t start_tile_id = tile_id;
      size_t end_tile_id = std::min(start_tile_id + tile_batch_size_,
                                    count_tiles_for_current_mic_);

      size_t bytes_read = read_a_batch_of_tiles(start_tile_id, end_tile_id);
      publish_perfmon(start_tile_id, end_tile_id, bytes_read);
    }
    sg_log("Shutdown TileReader %lu\n", thread_index_.id);
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileReader<APP, TVertexType, is_weighted>::publish_perfmon(
      size_t start_tile_id, size_t end_tile_id, size_t bytes_read) {
    size_t count_edges = 0;
    for (size_t tile_id = start_tile_id, i = 0; tile_id < end_tile_id;
         ++tile_id, ++i) {
      count_edges += ctx_.tile_stats_[tile_id].count_edges;
    }

    smp_faa(&ctx_.perfmon_.count_bytes_read_, bytes_read);
    smp_faa(&ctx_.perfmon_.count_edges_read_, count_edges);
    smp_faa(&ctx_.perfmon_.count_tiles_read_, end_tile_id - start_tile_id);
  }
}
}
