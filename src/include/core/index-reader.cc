#pragma once

#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "index-reader.h"
#endif

#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <util/perf-event/perf-event-manager.h>
#include <util/perf-event/perf-event-scoped.h>

using namespace scalable_graphs::util::perf_event;

#define DO_PROCESSING_IR 1

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, typename TVertexIdType>
  IndexReader<APP, TVertexType, TVertexIdType>::IndexReader(
      VertexProcessor<APP, TVertexType, TVertexIdType>& ctx,
      const thread_index_t& thread_index)
      : ReaderBase(thread_index), config_(ctx.config_), ctx_(ctx) {
    count_tiles_for_current_mic_ =
        core::countTilesPerMic(config_, ctx_.edge_engine_index_);

    // According to selective scheduling mode, change batch size and batch per
    // iter.
    if (config_.use_selective_scheduling) {
      tile_batch_size_ = 1;
    } else {
      tile_batch_size_ = 16;
    }

    num_batch_per_iter_ =
        ceil(double(count_tiles_for_current_mic_) / double(tile_batch_size_));
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  IndexReader<APP, TVertexType, TVertexIdType>::~IndexReader() {
    // do nothing
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void IndexReader<APP, TVertexType, TVertexIdType>::initDataFromParent() {
    tile_offsets_ = ctx_.tile_offsets_;
    tiles_offset_table_ = ctx_.index_offset_table_;
    rb_ = ctx_.index_rb_;
    fd_ = ctx_.meta_fd_;
    reader_progress_ = &ctx_.index_reader_progress_;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  size_t
  IndexReader<APP, TVertexType, TVertexIdType>::get_tile_size(size_t tile_id) {
    // step 1: find the current tile status
    tile_stats_t tile_stats = ctx_.tile_stats_[tile_id];

    // step 2: calculate required space to fetch the index blocks
    size_t size_edge_index_src_vertex_block =
        sizeof(uint32_t) * tile_stats.count_vertex_src;
    size_t size_edge_index_tgt_vertex_block =
        sizeof(uint32_t) * tile_stats.count_vertex_tgt;
    size_t size_edge_index_src_extended_block =
        ctx_.vd_.config_.is_index_32_bits
            ? 0
            : size_bool_array(tile_stats.count_vertex_src);
    size_t size_edge_index_tgt_extended_block =
        ctx_.vd_.config_.is_index_32_bits
            ? 0
            : size_bool_array(tile_stats.count_vertex_tgt);
    size_t size_index_block =
        sizeof(edge_block_index_t) + size_edge_index_src_vertex_block +
        size_edge_index_tgt_vertex_block + size_edge_index_src_extended_block +
        size_edge_index_tgt_extended_block;
    size_t size_rb_block = int_ceil(size_index_block, PAGE_SIZE);

    return size_rb_block;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void IndexReader<APP, TVertexType, TVertexIdType>::run() {
    size_t cur_iter = 0;
    size_t prev_iter = 0;
    size_t tile_id;

    // Wait for all memory inits to be done.
    int barrier_rc = pthread_barrier_wait(&ctx_.vd_.memory_init_barrier_);

    if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
      ctx_.vd_.initTimers();
    }

    // Wait for everything to be initialized before localizing pointers.
    initDataFromParent();

    while (true) {
      // grab a tile
      tile_id = grab_a_tile(cur_iter);
      scoped_profile(ComponentType::CT_IndexReader, "index");

      // end of iteration?
      if (cur_iter != prev_iter) {
        // done with all tiles, wait for next round:
        sg_dbg("Index Reader Done with round %lu\n", prev_iter);

        if (config_.use_selective_scheduling) {
          // in selective scheduling, wait for the apply-period to end before
          // advancing to the next round
          pthread_barrier_wait(&ctx_.vd_.end_apply_barrier_);

          // In case of shutdown, break after the end_apply_barrier, set by the
          // VertexApplier.
          if (ctx_.isShutdown()) {
            break;
          }
        }

        // did we run all iterations?
        if (cur_iter >= config_.max_iterations) {
          break;
        }

        if (config_.in_memory_mode) {
          fprintf(stderr, "[SG-LOG] break index reader");
          break;
        }

        // greeting a new iteration!
        sg_dbg("Index Reader Starting round %lu\n", cur_iter);
      }
      prev_iter = cur_iter;

      // if selective-scheduling is enabled,
      // check if the file even should be read
      if (config_.use_selective_scheduling) {
        if (!eval_bool_array(ctx_.tile_active_current_, tile_id))
          continue;
        sg_dbg("index reader %d, active tiles %lu\n", ctx_.edge_engine_index_,
               tile_id);
      }

      // read a batch of tiles
      size_t start_tile_id = tile_id;
      size_t end_tile_id = std::min(start_tile_id + tile_batch_size_,
                                    count_tiles_for_current_mic_);

      {
        scoped_profile(ComponentType::CT_IndexReader, "read_index");
        size_t bytes_read = read_a_batch_of_tiles(start_tile_id, end_tile_id);
      }
    }
    sg_log("Exit IndexReader %lu\n", thread_index_.id);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void IndexReader<APP, TVertexType, TVertexIdType>::on_before_publish_data(
      edge_block_index_t* data, const size_t tile_id) {}
}
}
