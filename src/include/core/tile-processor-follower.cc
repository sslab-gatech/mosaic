#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "tile-processor-follower.h"
#endif
#pragma once

#include <assert.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

#include <util/perf-event/perf-event-manager.h>
#include <util/perf-event/perf-event-scoped.h>

using namespace scalable_graphs::util::perf_event;

#define PROC_TIME_PROF 0

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, bool is_weighted>
  TileProcessorFollower<APP, TVertexType, is_weighted>::TileProcessorFollower(
      TileProcessor<APP, TVertexType, is_weighted>* tp,
      const thread_index_t& thread_index,
      pthread_barrier_t* tile_processor_barrier)
      : thread_index_(thread_index), tp_(tp), config_(tp_->config_),
        tile_processor_barrier_(tile_processor_barrier), shutdown_(false) {
    // Preallocate a private response block, write to the private block and let
    // the TileProcessor read from it afterwards.
    // The max size is both of the active fields (src, tgt) plus the size of the
    // actual vertex values.
    size_t max_size_response_block =
        sizeof(char) * MAX_VERTICES_PER_TILE +
        sizeof(char) * MAX_VERTICES_PER_TILE +
        sizeof(TVertexType) * MAX_VERTICES_PER_TILE;
    response_block_ =
        (processed_vertex_block_t*)malloc(max_size_response_block);
  }

  template <class APP, typename TVertexType, bool is_weighted>
  TileProcessorFollower<APP, TVertexType,
                        is_weighted>::~TileProcessorFollower() {
    free(response_block_);
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void
  TileProcessorFollower<APP, TVertexType, is_weighted>::getTileProcessorData() {
    // Copy over necessary fields:
    tile_partition_id_ = tp_->tile_partition_id_;
    vertex_edge_block_ = tp_->vertex_edge_block_;
    tile_stats_ = tp_->tile_stats_;
    active_vertices_src_ = tp_->active_vertices_src_;
    src_degrees_ = tp_->src_degrees_;
    tgt_degrees_ = tp_->tgt_degrees_;
    src_vertices_ = tp_->src_vertices_;
    extension_fields_ = tp_->extension_fields_;

    edge_block_ = tp_->edge_block_;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileProcessorFollower<APP, TVertexType, is_weighted>::initData() {
    response_block_->count_active_vertex_src_block =
        APP::need_active_source_block
            ? vertex_edge_block_->count_active_vertex_src_block
            : 0;
    response_block_->count_active_vertex_tgt_block =
        APP::need_active_target_block
            ? vertex_edge_block_->count_active_vertex_tgt_block
            : 0;
    response_block_->count_tgt_vertex_block = tile_stats_.count_vertex_tgt;

    size_t size_active_vertex_src_block =
        APP::need_active_source_block
            ? sizeof(char) * vertex_edge_block_->count_active_vertex_src_block
            : 0;
    size_t size_active_vertex_tgt_block =
        APP::need_active_target_block
            ? sizeof(char) * vertex_edge_block_->count_active_vertex_tgt_block
            : 0;

    response_block_->offset_active_vertices_src =
        sizeof(processed_vertex_block_t);
    response_block_->offset_active_vertices_tgt =
        response_block_->offset_active_vertices_src +
        size_active_vertex_src_block;
    response_block_->offset_vertices =
        response_block_->offset_active_vertices_tgt +
        size_active_vertex_tgt_block;

    active_vertices_src_next_ =
        APP::need_active_source_block
            ? get_array(char*, response_block_,
                        response_block_->offset_active_vertices_src)
            : NULL;
    active_vertices_tgt_next_ =
        APP::need_active_target_block
            ? get_array(char*, response_block_,
                        response_block_->offset_active_vertices_tgt)
            : NULL;
    tgt_vertices_ = get_array(TVertexType*, response_block_,
                              response_block_->offset_vertices);

    // ensure empty vertex-data as we intend to read and write from it
    APP::reset_vertices_tile_processor(tgt_vertices_,
                                       tile_stats_.count_vertex_tgt);
  }

  template <class APP, typename TVertexType, bool is_weighted>
  uint32_t
  TileProcessorFollower<APP, TVertexType, is_weighted>::process_edges() {
    uint32_t nedges_per_partition =
        tile_stats_.count_edges / vertex_edge_block_->num_tile_partition;
    uint32_t start = tile_partition_id_ * nedges_per_partition;
    uint32_t end = start + nedges_per_partition;

    // the last one will take all the rest
    if (vertex_edge_block_->num_tile_partition == (tile_partition_id_ + 1)) {
      end = tile_stats_.count_edges;
    }

    // Skip processing tiles if tile processor should not be active.
    if (config_.tile_processor_mode == TileProcessorMode::TPM_Active) {
      process_edges_range(start, end);
    }
    return (end - start) / (1 + config_.count_followers);
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void
  TileProcessorFollower<APP, TVertexType, is_weighted>::process_edges_range(
      uint32_t start, uint32_t end) {
#if PROC_TIME_PROF
    gettimeofday(&process_start, NULL);
#endif

    if (tile_stats_.use_rle) {
      process_edges_range_rle(start, end);
    } else {
      process_edges_range_list(start, end);
    }
  }

  template <class APP, typename TVertexType, bool is_weighted>
  uint32_t TileProcessorFollower<APP, TVertexType, is_weighted>::get_rle_offset(
      uint32_t start, uint32_t& tgt_count) {
    vertex_count_t* tgt_block_rle =
        get_array(vertex_count_t*, edge_block_, edge_block_->offset_tgt);

    uint32_t current_rle_offset = 0;
    uint32_t total_tgt_count = 0;
    uint32_t next_tgt_count = tgt_block_rle[current_rle_offset].count;

    // handle wrap around from using up all potential targets
    if (next_tgt_count == 0) {
      next_tgt_count = 65536;
    }

    while (total_tgt_count + next_tgt_count <= start) {
      ++current_rle_offset;
      total_tgt_count += next_tgt_count;
      next_tgt_count = tgt_block_rle[current_rle_offset].count;
      // handle wrap around from using up all potential targets
      if (next_tgt_count == 0) {
        next_tgt_count = 65536;
      }
    }
    tgt_count = start - total_tgt_count;

    return current_rle_offset;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void
  TileProcessorFollower<APP, TVertexType, is_weighted>::process_edges_range_rle(
      uint32_t start, uint32_t end) {
    local_vertex_id_t* src_block =
        get_array(local_vertex_id_t*, edge_block_, edge_block_->offset_src);
    vertex_count_t* tgt_block_rle =
        get_array(vertex_count_t*, edge_block_, edge_block_->offset_tgt);
    float* weight_block =
        is_weighted ? get_array(float*, edge_block_, edge_block_->offset_weight)
                    : NULL;

    uint32_t tgt_count = 0, rle_offset = 0;
    rle_offset = get_rle_offset(start, tgt_count);

    // With the followers scheme, every thread operates on chunks of size
    // EDGES_STRIPE_SIZE.
    int thread_count = 1 + config_.count_followers;

    // The offset is 1 (TileProcessor) plus every preceding Follower.
    uint32_t start_offset = (1 + thread_index_.id) * EDGES_STRIPE_SIZE;

    uint32_t start_index = start + start_offset;
    uint32_t end_index;
    uint32_t offset = thread_count * EDGES_STRIPE_SIZE;

    // The common rle offset, i.e. the number of edges being skipped by this
    // TileProcessor.
    uint32_t skip_rle_count = offset - EDGES_STRIPE_SIZE;

    // Advance the RLE initially:
    core::advance_rle_offset(start_offset, &tgt_count, &rle_offset,
                             tgt_block_rle);

    while (start_index < end) {
      end_index = std::min(start_index + EDGES_STRIPE_SIZE, end);

      // Loop all edges.
      for (uint32_t i = start_index; i < end_index; ++i) {
        // get args
        local_vertex_id_t src_id = src_block[i];

        if (APP::need_active_source_input) {
          // Skip if source is inactive.
          if (!eval_bool_array(active_vertices_src_, src_id)) {
            core::advance_rle_offset_once(&tgt_count, &rle_offset,
                                          tgt_block_rle);
            continue;
          }
        }
        local_vertex_id_t tgt_id = tgt_block_rle[rle_offset].id;
        TVertexType& src = src_vertices_[src_id];
        TVertexType& tgt = tgt_vertices_[tgt_id];
        vertex_degree_t* src_degree =
            APP::need_degrees_source_block ? &src_degrees_[src_id] : NULL;
        vertex_degree_t* tgt_degree =
            APP::need_degrees_target_block ? &tgt_degrees_[tgt_id] : NULL;

        // pull-gather
        if (is_weighted) {
          APP::pullGatherWeighted(
              src, tgt, weight_block[i], src_id, tgt_id, src_degree, tgt_degree,
              active_vertices_src_next_, active_vertices_tgt_next_, config_,
              extension_fields_);
        } else {
          APP::pullGather(src, tgt, src_id, tgt_id, src_degree, tgt_degree,
                          active_vertices_src_next_, active_vertices_tgt_next_,
                          config_, extension_fields_);
        }

        core::advance_rle_offset_once(&tgt_count, &rle_offset, tgt_block_rle);
      }
      start_index = start_index + offset;

      core::advance_rle_offset(skip_rle_count, &tgt_count, &rle_offset,
                               tgt_block_rle);
    }
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void
  TileProcessorFollower<APP, TVertexType,
                        is_weighted>::process_edges_range_list(uint32_t start,
                                                               uint32_t end) {
    local_vertex_id_t* src_block =
        get_array(local_vertex_id_t*, edge_block_, edge_block_->offset_src);
    local_vertex_id_t* tgt_block =
        get_array(local_vertex_id_t*, edge_block_, edge_block_->offset_tgt);
    float* weight_block =
        is_weighted ? get_array(float*, edge_block_, edge_block_->offset_weight)
                    : NULL;

    // With the followers scheme, every thread operates on chunks of size
    // EDGES_STRIPE_SIZE.
    int thread_count = 1 + config_.count_followers;

    uint32_t start_index = start + (1 + thread_index_.id) * EDGES_STRIPE_SIZE;
    uint32_t end_index;
    uint32_t offset = thread_count * EDGES_STRIPE_SIZE;
    while (start_index < end) {
      end_index = std::min(start_index + EDGES_STRIPE_SIZE, end);

      // Loop all edges.
      for (uint32_t i = start_index; i < end_index; ++i) {
        // get args
        local_vertex_id_t src_id = src_block[i];

        if (APP::need_active_source_input) {
          // Skip if source is inactive.
          if (!eval_bool_array(active_vertices_src_, src_id)) {
            continue;
          }
        }

        local_vertex_id_t tgt_id = tgt_block[i];
        TVertexType& src = src_vertices_[src_id];
        TVertexType& tgt = tgt_vertices_[tgt_id];
        vertex_degree_t* src_degree =
            APP::need_degrees_source_block ? &src_degrees_[src_id] : NULL;
        vertex_degree_t* tgt_degree =
            APP::need_degrees_target_block ? &tgt_degrees_[tgt_id] : NULL;

        // pull-gather
        if (is_weighted) {
          APP::pullGatherWeighted(
              src, tgt, weight_block[i], src_id, tgt_id, src_degree, tgt_degree,
              active_vertices_src_next_, active_vertices_tgt_next_, config_,
              extension_fields_);
        } else {
          APP::pullGather(src, tgt, src_id, tgt_id, src_degree, tgt_degree,
                          active_vertices_src_next_, active_vertices_tgt_next_,
                          config_, extension_fields_);
        }
      }
      start_index = start_index + offset;
    }
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileProcessorFollower<APP, TVertexType, is_weighted>::run() {
    bool init_shutdown = false;
    while (true) {
      // Wait for the leader to summon the follower.
      pthread_barrier_wait(tile_processor_barrier_);

      if (unlikely(shutdown_)) {
        break;
      }

      // Get the data pointers from the leader.
      getTileProcessorData();

      // Init the local array.
      initData();

      nedges_ = 0;
      {
        PerfEventScoped perf_event(
            PerfEventManager::getInstance(config_)->getRingBuffer(),
            "process_edges_follower", ComponentType::CT_TileProcessor,
            config_.mic_index, tp_->thread_index_.id,
            vertex_edge_block_->block_id, config_.enable_perf_event_collection);
        nedges_ = process_edges();
      }

      // Let the leader know his follower arrived at the barrier.
      pthread_barrier_wait(tile_processor_barrier_);
    }
    sg_log("Shutdown TileProcessorFollower %lu for TileProcessor %lu\n",
           thread_index_.id, tp_->thread_index_.id);
  }
}
}
