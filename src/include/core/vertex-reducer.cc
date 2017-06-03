#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "vertex-reducer.h"
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
#include <ring_buffer.h>
#include <util/arch.h>
#include <core/datatypes.h>
#include <core/util.h>
#include <util/arch.h>
#include <util/perf-event/perf-event-manager.h>
#include <util/perf-event/perf-event-scoped.h>

using namespace scalable_graphs::util::perf_event;

#define DO_PROCESSING 1

namespace scalable_graphs {
namespace core {

  template <class APP, typename TVertexType, typename TVertexIdType>
  VertexReducer<APP, TVertexType, TVertexIdType>::VertexReducer(
      VertexProcessor<APP, TVertexType, TVertexIdType>& ctx,
      vertex_array_t<TVertexType>* vertices, const thread_index_t& thread_index)
      : ctx_(ctx), config_(ctx_.config_), vertices_(vertices),
        thread_index_(thread_index) {
    // do nothing
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  VertexReducer<APP, TVertexType, TVertexIdType>::~VertexReducer() {
    free(response_block_);
    free(global_reducer_blocks_local_);
    free(global_reducer_blocks_remote_);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  bool VertexReducer<APP, TVertexType, TVertexIdType>::put_edge_block_index(
      uint64_t tile_id) {
    pointer_offset_t<edge_block_index_t, tile_data_vertex_engine_t>* meta_info =
        &ctx_.index_offset_table_.data_info[tile_id];
    bool no_ref = false;

    // reduce vr_refcnt for edge_block_index
    if (meta_info->meta.total_cnt > 1) {
      if (smp_faa(&meta_info->meta.vr_refcnt, -1) == 1) {
        no_ref = true;
      }
    } else {
      meta_info->meta.vr_refcnt = 0;
      no_ref = true;
    }

    // only throw away loaded tile if not using the in-memory-mode
    // when vr_refcnt drops to zero and all bundle elements have been processed,
    // reclaim
    if (!config_.in_memory_mode && no_ref) {
      if (smp_faa(meta_info->meta.bundle_refcnt, -1) == 1) {
        ring_buffer_elm_set_done(ctx_.index_rb_,
                                 (void*)meta_info->meta.bundle_raw);
      }
      meta_info->meta.data_ready = false;
      meta_info->meta.data_active = false;
      sg_dbg("VR %d: Reset tile index for block %lu\n", ctx_.edge_engine_index_,
             tile_id);
    }
    smp_wmb();

    return no_ref;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexReducer<APP, TVertexType, TVertexIdType>::preallocate() {
    size_t size_active_vertices_src_next =
        APP::need_active_source_block
            ? sizeof(char) * size_bool_array(MAX_VERTICES_PER_TILE)
            : 0;
    size_t size_active_vertices_tgt_next =
        APP::need_active_target_block
            ? sizeof(char) * size_bool_array(MAX_VERTICES_PER_TILE)
            : 0;
    size_t size_tgt_vertices = sizeof(TVertexType) * MAX_VERTICES_PER_TILE;

    size_t max_size_response_block =
        sizeof(processed_vertex_block_t) + size_active_vertices_src_next +
        size_active_vertices_tgt_next + size_tgt_vertices;

    response_block_ =
        (processed_vertex_block_t*)malloc(max_size_response_block);

    global_reducer_blocks_local_ = (processed_vertex_index_block_t**)malloc(
        sizeof(processed_vertex_index_block_t*) *
        config_.count_global_reducers);
    global_reducer_blocks_remote_ = (processed_vertex_index_block_t**)malloc(
        sizeof(processed_vertex_index_block_t*) *
        config_.count_global_reducers);

    size_t size_active_vertex_src_block =
        APP::need_active_source_block
            ? sizeof(char) * size_bool_array(MAX_VERTICES_PER_TILE)
            : 0;
    size_t size_active_vertex_tgt_block =
        APP::need_active_target_block
            ? sizeof(char) * size_bool_array(MAX_VERTICES_PER_TILE)
            : 0;
    size_t size_target_vertex_block =
        sizeof(TVertexType) * MAX_VERTICES_PER_TILE;
    size_t size_target_indices_block =
        sizeof(TVertexIdType) * MAX_VERTICES_PER_TILE;
    size_t size_source_indices_block =
        APP::need_active_source_block
            ? sizeof(TVertexIdType) * MAX_VERTICES_PER_TILE
            : 0;

    max_size_global_reducer_block_ =
        sizeof(processed_vertex_index_block_t) + size_active_vertex_src_block +
        size_active_vertex_tgt_block + size_target_vertex_block +
        size_target_indices_block + size_source_indices_block;

    for (int i = 0; i < config_.count_global_reducers; ++i) {
      global_reducer_blocks_local_[i] = (processed_vertex_index_block_t*)malloc(
          max_size_global_reducer_block_);
      global_reducer_blocks_local_[i]->shutdown = false;
      global_reducer_blocks_local_[i]->dummy = false;
      global_reducer_blocks_local_[i]->sample_execution_time = false;

      // fix offsets
      global_reducer_blocks_local_[i]->offset_active_vertices_src =
          sizeof(processed_vertex_index_block_t);
      global_reducer_blocks_local_[i]->offset_active_vertices_tgt =
          global_reducer_blocks_local_[i]->offset_active_vertices_src +
          size_active_vertex_src_block;
      global_reducer_blocks_local_[i]->offset_vertices =
          global_reducer_blocks_local_[i]->offset_active_vertices_tgt +
          size_active_vertex_tgt_block;
      global_reducer_blocks_local_[i]->offset_src_indices =
          global_reducer_blocks_local_[i]->offset_vertices +
          size_target_vertex_block;
      global_reducer_blocks_local_[i]->offset_tgt_indices =
          global_reducer_blocks_local_[i]->offset_src_indices +
          size_source_indices_block;
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void
  VertexReducer<APP, TVertexType, TVertexIdType>::initPreallocatedBlocks() {
    // set header for all global reducer blocks
    for (int i = 0; i < config_.count_global_reducers; ++i) {
      global_reducer_blocks_local_[i]->block_id = response_block_->block_id;
      global_reducer_blocks_local_[i]->count_src_vertex_block = 0;
      global_reducer_blocks_local_[i]->count_tgt_vertex_block = 0;
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  processed_block_sizes_t
  VertexReducer<APP, TVertexType, TVertexIdType>::calculateBlockSizeStruct(
      int index_global_reducer) {
    processed_block_sizes_t block_sizes;
    block_sizes.size_active_vertex_src_block =
        APP::need_active_source_block
            ? sizeof(char) *
                  size_bool_array(
                      global_reducer_blocks_local_[index_global_reducer]
                          ->count_src_vertex_block)
            : 0;
    block_sizes.size_active_vertex_tgt_block =
        APP::need_active_target_block
            ? sizeof(char) *
                  size_bool_array(
                      global_reducer_blocks_local_[index_global_reducer]
                          ->count_tgt_vertex_block)
            : 0;
    block_sizes.size_target_vertex_block =
        sizeof(TVertexType) *
        global_reducer_blocks_local_[index_global_reducer]
            ->count_tgt_vertex_block;
    block_sizes.size_target_indices_block =
        sizeof(TVertexIdType) *
        global_reducer_blocks_local_[index_global_reducer]
            ->count_tgt_vertex_block;
    block_sizes.size_source_indices_block =
        APP::need_active_source_block
            ? sizeof(TVertexIdType) *
                  global_reducer_blocks_local_[index_global_reducer]
                      ->count_src_vertex_block
            : 0;
    return block_sizes;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  size_t VertexReducer<APP, TVertexType, TVertexIdType>::calculateBlockSize(
      int index_global_reducer) {
    processed_block_sizes_t block_sizes =
        calculateBlockSizeStruct(index_global_reducer);
    return sizeof(processed_vertex_index_block_t) +
           block_sizes.size_active_vertex_src_block +
           block_sizes.size_active_vertex_tgt_block +
           block_sizes.size_target_vertex_block +
           block_sizes.size_target_indices_block +
           block_sizes.size_source_indices_block;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void
  VertexReducer<APP, TVertexType, TVertexIdType>::receive_response_block() {
    ring_buffer_req_t request_processed;
    ring_buffer_get_req_init(&request_processed, BLOCKING);
#if defined(MOSAIC_HOST_ONLY)
    ring_buffer_get(ctx_.response_rb_, &request_processed);
#else
    ring_buffer_scif_get(&ctx_.response_rb_, &request_processed);
#endif

    sg_rb_check(&request_processed);
#if !DO_PROCESSING
    ring_buffer_scif_elm_set_done(&ctx_.response_rb_, request_processed.data);
#else
    // copy from ring-buffer, set done immediately, don't need to hold the
    // space anymore
#if defined(MOSAIC_HOST_ONLY)
    int rc = copy_from_ring_buffer(ctx_.response_rb_, response_block_,
                                   request_processed.data,
                                   request_processed.size);
#else
    int rc = copy_from_ring_buffer_scif(&ctx_.response_rb_, response_block_,
                                        request_processed.data,
                                        request_processed.size);
#endif

    if (rc) {
      sg_log("copy_from_ring_buffer_scif failed in VR: %d\n", rc);
      util::die(1);
    }

#if defined(MOSAIC_HOST_ONLY)
    ring_buffer_elm_set_done(ctx_.response_rb_, request_processed.data);
#else
    ring_buffer_scif_elm_set_done(&ctx_.response_rb_, request_processed.data);
#endif
#endif
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void
  VertexReducer<APP, TVertexType, TVertexIdType>::parse_arrays_from_response() {
    active_vertices_src_next_ =
        APP::need_active_source_block
            ? get_array(char*, response_block_,
                        response_block_->offset_active_vertices_src)
            : NULL;
    active_vertices_tgt_next_ =
        APP::need_active_target_block
            ? get_array(char*, response_block_,
                        response_block_->offset_active_vertices_src)
            : NULL;
    tgt_vertices_ = get_array(TVertexType*, response_block_,
                              response_block_->offset_vertices);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexReducer<APP, TVertexType, TVertexIdType>::setProcessedBlockHeader(
      processed_vertex_index_block_t& block, int index_global_reducer) {
    // First, copy local header, then fix offset fields.
    block = *global_reducer_blocks_local_[index_global_reducer];

    processed_block_sizes_t block_sizes =
        calculateBlockSizeStruct(index_global_reducer);
    // Now set the correct offsets.
    block.offset_active_vertices_src = sizeof(processed_vertex_index_block_t);
    block.offset_active_vertices_tgt = block.offset_active_vertices_src +
                                       block_sizes.size_active_vertex_src_block;
    block.offset_vertices = block.offset_active_vertices_tgt +
                            block_sizes.size_active_vertex_tgt_block;
    block.offset_src_indices =
        block.offset_vertices + block_sizes.size_target_vertex_block;
    block.offset_tgt_indices =
        block.offset_src_indices + block_sizes.size_source_indices_block;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexReducer<APP, TVertexType,
                     TVertexIdType>::allocateGlobalReducerRingBufferSpace() {
    for (int i = 0; i < config_.count_global_reducers; ++i) {
      size_t block_size = calculateBlockSize(i);

      ring_buffer_req_t request_global_reducer_block;
      ring_buffer_put_req_init(&request_global_reducer_block, BLOCKING,
                               block_size);
      ring_buffer_put(ctx_.vd_.global_reducers_[i]->response_rb_,
                      &request_global_reducer_block);

      sg_rb_check(&request_global_reducer_block);

      global_reducer_blocks_remote_[i] =
          (processed_vertex_index_block_t*)request_global_reducer_block.data;
    }
  }
  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexReducer<APP, TVertexType, TVertexIdType>::copyGlobalReducerFields(
      int index_global_reducer) {
    // Copy the fields that are necessary to be copied.
    void* src;
    void* tgt;
    processed_block_sizes_t block_sizes =
        calculateBlockSizeStruct(index_global_reducer);
    if (APP::need_active_source_block) {
      src = (uint8_t*)global_reducer_blocks_local_[index_global_reducer] +
            global_reducer_blocks_local_[index_global_reducer]
                ->offset_active_vertices_src;
      tgt = (uint8_t*)global_reducer_blocks_remote_[index_global_reducer] +
            global_reducer_blocks_remote_[index_global_reducer]
                ->offset_active_vertices_src;

      copy_to_ring_buffer(
          ctx_.vd_.global_reducers_[index_global_reducer]->response_rb_, tgt,
          src, block_sizes.size_active_vertex_src_block);
    }

    if (APP::need_active_target_block) {
      src = (uint8_t*)global_reducer_blocks_local_[index_global_reducer] +
            global_reducer_blocks_local_[index_global_reducer]
                ->offset_active_vertices_tgt;
      tgt = (uint8_t*)global_reducer_blocks_remote_[index_global_reducer] +
            global_reducer_blocks_remote_[index_global_reducer]
                ->offset_active_vertices_tgt;

      copy_to_ring_buffer(
          ctx_.vd_.global_reducers_[index_global_reducer]->response_rb_, tgt,
          src, block_sizes.size_active_vertex_tgt_block);
    }

    // Always copy vertices.
    src = (uint8_t*)global_reducer_blocks_local_[index_global_reducer] +
          global_reducer_blocks_local_[index_global_reducer]->offset_vertices;
    tgt = (uint8_t*)global_reducer_blocks_remote_[index_global_reducer] +
          global_reducer_blocks_remote_[index_global_reducer]->offset_vertices;

    copy_to_ring_buffer(
        ctx_.vd_.global_reducers_[index_global_reducer]->response_rb_, tgt, src,
        block_sizes.size_target_vertex_block);

    // Always copy the target index block.
    src =
        (uint8_t*)global_reducer_blocks_local_[index_global_reducer] +
        global_reducer_blocks_local_[index_global_reducer]->offset_src_indices;
    tgt =
        (uint8_t*)global_reducer_blocks_remote_[index_global_reducer] +
        global_reducer_blocks_remote_[index_global_reducer]->offset_src_indices;

    copy_to_ring_buffer(
        ctx_.vd_.global_reducers_[index_global_reducer]->response_rb_, tgt, src,
        block_sizes.size_target_indices_block);

    if (APP::need_active_source_block) {
      src = (uint8_t*)global_reducer_blocks_local_[index_global_reducer] +
            global_reducer_blocks_local_[index_global_reducer]
                ->offset_tgt_indices;
      tgt = (uint8_t*)global_reducer_blocks_remote_[index_global_reducer] +
            global_reducer_blocks_remote_[index_global_reducer]
                ->offset_tgt_indices;

      copy_to_ring_buffer(
          ctx_.vd_.global_reducers_[index_global_reducer]->response_rb_, tgt,
          src, block_sizes.size_source_indices_block);
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexReducer<APP, TVertexType, TVertexIdType>::copyGlobalReducerBlocks(
      bool completed) {
    // set all blocks done for all global-reducers
    for (int i = 0; i < config_.count_global_reducers; ++i) {
      global_reducer_blocks_local_[i]->completed = completed;
      setProcessedBlockHeader(*global_reducer_blocks_remote_[i], i);
      copyGlobalReducerFields(i);

      // If this block was sampled, pass the information along to the global
      // reducer 1.
      if (i == 0 && response_block_->sample_execution_time) {
        global_reducer_blocks_remote_[i]->sample_execution_time = true;
        global_reducer_blocks_remote_[i]->processing_time_nano =
            response_block_->processing_time_nano;
        global_reducer_blocks_remote_[i]->count_edges =
            response_block_->count_edges;
      }

      ring_buffer_elm_set_ready(ctx_.vd_.global_reducers_[i]->response_rb_,
                                global_reducer_blocks_remote_[i]);
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexReducer<APP, TVertexType, TVertexIdType>::processSourceVertices() {
    // uint32_t as we get the extra one bit from another array
    uint32_t* edge_block_index_src = get_array(
        uint32_t*, edge_block_index_, edge_block_index_->offset_src_index);

    char* edge_block_index_src_upper_bits =
        get_array(char*, edge_block_index_,
                  edge_block_index_->offset_src_index_bit_extension);

    // if using the src-indices for the active-array, do a second loop to
    // only update these, using the cached src-index-block
    for (uint32_t i = 0; i < edge_block_index_->count_src_vertices; ++i) {
      TVertexIdType id_src;

      // only OR the upper bits together if they are actually in use.
      if (config_.is_index_32_bits) {
        id_src = edge_block_index_src[i];
      } else {
        id_src =
            (size_t)edge_block_index_src[i] |
            ((size_t)eval_bool_array(edge_block_index_src_upper_bits, i) << 32);
      }

      int reducerPartition =
          core::getPartitionOfVertex(id_src, config_.count_global_reducers);

      uint32_t local_id = global_reducer_blocks_local_[reducerPartition]
                              ->count_src_vertex_block++;
      // fill into the partition-block:
      // fill active-information with new local-id, fill
      // src-index-translation for this as well
      char* active_src_vertices =
          get_array(char*, global_reducer_blocks_local_[reducerPartition],
                    global_reducer_blocks_local_[reducerPartition]
                        ->offset_active_vertices_src);
      set_bool_array(active_src_vertices, local_id,
                     eval_bool_array(active_vertices_src_next_, i));

      TVertexIdType* src_indices = get_array(
          TVertexIdType*, global_reducer_blocks_local_[reducerPartition],
          global_reducer_blocks_local_[reducerPartition]->offset_src_indices);
      src_indices[local_id] = id_src;
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexReducer<APP, TVertexType, TVertexIdType>::processTargetVertices() {
    // uint32_t as we get the extra one bit from another array
    uint32_t* edge_block_index_tgt = get_array(
        uint32_t*, edge_block_index_, edge_block_index_->offset_tgt_index);

    char* edge_block_index_tgt_upper_bits =
        get_array(char*, edge_block_index_,
                  edge_block_index_->offset_tgt_index_bit_extension);

    for (uint32_t i = 0; i < edge_block_index_->count_tgt_vertices; ++i) {
// Make the compiler happy, the MPSS gcc is too old to support the necessary
// c++11 features to instantiate the neutral element properly.
#ifndef TARGET_ARCH_K1OM
      // First, check if the vertex was even touched on the Edge engine.
      if (tgt_vertices_[i] == APP::neutral_element) {
        // Skip current vertex if it was not touched.
        continue;
      }
#endif

      TVertexIdType id_tgt;

      // only OR the upper bits together if they are actually in use.
      if (config_.is_index_32_bits) {
        id_tgt = edge_block_index_tgt[i];
      } else {
        id_tgt =
            (size_t)edge_block_index_tgt[i] |
            ((size_t)eval_bool_array(edge_block_index_tgt_upper_bits, i) << 32);
      }

      int reducerPartition =
          core::getPartitionOfVertex(id_tgt, config_.count_global_reducers);

      // fill into the partition-block:
      // fill active-information with new local-id, fill
      // src-index-translation for this as well
      TVertexIdType* tgt_indices = get_array(
          TVertexIdType*, global_reducer_blocks_local_[reducerPartition],
          global_reducer_blocks_local_[reducerPartition]->offset_tgt_indices);

      TVertexType* vertices = get_array(
          TVertexType*, global_reducer_blocks_local_[reducerPartition],
          global_reducer_blocks_local_[reducerPartition]->offset_vertices);

      uint32_t local_id = global_reducer_blocks_local_[reducerPartition]
                              ->count_tgt_vertex_block++;
      tgt_indices[local_id] = id_tgt;

      if (APP::need_active_target_block) {
        char* active_tgt_vertices =
            get_array(char*, global_reducer_blocks_local_[reducerPartition],
                      global_reducer_blocks_local_[reducerPartition]
                          ->offset_active_vertices_tgt);
        // first update active-status for next round
        set_bool_array(active_tgt_vertices, local_id,
                       eval_bool_array(active_vertices_tgt_next_, i));
      }

      vertices[local_id] = tgt_vertices_[i];
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexReducer<APP, TVertexType, TVertexIdType>::reduceTargetVertices() {
    uint32_t* edge_block_index_tgt = get_array(
        uint32_t*, edge_block_index_, edge_block_index_->offset_tgt_index);

    char* edge_block_index_tgt_upper_bits =
        get_array(char*, edge_block_index_,
                  edge_block_index_->offset_tgt_index_bit_extension);

    for (uint32_t i = 0; i < edge_block_index_->count_tgt_vertices; ++i) {
      TVertexIdType id_tgt;

      // only OR the upper bits together if they are actually in use.
      if (config_.is_index_32_bits) {
        id_tgt = edge_block_index_tgt[i];
      } else {
        id_tgt =
            (size_t)edge_block_index_tgt[i] |
            ((size_t)eval_bool_array(edge_block_index_tgt_upper_bits, i) << 32);
      }

      if (APP::need_active_target_block) {
        // first update active-status for next round
        bool old_active_status =
            eval_bool_array(vertices_->active_next, id_tgt);
        bool new_active_status =
            old_active_status || eval_bool_array(active_vertices_tgt_next_, i);
        set_bool_array(vertices_->active_next, id_tgt, new_active_status);
      }

      if (config_.local_reducer_mode == LocalReducerMode::LRM_Locking) {
        // first, lock the vertex in question to circumvent concurrent
        // updates to the same vertex
        pthread_spinlock_t* spinlock =
            core::getSpinlockForVertex(id_tgt, ctx_.vd_.vertex_lock_table);

        pthread_spin_lock(spinlock);

        APP::reduceVertex(vertices_->next[id_tgt], tgt_vertices_[i],
                          vertices_->next[id_tgt], id_tgt,
                          vertices_->degrees[id_tgt], vertices_->active_next,
                          config_);

        pthread_spin_unlock(spinlock);
      } else if (config_.local_reducer_mode == LocalReducerMode::LRM_Atomic) {
        bool successful = false;

        do {
          TVertexType* val_ptr = &vertices_->next[id_tgt];
          TVertexType old_value = vertices_->next[id_tgt];
          TVertexType new_value;

          APP::reduceVertex(new_value, tgt_vertices_[i], *val_ptr, id_tgt,
                            vertices_->degrees[id_tgt], vertices_->active_next,
                            config_);
          successful = smp_cas(reinterpret_cast<uint32_t*>(val_ptr),
                               *reinterpret_cast<const uint32_t*>(&old_value),
                               *reinterpret_cast<const uint32_t*>(&new_value));
        } while (!successful);
      }
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexReducer<APP, TVertexType, TVertexIdType>::sendDummyBlock(
      bool completed) {
    for (int i = 0; i < config_.count_global_reducers; ++i) {
      size_t block_size = sizeof(processed_vertex_index_block_t);

      ring_buffer_req_t request_global_reducer_block;
      ring_buffer_put_req_init(&request_global_reducer_block, BLOCKING,
                               block_size);
      ring_buffer_put(ctx_.vd_.global_reducers_[i]->response_rb_,
                      &request_global_reducer_block);

      sg_rb_check(&request_global_reducer_block);

      processed_vertex_index_block_t* block =
          (processed_vertex_index_block_t*)request_global_reducer_block.data;
      block->block_id = response_block_->block_id;
      block->completed = completed;
      block->count_src_vertex_block = 0;
      block->count_tgt_vertex_block = 0;
      block->sample_execution_time = false;

      ring_buffer_elm_set_ready(ctx_.vd_.global_reducers_[i]->response_rb_,
                                block);
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexReducer<APP, TVertexType, TVertexIdType>::run() {
    preallocate();

    int barrier_rc = pthread_barrier_wait(&ctx_.vd_.memory_init_barrier_);

    if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
      ctx_.vd_.initTimers();
    }

    // wait for responses

    while (true) {
      sg_print("Waiting for responses\n");
      receive_response_block();
      scoped_profile_tid_meta(ComponentType::CT_VertexReducer, "tile",
                              response_block_->block_id,
                              response_block_->count_tgt_vertex_block);

      // Break on shutdown.
      if (response_block_->shutdown) {
        break;
      }

      // Die if magic is corrupted.
      if (response_block_->magic_identifier != MAGIC_IDENTIFIER) {
        sg_log("Magic identifier doesn't match: %ld vs. %ld (expected)",
               response_block_->magic_identifier, MAGIC_IDENTIFIER);
        util::die(1);
      }

#if DO_PROCESSING
      {
        scoped_profile_tid(ComponentType::CT_VertexReducer, "init",
                           response_block_->block_id);
        parse_arrays_from_response();
        initPreallocatedBlocks();
      }

      sg_dbg("Processing response for block %lu\n", response_block_->block_id);

      pointer_offset_t<edge_block_index_t, tile_data_vertex_engine_t>*
          meta_info =
              &ctx_.index_offset_table_.data_info[response_block_->block_id];

      while (!meta_info->meta.data_ready) {
        pthread_yield();
        smp_rmb();
      }

      edge_block_index_ = const_cast<edge_block_index_t*>(meta_info->data);

      if (APP::need_active_source_block) {
        processSourceVertices();
      }

      if (config_.local_reducer_mode == LocalReducerMode::LRM_GlobalReducer) {
        {
          scoped_profile_tid(ComponentType::CT_VertexReducer, "process_target",
                             response_block_->block_id);
          // now process target-indices
          processTargetVertices();
        }
      } else {
        reduceTargetVertices();
      }
      // put reference of edge block index
      bool completed = put_edge_block_index(response_block_->block_id);

      if (config_.local_reducer_mode == LocalReducerMode::LRM_GlobalReducer) {
        {
          scoped_profile_tid(ComponentType::CT_VertexReducer, "allocate",
                             response_block_->block_id);
          allocateGlobalReducerRingBufferSpace();
        }
        {
          scoped_profile_tid(ComponentType::CT_VertexReducer, "copy",
                             response_block_->block_id);
          copyGlobalReducerBlocks(completed);
        }
      } else {
        // Send dummy block when GlobalReducer is not active.
        sendDummyBlock(completed);
      }

      // done with processing response, let it be reclaimed
      sg_dbg("Done processing response for block %lu\n",
             response_block_->block_id);
#endif
    }

    sg_log("Shutdown VertexReducer %lu\n", thread_index_.id);
  }
}
}
