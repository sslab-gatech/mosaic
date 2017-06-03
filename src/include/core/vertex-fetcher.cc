#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "vertex-fetcher.h"
#endif
#pragma once

#include <assert.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <cmath>
#include <pthread.h>
#include <sys/stat.h>
#include <ring_buffer.h>
#include <util/arch.h>
#include <core/datatypes.h>
#include <core/util.h>
#include <util/perf-event/perf-event-manager.h>
#include <util/perf-event/perf-event-scoped.h>

using namespace scalable_graphs::util::perf_event;

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, typename TVertexIdType>
  VertexFetcher<APP, TVertexType, TVertexIdType>::VertexFetcher(
      VertexProcessor<APP, TVertexType, TVertexIdType>& ctx,
      vertex_array_t<TVertexType>* vertices, tile_stats_t* tile_stats,
      const thread_index_t& thread_index)
      : ctx_(ctx), vertices_(vertices), thread_index_(thread_index),
        offset_indices_(NULL), fetch_requests_(NULL),
        fetch_requests_vertices_(NULL), src_vertices_aggregate_block_(NULL),
        config_(ctx_.config_), tile_break_point_(0) {

    int rc =
        ring_buffer_create(response_rb_size_, PAGE_SIZE, RING_BUFFER_BLOCKING,
                           NULL, NULL, &response_rb_);
    if (rc) {
      scalable_graphs::util::die(1);
    }

    count_tiles_for_current_mic_ =
        core::countTilesPerMic(config_, ctx_.edge_engine_index_);

    // Set tile breakpoint.
    tile_break_point_ = 1250000;
    sg_log("VF: tile break point = %lu\n", tile_break_point_);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  VertexFetcher<APP, TVertexType, TVertexIdType>::~VertexFetcher() {
    for (int i = 0; i < config_.count_global_fetchers; ++i) {
      if (offset_indices_) {
        free(offset_indices_[i]);
      }
      if (fetch_requests_) {
        free(fetch_requests_[i]);
      }
    }
    delete[] offset_indices_;
    delete[] fetch_requests_;
    delete[] fetch_requests_vertices_;
    free(src_vertices_aggregate_block_);
    free(tile_block_);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexFetcher<APP, TVertexType, TVertexIdType>::updateTileBreakPoint() {
    tile_break_point_ = ctx_.vd_.tile_break_point_;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  uint32_t
  VertexFetcher<APP, TVertexType, TVertexIdType>::calc_num_percessors_per_tile(
      int tile_id) {
    if (config_.enable_tile_partitioning) {
      uint32_t nproc = std::ceil(tile_stats_[tile_id].count_edges /
                                 (double)tile_break_point_);
      sg_dbg("TilePartitioning: tile %d has %d edges and is processed by %d "
             "processors\n",
             tile_id, tile_stats_[tile_id].count_edges, nproc);
      return nproc > 0 ? nproc : 1;
    } else {
      return 1;
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexFetcher<APP, TVertexType, TVertexIdType>::init() {
    // Allocate and localize tile stats.
    size_t size_tile_stats =
        sizeof(tile_stats_t) * count_tiles_for_current_mic_;
    tile_stats_ = (tile_stats_t*)malloc(size_tile_stats);

    tile_stats_ =
        (tile_stats_t*)memcpy(tile_stats_, ctx_.tile_stats_, size_tile_stats);

    // Preallocate tile block to reuse.
    vertex_edge_tiles_block_sizes_t sizes =
        getMaxTileBlockSizes<APP, TVertexType>();
    size_t max_size_tile_block = getSizeTileBlock(sizes);

    tile_block_ = (vertex_edge_tiles_block_t*)malloc(max_size_tile_block);

    // pre-allocate blocks for requesting vertices from the global array
    size_t size_fetch_request =
        sizeof(fetch_vertices_request_t) + sizeof(TVertexIdType) * UINT16_MAX;

    offset_indices_ = new uint16_t*[config_.count_global_fetchers];
    fetch_requests_ =
        new fetch_vertices_request_t*[config_.count_global_fetchers];
    fetch_requests_vertices_ =
        new TVertexIdType*[config_.count_global_fetchers];

    size_t size_offset_index = sizeof(uint16_t) * UINT16_MAX;

    // initialize fields
    for (int i = 0; i < config_.count_global_fetchers; ++i) {
      offset_indices_[i] = (uint16_t*)malloc(size_offset_index);

      fetch_requests_[i] =
          (fetch_vertices_request_t*)malloc(size_fetch_request);
      fetch_requests_[i]->shutdown = false;
      fetch_requests_[i]->response_ring_buffer = response_rb_;
      fetch_requests_[i]->offset_request_vertices =
          sizeof(fetch_vertices_request_t);

      fetch_requests_vertices_[i] =
          get_array(TVertexIdType*, fetch_requests_[i],
                    fetch_requests_[i]->offset_request_vertices);
    }

    // also pre-allocate block for storing the repsonse into
    size_t max_size_src_vertices_block = sizeof(TVertexType) * UINT16_MAX;
    src_vertices_aggregate_block_ =
        (TVertexType*)malloc(max_size_src_vertices_block);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  bool VertexFetcher<APP, TVertexType, TVertexIdType>::sample_current_tile() {
    double rand = rand32_seedless() / (double)UINT32_MAX;
    return (rand < SAMPLE_THRESHOLD);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void
  VertexFetcher<APP, TVertexType, TVertexIdType>::send_tile_block(int tile_id,
                                                                  size_t len) {
    // properly set up pointer table
    pointer_offset_t<edge_block_index_t, tile_data_vertex_engine_t>* meta_info =
        &ctx_.index_offset_table_.data_info[tile_id];
    meta_info->meta.total_cnt = tile_block_->num_tile_partition;
    meta_info->meta.vr_refcnt = tile_block_->num_tile_partition;
    smp_wmb();

    // send partitioned tile blocks to tile-processors
    ring_buffer_req_t tiles_req;
    size_t local_len = len;
    for (uint32_t tpid = 0; tpid < tile_block_->num_tile_partition; ++tpid) {
      // fill tile processing information
      tile_block_->tile_partition_id = tpid;
      tile_block_->sample_execution_time = sample_current_tile();

      // push a tile block
      ring_buffer_put_req_init(&tiles_req, BLOCKING, local_len);
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_put(ctx_.tiles_data_rb_, &tiles_req);
#else
      ring_buffer_scif_put(&ctx_.tiles_data_rb_, &tiles_req);
#endif
      sg_rb_check(&tiles_req);

#if defined(MOSAIC_HOST_ONLY)
      int rc = copy_to_ring_buffer(ctx_.tiles_data_rb_, tiles_req.data,
                                   tile_block_, local_len);
#else
      int rc = copy_to_ring_buffer_scif(&ctx_.tiles_data_rb_, tiles_req.data,
                                        tile_block_, local_len);
#endif
      if (rc) {
        sg_log("Copy to ringbuffer failed in VF: %d\n", rc);
        util::die(1);
      }
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_elm_set_ready(ctx_.tiles_data_rb_, tiles_req.data);
#else
      ring_buffer_scif_elm_set_ready(&ctx_.tiles_data_rb_, tiles_req.data);
#endif
    }
    smp_faa(&ctx_.vd_.perfmon_.count_tile_partitions_sent_,
            tile_block_->num_tile_partition);

    if (tile_block_->num_tile_partition > 1) {
      sg_dbg("Tile %lu will be processed by %d processors\n",
             tile_block_->block_id, tile_block_->num_tile_partition);
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  edge_block_index_t*
  VertexFetcher<APP, TVertexType, TVertexIdType>::get_edge_block_index(
      int tile_id) {
    pointer_offset_t<edge_block_index_t, tile_data_vertex_engine_t>* meta_info =
        &ctx_.index_offset_table_.data_info[tile_id];

    sg_dbg("Wait for index for block %d on %d\n", tile_id,
           ctx_.edge_engine_index_);
    smp_rmb();
    while (!meta_info->meta.data_ready) {
      pthread_yield();
      smp_rmb();
    }
    volatile edge_block_index_t* edge_block_index = meta_info->data;
    sg_dbg("Got index for block %d on %d\n", tile_id, ctx_.edge_engine_index_);

    return const_cast<edge_block_index_t*>(edge_block_index);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexFetcher<APP, TVertexType, TVertexIdType>::fetch_source_vertex_info(
      int tile_id) {
    // Only request information from global fetcher if instructed to do so,
    // otherwise this information is being fetched directly.
    // XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
    // XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
    // XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
    // XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
    // XXX following code should be rewritten in an async. style.

    // now go to all global-fetchers and enqueue the request there:
    ring_buffer_req_t put_fetch_req;
    for (int i = 0; i < config_.count_global_fetchers; ++i) {
      size_t size_fetch_request =
          sizeof(fetch_vertices_request_t) +
          sizeof(TVertexIdType) * fetch_requests_[i]->count_vertices;

      ring_buffer_put_req_init(&put_fetch_req, BLOCKING, size_fetch_request);
      ring_buffer_put(ctx_.vd_.global_fetchers_[i]->request_rb_,
                      &put_fetch_req);
      sg_rb_check(&put_fetch_req);

      copy_to_ring_buffer(ctx_.vd_.global_fetchers_[i]->request_rb_,
                          put_fetch_req.data, fetch_requests_[i],
                          size_fetch_request);

      ring_buffer_elm_set_ready(ctx_.vd_.global_fetchers_[i]->request_rb_,
                                put_fetch_req.data);
    }
    // wait for exactly $count_global_fetchers-many responses
    ring_buffer_req_t req_resp;
    for (int i = 0; i < config_.count_global_fetchers; ++i) {
      ring_buffer_get_req_init(&req_resp, BLOCKING);
      ring_buffer_get(response_rb_, &req_resp);
      sg_rb_check(&req_resp);

      fetch_vertices_response_t* response =
          (fetch_vertices_response_t*)req_resp.data;
      TVertexType* fetched_src_vertices =
          get_array(TVertexType*, response, response->offset_vertex_responses);

      // iterate response, translate contiguos array back to original
      // position in src-vertices-block
      for (uint32_t j = 0; j < response->count_vertices; ++j) {
        uint16_t local_index = offset_indices_[response->global_fetcher_id][j];

        src_vertices_aggregate_block_[local_index] = fetched_src_vertices[j];
      }

      // done with this response
      ring_buffer_elm_set_done(response_rb_, req_resp.data);
    }

    // XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
    // XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
    // XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
    // XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  size_t VertexFetcher<APP, TVertexType, TVertexIdType>::fill_tile_block_header(
      int tile_id) {
    tile_stats_t tile_stats = tile_stats_[tile_id];

    vertex_edge_tiles_block_sizes_t sizes =
        getTileBlockSizes<APP, TVertexType>(tile_stats);

    uint32_t tile_partition_id = -1;

    // allocate header
    fillTileBlockHeader(tile_block_, tile_id, tile_stats, sizes,
                        calc_num_percessors_per_tile(tile_id),
                        tile_partition_id);

    return getSizeTileBlock(sizes);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexFetcher<APP, TVertexType, TVertexIdType>::fetch_indices(
      edge_block_index_t* edge_block_index, int tile_id) {
    // uint32_t as we get the extra one bit from another array
    uint32_t* edge_block_index_src = get_array(
        uint32_t*, edge_block_index, edge_block_index->offset_src_index);

    char* edge_block_index_src_upper_bits =
        get_array(char*, edge_block_index,
                  edge_block_index->offset_src_index_bit_extension);

    // request vertices from global-fetchers
    // first, init/reset fields
    for (int i = 0; i < config_.count_global_fetchers; ++i) {
      fetch_requests_[i]->block_id = tile_id;
      fetch_requests_[i]->count_vertices = 0;
    }

    // fetch indices and assign them to the various global-entities
    for (uint32_t i = 0; i < edge_block_index->count_src_vertices; ++i) {
      TVertexIdType id;

      // only OR the upper bits together if they are actually in use.
      if (config_.is_index_32_bits) {
        id = edge_block_index_src[i];
      } else {
        id =
            (size_t)edge_block_index_src[i] |
            ((size_t)eval_bool_array(edge_block_index_src_upper_bits, i) << 32);
      }

      int global_fetcher_id =
          core::getPartitionOfVertex(id, config_.count_global_fetchers);

      int array_index = fetch_requests_[global_fetcher_id]->count_vertices;
      ++fetch_requests_[global_fetcher_id]->count_vertices;

      fetch_requests_vertices_[global_fetcher_id][array_index] = id;

      offset_indices_[global_fetcher_id][array_index] = i;
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexFetcher<APP, TVertexType, TVertexIdType>::fill_source_fields(
      edge_block_index_t* edge_block_index) {
    char* active_vertices_src =
        APP::need_active_source_input
            ? get_array(char*, tile_block_,
                        tile_block_->offset_active_vertices_src)
            : 0;
    vertex_degree_t* src_degrees =
        APP::need_degrees_source_block
            ? get_array(vertex_degree_t*, tile_block_,
                        tile_block_->offset_src_degrees)
            : NULL;

    TVertexType* src_vertices = get_array(
        TVertexType*, tile_block_, tile_block_->offset_source_vertex_block);
    uint32_t* edge_block_index_src = get_array(
        uint32_t*, edge_block_index, edge_block_index->offset_src_index);
    char* edge_block_index_src_upper_bits =
        get_array(char*, edge_block_index,
                  edge_block_index->offset_src_index_bit_extension);

    for (uint32_t i = 0; i < edge_block_index->count_src_vertices; ++i) {
      TVertexIdType id;

      // only OR the upper bits together if they are actually in use.
      if (config_.is_index_32_bits) {
        id = edge_block_index_src[i];
      } else {
        id =
            (size_t)edge_block_index_src[i] |
            ((size_t)eval_bool_array(edge_block_index_src_upper_bits, i) << 32);
      }

      // Fill active data.
      bool src_active;
      if (APP::need_active_source_input) {
        src_active = eval_bool_array(vertices_->active_current, id);
        set_bool_array(active_vertices_src, i, src_active);
      } else {
        src_active = true;
      }

      if (APP::need_degrees_source_block) {
        src_degrees[i] = vertices_->degrees[id];
      }
      if (src_active) {
        // Switch between using result from global fetcher or directly go to
        // array.
        if (config_.local_fetcher_mode == LocalFetcherMode::LFM_GlobalFetcher) {
          src_vertices[i] = src_vertices_aggregate_block_[i];
        } else if (config_.local_fetcher_mode ==
                   LocalFetcherMode::LFM_DirectAccess) {
          src_vertices[i] = vertices_->current[id];
        } else if (config_.local_fetcher_mode ==
                   LocalFetcherMode::LFM_ConstantValue) {
          src_vertices[i] = 0.5;
        } else {
#ifndef TARGET_ARCH_K1OM
          src_vertices[i] = APP::neutral_element;
#endif
        }
      }
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexFetcher<APP, TVertexType, TVertexIdType>::fill_target_fields(
      edge_block_index_t* edge_block_index) {
    char* active_vertices_tgt =
        APP::need_active_target_block
            ? get_array(char*, tile_block_,
                        tile_block_->offset_active_vertices_tgt)
            : NULL;
    vertex_degree_t* tgt_degrees =
        APP::need_degrees_target_block
            ? get_array(vertex_degree_t*, tile_block_,
                        tile_block_->offset_tgt_degrees)
            : NULL;
    uint32_t* edge_block_index_tgt = get_array(
        uint32_t*, edge_block_index, edge_block_index->offset_tgt_index);
    char* edge_block_index_tgt_upper_bits =
        get_array(char*, edge_block_index,
                  edge_block_index->offset_tgt_index_bit_extension);
    bool need_target_index =
        APP::need_active_target_block || APP::need_degrees_target_block;

    if (need_target_index) {
      for (uint32_t i = 0; i < edge_block_index->count_tgt_vertices; ++i) {
        TVertexIdType id;

        // only OR the upper bits together if they are actually in use.
        if (config_.is_index_32_bits) {
          id = edge_block_index_tgt[i];
        } else {
          id = (size_t)edge_block_index_tgt[i] |
               ((size_t)eval_bool_array(edge_block_index_tgt_upper_bits, i)
                << 32);
        }

        if (APP::need_active_target_block) {
          // fill active+data
          set_bool_array(active_vertices_tgt, i,
                         eval_bool_array(vertices_->active_current, id));
        }
        if (APP::need_degrees_target_block) {
          tgt_degrees[i] = vertices_->degrees[id];
        }
      }
    }

    if (APP::need_vertex_block_extension_fields) {
      void* extension_block =
          APP::need_vertex_block_extension_fields
              ? get_array(void*, tile_block_, tile_block_->offset_extensions)
              : NULL;

      uint32_t* edge_block_index_src = get_array(
          uint32_t*, edge_block_index, edge_block_index->offset_src_index);

      APP::fillExtensionFieldsVertexBlock(extension_block, edge_block_index,
                                          edge_block_index_src,
                                          edge_block_index_tgt, vertices_);
    }
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexFetcher<APP, TVertexType, TVertexIdType>::fetch_vertices_for_tile(
      int tile_id) {

    sg_dbg("Filling tile %d on %d\n", tile_id, ctx_.edge_engine_index_);

    size_t tile_block_len = fill_tile_block_header(tile_id);
    edge_block_index_t* edge_block_index;
    {
      scoped_profile(ComponentType::CT_VertexFetcher, "get_edge_block_index");
      edge_block_index = get_edge_block_index(tile_id);
    }

    if (config_.local_fetcher_mode == LocalFetcherMode::LFM_GlobalFetcher) {
      scoped_profile(ComponentType::CT_VertexFetcher, "fetch_index_source");
      fetch_indices(edge_block_index, tile_id);
      fetch_source_vertex_info(tile_id);
    }
    {
      scoped_profile(ComponentType::CT_VertexFetcher, "fill");
      fill_source_fields(edge_block_index);
      fill_target_fields(edge_block_index);
    }
    {
      scoped_profile(ComponentType::CT_VertexFetcher, "send_tile_block");
      send_tile_block(tile_id, tile_block_len);
    }

    /* tile accounting */
    smp_faa(&ctx_.vd_.perfmon_.count_tiles_fetched_, 1);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  size_t VertexFetcher<APP, TVertexType, TVertexIdType>::grab_a_tile(
      size_t& iteration) {
    uint64_t cnt = ctx_.fetcher_progress_.inc();
    iteration = cnt / count_tiles_for_current_mic_;
    return cnt % count_tiles_for_current_mic_;
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexFetcher<APP, TVertexType, TVertexIdType>::run() {
    size_t cur_iter = 0;
    size_t prev_iter = 0;
    size_t tile_id;

    // Init the VertexProcessor first, elect a single VertexFetcher to do
    // this.
    int barrier_rc = pthread_barrier_wait(&ctx_.fetchers_barrier_);

    if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
      ctx_.init();

      // Join onto init_algorithm_barrier to signal init complete for the
      // current VertexProcessors.
      pthread_barrier_wait(&ctx_.vd_.init_algorithm_barrier_);

      // Wwait for the active tiles to be initialized, then copy the tiles to
      // remote.
      pthread_barrier_wait(&ctx_.vd_.init_active_tiles_barrier_);

      if (config_.use_selective_scheduling) {
        // A count of 1 is enough, simply make sure the Edge Engine is not
        // asked
        // to shut down immediately.
        ctx_.sendActiveTiles(1);
      }
    }

    // Wait until the parent is properly initialized.
    pthread_barrier_wait(&ctx_.fetchers_barrier_);
    init();
    updateTileBreakPoint();

    // Wait for all memory inits to be done.
    barrier_rc = pthread_barrier_wait(&ctx_.vd_.memory_init_barrier_);

    if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
      ctx_.vd_.initTimers();
    }

    sg_log("Updater started on %d, count: %lu\n", ctx_.edge_engine_index_,
           count_tiles_for_current_mic_);
    sg_dbg("-----------------\nStarting round %d\n", 0);

    // for an iteration
    while (true) {
      // grab a tile
      tile_id = grab_a_tile(cur_iter);

      scoped_profile_meta(ComponentType::CT_VertexFetcher, "tile",
                          tile_stats_[tile_id].count_vertex_src);

      // end of iteration?
      if (cur_iter != prev_iter) {
        // done with all tiles, wait for next round:
        sg_dbg("Done with round %lu\n", prev_iter);
        sg_dbg("Count tile_partitions: %lu\n",
               ctx_.vd_.perfmon_.count_tile_partitions_sent_);

        pthread_barrier_wait(&ctx_.vd_.end_apply_barrier_);
        // Update the tile break point from the VertexDomain.
        updateTileBreakPoint();

        // If the system is shutdown, break.
        if (ctx_.isShutdown()) {
          // Shutdown the remote side as well.
          shutdown();
          break;
        }

        // greeting a new iteration!
        sg_dbg("-----------------\nStarting round %lu\n", cur_iter);
      }
      prev_iter = cur_iter;

      if (config_.use_selective_scheduling) {
        if (!eval_bool_array(ctx_.tile_active_current_, tile_id)) {
          // Inform global-reducer, all of them are waiting for exactly
          // count_tiles-many responses, simply send empty response
          // TODO: it's better to create dummy processed_block for reducers
          for (int i = 0; i < config_.count_global_reducers; ++i) {
            ring_buffer_req_t request_global_reducer_block;
            ring_buffer_put_req_init(&request_global_reducer_block, BLOCKING,
                                     sizeof(processed_vertex_block_t));
            ring_buffer_put(ctx_.vd_.global_reducers_[i]->response_rb_,
                            &request_global_reducer_block);

            sg_rb_check(&request_global_reducer_block);

            processed_vertex_index_block_t* global_reducer_block =
                (processed_vertex_index_block_t*)
                    request_global_reducer_block.data;

            // set header
            global_reducer_block->block_id = tile_id;
            global_reducer_block->shutdown = false;
            global_reducer_block->completed = true;
            global_reducer_block->dummy = true;
            global_reducer_block->count_src_vertex_block = 0;
            global_reducer_block->count_tgt_vertex_block = 0;
            global_reducer_block->offset_src_indices = 0;
            global_reducer_block->offset_tgt_indices = 0;

            ring_buffer_elm_set_ready(
                ctx_.vd_.global_reducers_[i]->response_rb_,
                request_global_reducer_block.data);
          }
          // then, skip current iteration
          continue;
        }
        sg_dbg("Vertex-Fetcher %lu, active tiles %lu\n", thread_index_.id,
               tile_id);
      }

      // fetch vertices and send it to edge processor
      fetch_vertices_for_tile(tile_id);
    }
    sg_log("Shutdown VertexFetcher %lu\n", thread_index_.id);
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void VertexFetcher<APP, TVertexType, TVertexIdType>::shutdown() {
    // The VertexFetcher with id == 0 will shutdown all tile processors.
    if (thread_index_.id != 0) {
      return;
    }

    // Send shutdown request to all tile processors.
    ring_buffer_req_t tiles_req;
    size_t size_block = sizeof(vertex_edge_tiles_block_t);
    vertex_edge_tiles_block_t block;
    block.shutdown = true;

    for (int i = 0; i < config_.count_tile_processors; ++i) {

      ring_buffer_put_req_init(&tiles_req, BLOCKING, size_block);
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_put(ctx_.tiles_data_rb_, &tiles_req);
      sg_rb_check(&tiles_req);

      copy_to_ring_buffer(ctx_.tiles_data_rb_, tiles_req.data, &block,
                          size_block);
      ring_buffer_elm_set_ready(ctx_.tiles_data_rb_, tiles_req.data);
#else
      ring_buffer_scif_put(&ctx_.tiles_data_rb_, &tiles_req);
      sg_rb_check(&tiles_req);

      copy_to_ring_buffer_scif(&ctx_.tiles_data_rb_, tiles_req.data, &block,
                               size_block);
      ring_buffer_scif_elm_set_ready(&ctx_.tiles_data_rb_, tiles_req.data);
#endif
    }
  }
}
}
