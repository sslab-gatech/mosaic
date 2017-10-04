#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "tile-processor.h"
#endif
#pragma once

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
  TileProcessor<APP, TVertexType, is_weighted>::TileProcessor(
      EdgeProcessor<APP, TVertexType, is_weighted>& ctx,
      const thread_index_t& thread_index)
      : ctx_(ctx), thread_index_(thread_index), config_(ctx_.config_) {
    // Init the followers barrier, adding the TileProcessor and all its
    // followers.
    int count_barrier = 1 + config_.count_followers;
    pthread_barrier_init(&tile_processor_barrier_, NULL, count_barrier);

    // Initiate the follower threads.
    followers_ =
        new TileProcessorFollower<APP, TVertexType,
                                  is_weighted>*[config_.count_followers];

    for (int i = 0; i < config_.count_followers; ++i) {
      thread_index_t ti;
      ti.count = config_.count_followers;
      ti.id = i;

      followers_[i] = new TileProcessorFollower<APP, TVertexType, is_weighted>(
          this, ti, &tile_processor_barrier_);
    }

    // In case of using the Fake or the ConstantValue input, preallocate the
    // vertex_edge_block to be used.
    if ((config_.tile_processor_input_mode ==
         TileProcessorInputMode::TPIM_ConstantValue) ||
        (config_.tile_processor_input_mode ==
         TileProcessorInputMode::TPIM_FakeVertexFetcher)) {
      vertex_edge_tiles_block_sizes_t sizes =
          getMaxTileBlockSizes<APP, TVertexType>();
      size_t max_size_fake_vertex_edge_block = getSizeTileBlock(sizes);

      fake_vertex_edge_block_ =
          (vertex_edge_tiles_block_t*)malloc(max_size_fake_vertex_edge_block);

      tile_stats_t tile_stats_max;
      tile_stats_max.count_vertex_src = MAX_VERTICES_PER_TILE;
      tile_stats_max.count_vertex_tgt = MAX_VERTICES_PER_TILE;

      // Allocate a header, then fill in the fake data.
      uint32_t num_tile_partition = 1;
      uint32_t tile_partition_id = 0;
      fillTileBlockHeader(fake_vertex_edge_block_, 0, tile_stats_max, sizes,
                          num_tile_partition, tile_partition_id);

      char* active_vertices_src =
          APP::need_active_source_block
              ? get_array(char*, fake_vertex_edge_block_,
                          fake_vertex_edge_block_->offset_active_vertices_src)
              : NULL;
      char* active_vertices_tgt =
          APP::need_active_target_block
              ? get_array(char*, fake_vertex_edge_block_,
                          fake_vertex_edge_block_->offset_active_vertices_tgt)
              : NULL;
      vertex_degree_t* src_degrees =
          APP::need_degrees_source_block
              ? get_array(vertex_degree_t*, fake_vertex_edge_block_,
                          fake_vertex_edge_block_->offset_src_degrees)
              : NULL;
      vertex_degree_t* tgt_degrees =
          APP::need_degrees_target_block
              ? get_array(vertex_degree_t*, fake_vertex_edge_block_,
                          fake_vertex_edge_block_->offset_tgt_degrees)
              : NULL;
      TVertexType* src_vertices =
          get_array(TVertexType*, fake_vertex_edge_block_,
                    fake_vertex_edge_block_->offset_source_vertex_block);

      // Set all vertices active.
      if (active_vertices_src != NULL) {
        memset(active_vertices_src, (unsigned char)255,
               fake_vertex_edge_block_->count_active_vertex_src_block);
      }
      if (active_vertices_tgt != NULL) {
        memset(active_vertices_tgt, (unsigned char)255,
               fake_vertex_edge_block_->count_active_vertex_src_block);
      }
      // Set all degrees to 1 incoming and 1 outgoing.
      if (src_degrees != NULL) {
        for (uint32_t i = 0; i < MAX_VERTICES_PER_TILE; ++i) {
          src_degrees[i].in_degree = 1;
          src_degrees[i].out_degree = 1;
        }
      }
      if (tgt_degrees != NULL) {
        for (uint32_t i = 0; i < MAX_VERTICES_PER_TILE; ++i) {
          tgt_degrees[i].in_degree = 1;
          tgt_degrees[i].out_degree = 1;
        }
      }
      // Set all source vertices to 0.5.
      for (uint32_t i = 0; i < MAX_VERTICES_PER_TILE; ++i) {
        src_vertices[i] = 0.5;
      }
    }
  }

  template <class APP, typename TVertexType, bool is_weighted>
  TileProcessor<APP, TVertexType, is_weighted>::~TileProcessor() {
    for (int i = 0; i < config_.count_followers; ++i) {
      followers_[i]->join();
      delete followers_[i];
    }
    delete[] followers_;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  bool TileProcessor<APP, TVertexType, is_weighted>::get_tile_block() {
#if PROC_TIME_PROF
    gettimeofday(&init_start, NULL);
#endif
    // In case of the FakeVertexFetcher, we need to receive a block from the
    // VertexFetcher but don't act on it, just extract the block_id.
    uint32_t fake_block_id;
    uint32_t num_tile_partition;
    uint32_t tile_partition_id;
    if (config_.tile_processor_input_mode ==
        TileProcessorInputMode::TPIM_FakeVertexFetcher) {
      // Fetch a tile block information.
      ring_buffer_req_t request_tiles;
      ring_buffer_get_req_init(&request_tiles, BLOCKING);
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_get(ctx_.tiles_rb_, &request_tiles);
#else
      ring_buffer_scif_get(&ctx_.tiles_rb_, &request_tiles);
#endif
      sg_rb_check(&request_tiles);

      vertex_edge_tiles_block_t* temp_tiles_block =
          (vertex_edge_tiles_block_t*)request_tiles.data;

      fake_block_id = temp_tiles_block->block_id;
      num_tile_partition = temp_tiles_block->num_tile_partition;
      tile_partition_id = temp_tiles_block->tile_partition_id;

      bool shutdown = temp_tiles_block->shutdown;
      // Immediately free the block again.
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_elm_set_done(ctx_.tiles_rb_, temp_tiles_block);
#else
      ring_buffer_scif_elm_set_done(&ctx_.tiles_rb_, temp_tiles_block);
#endif

      // Shutdown if necessary.
      if (shutdown) {
        return true;
      }
    } else if (config_.tile_processor_input_mode ==
               TileProcessorInputMode::TPIM_ConstantValue) {
      // Use a simple incrementing block_id when using the constant values
      // input.
      fake_block_id = ctx_.fake_block_id_counter_.inc() % config_.count_tiles;
      // TODO: Calc number of tile partitions here.
      num_tile_partition = 1;
      tile_partition_id = 0;
    }

    if ((config_.tile_processor_input_mode ==
         TileProcessorInputMode::TPIM_ConstantValue) ||
        (config_.tile_processor_input_mode ==
         TileProcessorInputMode::TPIM_FakeVertexFetcher)) {
      // Allocate a fake id for the block, set header fields to appropriate
      // values. Let offset pointers stay the same, they point to the correct
      // locations in the preallocated block already.
      tile_stats_t tile_stats = ctx_.tile_stats_[fake_block_id];
      vertex_edge_tiles_block_sizes_t sizes =
          getTileBlockSizes<APP, TVertexType>(tile_stats);
      fake_vertex_edge_block_->block_id = fake_block_id;
      fake_vertex_edge_block_->count_active_vertex_src_block =
          sizes.count_active_vertex_src_block;
      fake_vertex_edge_block_->count_active_vertex_tgt_block =
          sizes.count_active_vertex_tgt_block;
      fake_vertex_edge_block_->count_src_vertex_block =
          tile_stats.count_vertex_src;
      fake_vertex_edge_block_->count_tgt_vertex_block =
          tile_stats.count_vertex_tgt;
      fake_vertex_edge_block_->num_tile_partition = num_tile_partition;
      fake_vertex_edge_block_->tile_partition_id = tile_partition_id;

      sg_dbg("Using fake block with id %d, num_tiles: %d, tp_id: %d\n",
             fake_block_id, num_tile_partition, tile_partition_id);

      // Use the fake block as the real one now.
      vertex_edge_block_ = fake_vertex_edge_block_;
    } else if (config_.tile_processor_input_mode ==
               TileProcessorInputMode::TPIM_VertexFetcher) {
      // fetch a tile block information
      ring_buffer_req_t request_tiles;
      ring_buffer_get_req_init(&request_tiles, BLOCKING);
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_get(ctx_.tiles_rb_, &request_tiles);
#else
      ring_buffer_scif_get(&ctx_.tiles_rb_, &request_tiles);
#endif
      sg_rb_check(&request_tiles);

      vertex_edge_block_ = (vertex_edge_tiles_block_t*)request_tiles.data;
    }

    // Return from this function if shutdown requested.
    if (vertex_edge_block_->shutdown) {
      return true;
    }

    if (vertex_edge_block_->magic_identifier != MAGIC_IDENTIFIER) {
      sg_log("Magic identifier doesn't match: %ld vs. %ld (expected)",
             vertex_edge_block_->magic_identifier, MAGIC_IDENTIFIER);
      util::die(1);
    }

    block_id_ = vertex_edge_block_->block_id;
    tile_partition_id_ = vertex_edge_block_->tile_partition_id;

#if PROC_TIME_PROF
    gettimeofday(&init_end, NULL);
    timersub(&init_end, &init_start, &init_result);
    init_lat += (init_result.tv_sec * 1000, init_result.tv_usec / 1000.0);
#endif
    // No shutdown requested.
    return false;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileProcessor<APP, TVertexType, is_weighted>::get_vertex_edge_block() {
    tile_info_ = &ctx_.tiles_offset_table_.data_info[block_id_];

    // to avoid deadlock(starvation) sequence
    // wait until tile reader has filled ringbuffer
    while (!tile_info_->meta.data_ready) {
      pthread_yield();
      smp_rmb();
    }

    // if this processor is a leader, init the pointer offset table
    if (tile_partition_id_ == 0) {
      if (!config_.in_memory_mode) {
        // wait until tile_block is completely consumed
        sg_assert(tile_info_->meta.tile_block == NULL,
                  "tile_info_->meta.tile_block was not NULL!\n");
      }
      tile_info_->meta.fetch_refcnt = vertex_edge_block_->num_tile_partition;
      tile_info_->meta.process_refcnt = vertex_edge_block_->num_tile_partition;
      tile_info_->meta.tile_block = vertex_edge_block_;
      smp_wmb();
    }
    // otherwise fetch the original
    else {
      // throw away this and get real one
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_elm_set_done(ctx_.tiles_rb_, vertex_edge_block_);
#else
      ring_buffer_scif_elm_set_done(&ctx_.tiles_rb_, vertex_edge_block_);
#endif

      // fetch the tile block read by the leader, waiting for the leader to
      // fill
      // in the block
      vertex_edge_block_ =
          (vertex_edge_tiles_block_t*)tile_info_->meta.tile_block;
      while (vertex_edge_block_ == NULL) {
        pthread_yield();
        smp_rmb();
        vertex_edge_block_ =
            (vertex_edge_tiles_block_t*)tile_info_->meta.tile_block;
      }
    }

    // Now, we got a tile block
    sg_dbg("TP:Got tiles-block %lu!\n", block_id_);
    tile_stats_ = ctx_.tile_stats_[block_id_];

    // fix offset pointers
    size_active_vertex_src_block_ =
        APP::need_active_source_block
            ? sizeof(char) * vertex_edge_block_->count_active_vertex_src_block
            : 0;
    size_active_vertex_tgt_block_ =
        APP::need_active_target_block
            ? sizeof(char) * vertex_edge_block_->count_active_vertex_tgt_block
            : 0;

    active_vertices_tgt_ =
        APP::need_active_target_block
            ? get_array(char*, vertex_edge_block_,
                        vertex_edge_block_->offset_active_vertices_tgt)
            : NULL;

    active_vertices_src_ =
        APP::need_active_source_input
            ? get_array(char*, vertex_edge_block_,
                        vertex_edge_block_->offset_active_vertices_src)
            : NULL;

    src_degrees_ = get_array(vertex_degree_t*, vertex_edge_block_,
                             vertex_edge_block_->offset_src_degrees);

    tgt_degrees_ = get_array(vertex_degree_t*, vertex_edge_block_,
                             vertex_edge_block_->offset_tgt_degrees);

    src_vertices_ = get_array(TVertexType*, vertex_edge_block_,
                              vertex_edge_block_->offset_source_vertex_block);

    extension_fields_ = APP::need_vertex_block_extension_fields
                            ? get_array(void*, vertex_edge_block_,
                                        vertex_edge_block_->offset_extensions)
                            : NULL;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileProcessor<APP, TVertexType, is_weighted>::get_tile_data() {
#if PROC_TIME_PROF
    gettimeofday(&put_end, NULL);
    timersub(&put_end, &put_start, &put_result);
    put_lat += (put_result.tv_sec * 1000, put_result.tv_usec / 1000.0);

    gettimeofday(&get_tile_start, NULL);
#endif

    sg_dbg("Wait for edges for block %lu\n", block_id_);
    bundle_raw_ = tile_info_->meta.bundle_raw;
    bundle_refcnt_ = tile_info_->meta.bundle_refcnt;
    edge_block_ = const_cast<edge_block_t*>(tile_info_->data);

    sg_dbg("Got edges for block %lu\n", block_id_);

    // only throw away loaded tile if not using the in-memory-mode
    if (!config_.in_memory_mode) {
      bool reset_cache = false;
      // if a vertex_edge_block is completely fetched, reset the tile_info table
      if (vertex_edge_block_->num_tile_partition > 1) {
        // decrement fetch refcnt if there are multiple processors for this tile
        uint32_t refcnt = smp_faa(&tile_info_->meta.fetch_refcnt, -1);
        if (refcnt == 1) {
          reset_cache = true;
        }
      } else {
        // if it is the only one processor for this tile, reset is implied
        reset_cache = true;
      }

      // reset cache immediately, we only need the pointer once
      if (reset_cache) {
        tile_info_->meta.data_ready = false;
        tile_info_->meta.data_active = false;
        tile_info_->meta.tile_block = NULL;
        sg_dbg("TP:tile %lu will be processed by %d processors\n", block_id_,
               vertex_edge_block_->num_tile_partition);
      }
      smp_wmb();
    }

#if PROC_TIME_PROF
    gettimeofday(&get_tile_end, NULL);
    timersub(&get_tile_end, &get_tile_start, &get_tile_result);
    get_tile_lat +=
        (get_tile_result.tv_sec * 1000, get_tile_result.tv_usec / 1000.0);
#endif
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void
  TileProcessor<APP, TVertexType, is_weighted>::calc_start_end_current_tile(
      uint32_t* start, uint32_t* end) {
    uint32_t nedges_per_partition =
        tile_stats_.count_edges / vertex_edge_block_->num_tile_partition;
    *start = tile_partition_id_ * nedges_per_partition;
    *end = *start + nedges_per_partition;

    // The last partition will take all the rest.
    if (vertex_edge_block_->num_tile_partition == (tile_partition_id_ + 1)) {
      *end = tile_stats_.count_edges;
    }
  }

  template <class APP, typename TVertexType, bool is_weighted>
  uint32_t
  TileProcessor<APP, TVertexType, is_weighted>::count_edges_current_tile() {
    uint32_t start, end;
    calc_start_end_current_tile(&start, &end);
    return end - start;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  uint32_t TileProcessor<APP, TVertexType, is_weighted>::process_edges() {
    uint32_t start, end;
    calc_start_end_current_tile(&start, &end);

    // Skip processing tiles if tile processor should not be active.
    if (config_.tile_processor_mode == TileProcessorMode::TPM_Active) {
      process_edges_range(start, end);
    }
    return (end - start) / (1 + config_.count_followers);
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileProcessor<APP, TVertexType, is_weighted>::process_edges_range(
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
  uint32_t TileProcessor<APP, TVertexType, is_weighted>::get_rle_offset(
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
  void TileProcessor<APP, TVertexType, is_weighted>::process_edges_range_rle(
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

    uint32_t start_index = start;
    uint32_t end_index;
    uint32_t offset = thread_count * EDGES_STRIPE_SIZE;

    // The common rle offset, i.e. the number of edges being skipped by this
    // TileProcessor.
    uint32_t skip_rle_count = offset - EDGES_STRIPE_SIZE;

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
  void TileProcessor<APP, TVertexType, is_weighted>::process_edges_range_list(
      uint32_t start, uint32_t end) {
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

    uint32_t start_index = start;
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
  void TileProcessor<APP, TVertexType, is_weighted>::prepare_response() {
#if PROC_TIME_PROF
    gettimeofday(&put_start, NULL);
#endif

    size_t size_response_vertices =
        sizeof(TVertexType) * tile_stats_.count_vertex_tgt;
    size_t size_response =
        sizeof(processed_vertex_block_t) + size_response_vertices +
        size_active_vertex_src_block_ + size_active_vertex_tgt_block_;

    ring_buffer_req_t request_processed;
    ring_buffer_put_req_init(&request_processed, BLOCKING, size_response);
#if defined(MOSAIC_HOST_ONLY)
    ring_buffer_put(ctx_.processed_rb_, &request_processed);
#else
    ring_buffer_scif_put(&ctx_.processed_rb_, &request_processed);
#endif
    sg_rb_check(&request_processed);
    ring_buffer_assert_fingerprint(request_processed.data);

    response_block_ = (processed_vertex_block_t*)request_processed.data;
    response_block_->shutdown = false;
    response_block_->magic_identifier = MAGIC_IDENTIFIER;
    response_block_->block_id = block_id_;
    response_block_->count_active_vertex_src_block =
        APP::need_active_source_block
            ? vertex_edge_block_->count_active_vertex_src_block
            : 0;
    response_block_->count_active_vertex_tgt_block =
        APP::need_active_target_block
            ? vertex_edge_block_->count_active_vertex_tgt_block
            : 0;
    response_block_->count_tgt_vertex_block = tile_stats_.count_vertex_tgt;

    response_block_->offset_active_vertices_src =
        sizeof(processed_vertex_block_t);
    response_block_->offset_active_vertices_tgt =
        response_block_->offset_active_vertices_src +
        size_active_vertex_src_block_;
    response_block_->offset_vertices =
        response_block_->offset_active_vertices_tgt +
        size_active_vertex_tgt_block_;

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
#if PROC_TIME_PROF
    gettimeofday(&put_end, NULL);
    timersub(&put_end, &put_start, &put_result);
    put_lat += (put_result.tv_sec * 1000, put_result.tv_usec / 1000.0);
#endif
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileProcessor<APP, TVertexType, is_weighted>::wrap_up(uint32_t nedges) {
    // keep host busy
    bool no_ref = false;
    if (vertex_edge_block_->num_tile_partition == 1 ||
        smp_faa(&tile_info_->meta.process_refcnt, -1) == 1) {
      // Only set the block done if using the input from the vertex fetcher.
      if (config_.tile_processor_input_mode ==
          TileProcessorInputMode::TPIM_VertexFetcher) {
#if defined(MOSAIC_HOST_ONLY)
        ring_buffer_elm_set_done(ctx_.tiles_rb_, vertex_edge_block_);
#else
        ring_buffer_scif_elm_set_done(&ctx_.tiles_rb_, vertex_edge_block_);
#endif
      }
      no_ref = true;
    }
#if defined(MOSAIC_HOST_ONLY)
    ring_buffer_elm_set_ready(ctx_.processed_rb_, response_block_);
#else
    ring_buffer_scif_elm_set_ready(&ctx_.processed_rb_, response_block_);
#endif

    // done with processing this block, send back to reducer, set tile done
    // and mark as NULL in offset-table
    if (!config_.in_memory_mode && no_ref) {
      // mark as done only if
      //  - not using the in-memory-mode
      //  - and all tiles in a batch have been used
      if (smp_faa(bundle_refcnt_, -1) == 1) {
        ring_buffer_elm_set_done(ctx_.local_tiles_rb_, (void*)bundle_raw_);
      }
    }
    sg_print("Done resetting, next tile!\n");

#if PROC_TIME_PROF
    gettimeofday(&process_end, NULL);
    timersub(&process_end, &process_start, &process_result);
    process_lat +=
        (process_result.tv_sec * 1000, process_result.tv_usec / 1000.0);

    sg_log("Processor [%lu] init : %.3f, put : %.3f, get tile : %.3f, process "
           ": %.3f\n",
           thread_index_.id, init_lat, put_lat, get_tile_lat, process_lat);
    tile_count = 0;
    init_lat = 0.0;
    put_lat = 0.0;
    get_tile_lat = 0.0;
    process_lat = 0.0;
#endif
    smp_faa(&ctx_.perfmon_.count_edges_processed_, nedges);
    if (no_ref) {
      smp_faa(&ctx_.perfmon_.count_tiles_processed_, 1);
    }
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileProcessor<APP, TVertexType, is_weighted>::run() {
    bool init_shutdown = false;
    bool sample_execution_time = false;
    size_t start_time_ns;
    size_t end_time_ns;

    // Start followers.
    for (int i = 0; i < config_.count_followers; ++i) {
      followers_[i]->start();
      followers_[i]->setName("Follow_" + std::to_string(thread_index_.id) +
                             "_" + std::to_string(i));
      // On Mic, pin the follower to the same core as the current thread.
      if (config_.run_on_mic) {
        // Just pin to the same socket and cpu as the main thread.
        util::cpu_id_t cpu_id(cpu_id_.socket, cpu_id_.pcpu,
                              util::cpu_id_t::ANYWHERE);
        followers_[i]->setAffinity(cpu_id);
      }
    }

    while (true) {
      init_shutdown = get_tile_block();
      // Break out of loop and allow join() to succeed on shutdown.
      if (unlikely(init_shutdown)) {
        // Notify the followers:
        for (int i = 0; i < config_.count_followers; ++i) {
          followers_[i]->shutdown_ = true;
        }
        pthread_barrier_wait(&tile_processor_barrier_);

        break;
      }

      uint32_t edges_for_partition = count_edges_current_tile();

      PerfEventScoped perf_event_tile(
          PerfEventManager::getInstance(config_)->getRingBuffer(), "tile",
          ComponentType::CT_TileProcessor, config_.mic_index, thread_index_.id,
          edges_for_partition, vertex_edge_block_->block_id,
          config_.enable_perf_event_collection);
      // Copy the variable before it gets overwritten when getting the leader
      // vertex_edge_block.
      sample_execution_time = vertex_edge_block_->sample_execution_time;

      // If sampling the current tile, take the time from beginning to end:
      if (sample_execution_time) {
        start_time_ns = util::get_time_nsec();
      }

      {
        PerfEventScoped perf_event(
            PerfEventManager::getInstance(config_)->getRingBuffer(),
            "get_vertex_edge_block", ComponentType::CT_TileProcessor,
            config_.mic_index, thread_index_.id, vertex_edge_block_->block_id,
            config_.enable_perf_event_collection);
        get_vertex_edge_block();
      }
      {
        PerfEventScoped perf_event(
            PerfEventManager::getInstance(config_)->getRingBuffer(),
            "prepare_response", ComponentType::CT_TileProcessor,
            config_.mic_index, thread_index_.id, vertex_edge_block_->block_id,
            config_.enable_perf_event_collection);
        prepare_response();
      }
      {
        PerfEventScoped perf_event(
            PerfEventManager::getInstance(config_)->getRingBuffer(),
            "get_tile_data", ComponentType::CT_TileProcessor, config_.mic_index,
            thread_index_.id, vertex_edge_block_->block_id,
            config_.enable_perf_event_collection);
        get_tile_data();
      }
      // Notify the followers that we got a tile block.
      pthread_barrier_wait(&tile_processor_barrier_);
      uint32_t nedges;
      {
        PerfEventScoped perf_event(
            PerfEventManager::getInstance(config_)->getRingBuffer(),
            "process_edges", ComponentType::CT_TileProcessor, config_.mic_index,
            thread_index_.id, vertex_edge_block_->block_id,
            config_.enable_perf_event_collection);
        nedges = process_edges();
      }

      // Wait for all the followers to be done.
      pthread_barrier_wait(&tile_processor_barrier_);
      nedges += gather_follower_output();

      // Sample the end time, if instructed to do so, and copy the result to the
      // output.
      if (sample_execution_time) {
        end_time_ns = util::get_time_nsec();
        response_block_->sample_execution_time = true;
        response_block_->count_edges = edges_for_partition;
        response_block_->processing_time_nano = end_time_ns - start_time_ns;
      } else {
        response_block_->sample_execution_time = false;
      }
      {
        PerfEventScoped perf_event(
            PerfEventManager::getInstance(config_)->getRingBuffer(), "wrap_up",
            ComponentType::CT_TileProcessor, config_.mic_index,
            thread_index_.id, vertex_edge_block_->block_id,
            config_.enable_perf_event_collection);
        wrap_up(nedges);
      }
    }

    // Wait for all tile processors to be shut down, then shut down the vertex
    // reducers with a single thread.
    sg_log("Shutdown initiated for TileProcessor %lu\n", thread_index_.id);
    int barrier_rc = pthread_barrier_wait(&ctx_.barrier_tile_processors_);

    if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
      // Single thread shuts down all the VertexReducers, done.
      shutdown();
    }
    sg_log("Shutdown finished for TileProcessor %lu\n", thread_index_.id);
  }

  template <class APP, typename TVertexType, bool is_weighted>
  uint32_t
  TileProcessor<APP, TVertexType, is_weighted>::gather_follower_output() {
    uint32_t nedges;
    // Collect the edge count from all followers and apply their results to
    // the buffer in the ringbuffer.
    for (int i = 0; i < config_.count_followers; ++i) {
      nedges += followers_[i]->nedges_;
      for (uint32_t vertex_id = 0; vertex_id < tile_stats_.count_vertex_tgt;
           ++vertex_id) {
        TVertexType& u = followers_[i]->tgt_vertices_[vertex_id];
        TVertexType& v = tgt_vertices_[vertex_id];
        APP::gather(u, v, vertex_id, extension_fields_);

        // OR the active arrays.
        if (APP::need_active_source_block) {
          bool value_src_active_next =
              eval_bool_array(active_vertices_src_next_, i) ||
              eval_bool_array(followers_[i]->active_vertices_src_next_, i);
          set_bool_array(active_vertices_src_next_, i, value_src_active_next);
        }

        if (APP::need_active_target_block) {
          bool value_tgt_active_next =
              eval_bool_array(active_vertices_tgt_next_, i) ||
              eval_bool_array(followers_[i]->active_vertices_tgt_next_, i);
          set_bool_array(active_vertices_tgt_next_, i, value_tgt_active_next);
        }
      }
    }
    return nedges;
  }

  template <class APP, typename TVertexType, bool is_weighted>
  void TileProcessor<APP, TVertexType, is_weighted>::shutdown() {
    ring_buffer_req_t request_processed;
    processed_vertex_block_t block;
    block.shutdown = true;
    size_t size_block = sizeof(processed_vertex_block_t);

    for (int i = 0; i < config_.count_vertex_reducers; ++i) {
      ring_buffer_put_req_init(&request_processed, BLOCKING, size_block);
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_put(ctx_.processed_rb_, &request_processed);
#else
      ring_buffer_scif_put(&ctx_.processed_rb_, &request_processed);
#endif
      sg_rb_check(&request_processed);

      memcpy(request_processed.data, &block, size_block);
#if defined(MOSAIC_HOST_ONLY)
      ring_buffer_elm_set_ready(ctx_.processed_rb_,
                                request_processed.data);
#else
      ring_buffer_scif_elm_set_ready(&ctx_.processed_rb_,
                                     request_processed.data);
#endif
    }
  }
}
}
