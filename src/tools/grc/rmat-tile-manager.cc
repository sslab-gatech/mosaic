#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "rmat-tile-manager.h"
#endif

#include <algorithm>
#include <fstream>

#include <assert.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <cmath>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <malloc.h>

#include <util/hilbert.h>
#include <core/datatypes.h>
#include <util/util.h>
#include <util/arch.h>
#include <core/util.h>

namespace scalable_graphs {
namespace graph_load {

  template <typename TVertexIdType>
  struct ThreadInfo {
    pthread_t thr;
    RMATTileManager<TVertexIdType>* tm;
    int64_t start;
    int64_t end;
    int64_t count_tiles;
    int64_t count_edges;
  };

  template <typename TVertexIdType>
  local_vertex_id_t generateLocalVertexID(
      const TVertexIdType& vertex_id, local_vertex_id_t& local_vertex_id_base,
      std::unordered_set<TVertexIdType>& vertex_set,
      std::unordered_map<TVertexIdType, local_vertex_id_t>& global_to_local,
      std::vector<TVertexIdType>& local_to_global) {
    // Check if src/tgt already available:
    if (vertex_set.find(vertex_id) != vertex_set.end()) {
      auto it = global_to_local.find(vertex_id);

      if (it != global_to_local.end()) {
        return it->second;
      }

      sg_err("id not found %u\n", (unsigned int)vertex_id);
      util::die(1);
    }

    vertex_set.insert(vertex_id);
    local_vertex_id_t id_local = local_vertex_id_base++;
    global_to_local[vertex_id] = id_local;
    local_to_global.push_back(vertex_id);

    sg_assert(vertex_set.size() <= MAX_VERTICES_PER_TILE, "");

    return id_local;
  }

  template <typename TVertexIdType>
  RMATTileManager<TVertexIdType>::RMATTileManager(
      const rmat_tile_manager_arguments_t& arguments)
      : partition_managers_(arguments.partition_managers),
        config_(arguments.config),
        edge_receiver_barrier_(arguments.edge_receiver_barrier) {
    // vertex_to_tiles_per_vertices_ =
    //     (uint32_t*)calloc(1, sizeof(uint32_t) * config_.count_vertices);
    pthread_spin_init(&gv_lock, PTHREAD_PROCESS_PRIVATE);
  }

  template <typename TVertexIdType>
  void RMATTileManager<TVertexIdType>::processPartition(
      RMATTileManager::Context& ctx, int src, int tgt) {
    // load partition
    partition_edge_t* partition = getEdges({(uint64_t)src, (uint64_t)tgt});

    // process partition
    for (int i = 0; i < partition->count_edges; ++i) {
      edge_t edge;
      edge.src = partition->edges[i].src;
      edge.tgt = partition->edges[i].tgt;

      sg_assert(edge.src < config_.count_vertices, "src < count-vertices");
      sg_assert(edge.tgt < config_.count_vertices, "tgt < count-vertices");

      if (needToStartNewEdgeBlock(ctx, edge)) {
        writeTile(ctx);
        uint64_t hilbert_id =
            ::traversal::hilbert::xy2d(config_.count_rows_partitions, src, tgt);
        ctx.block_id = hilbert_id;
      }

      sg_assert(ctx.src_set_.size() <= MAX_VERTICES_PER_TILE, "");
      sg_assert(ctx.tgt_set_.size() <= MAX_VERTICES_PER_TILE, "");

      size_t src_size_before = ctx.src_set_.size();
      size_t tgt_size_before = ctx.tgt_set_.size();

      local_vertex_id_t src_local = generateLocalVertexID<TVertexIdType>(
          edge.src, ctx.local_vertex_id_src_base_, ctx.src_set_,
          ctx.src_global_to_local_, ctx.src_local_to_global_);
      local_vertex_id_t tgt_local = generateLocalVertexID<TVertexIdType>(
          edge.tgt, ctx.local_vertex_id_tgt_base_, ctx.tgt_set_,
          ctx.tgt_global_to_local_, ctx.tgt_local_to_global_);

      sg_assert(src_size_before + 1 >= ctx.src_set_.size(), "");
      sg_assert(tgt_size_before + 1 >= ctx.tgt_set_.size(), "");

      local_edge_t local_edge;
      local_edge.src = src_local;
      local_edge.tgt = tgt_local;
      ctx.edge_set_.push_back(local_edge);
    }

    // clean up
    free(partition);
  }

  template <typename TVertexIdType>
  RMATTileManager<TVertexIdType>::Context::Context(
      int64_t start_id, uint64_t count_partitions, uint64_t count_vertices,
      const std::vector<std::string>& paths_to_partitions)
      : vertex_to_tiles_per_vertices_(NULL), local_vertex_id_src_base_(0),
        local_vertex_id_tgt_base_(0), start_id_(start_id), count_tiles_(0),
        count_edges_(0) {
    // vertex_to_tiles_per_vertices_ =
    //     (uint32_t*)calloc(1, sizeof(uint32_t) * count_vertices);
  }

  template <typename TVertexIdType>
  RMATTileManager<TVertexIdType>::Context::~Context() {
    // free(vertex_to_tiles_per_vertices_);
  }

  template <typename TVertexIdType>
  partition_edge_t*
  RMATTileManager<TVertexIdType>::getEdges(const partition_t& partition) {
    partition_t partition_partition_manager =
        core::getPartitionManagerOfPartition(partition, config_);

    size_t offset_partition_manager =
        core::getIndexOfPartitionManager(partition_partition_manager, config_);

    return partition_managers_[offset_partition_manager]->getPartition(
        partition);
  }

  template <typename TVertexIdType>
  void RMATTileManager<TVertexIdType>::processPartitionsInRange(
      int64_t start, int64_t end, int64_t* count_tiles, int64_t* count_edges) {
    Context ctx(start, config_.count_rows_partitions, config_.count_vertices,
                config_.paths_to_partition);
    ctx.block_id = start;

    // tiling a range in a Hilbert order
    for (int64_t i = start; i < end; ++i) {
      int64_t x, y;
      ::traversal::hilbert::d2xy(config_.count_rows_partitions, i, &x, &y);
      processPartition(ctx, x, y);
    }

    // one final write, if something to write:
    if (ctx.edge_set_.size() > 0) {
      writeTile(ctx);
    }

    // pthread_spin_lock(&gv_lock);
    // {
    //   for (uint64_t id = 0; id < config_.count_vertices; ++id) {
    //     vertex_to_tiles_per_vertices_[id] +=
    //         ctx.vertex_to_tiles_per_vertices_[id];
    //   }
    // }
    // pthread_spin_unlock(&gv_lock);

    // return the number of written tiles
    *count_tiles = ctx.count_tiles_;
    *count_edges = ctx.count_edges_;
  }

  template <typename TVertexIdType>
  int RMATTileManager<TVertexIdType>::calcProperNumThreads(int max_thread) {
    // int64_t p2 = count_partitions_ * count_partitions_;
    // int num_chunk = (p2 + min_partitions_ - 1) / min_partitions_;
    // int nthread = std::min(num_chunk, max_thread);
    // nthread = std::max(paths_to_tile_.size(),
    //                    nthread - (nthread % paths_to_tile_.size()));
    // return nthread;

    return max_thread;
  }

  template <typename TVertexIdType>
  void* RMATTileManager<TVertexIdType>::threadMain(void* arg) {
    auto ti = static_cast<ThreadInfo<TVertexIdType>*>(arg);
    ti->tm->processPartitionsInRange(ti->start, ti->end, &ti->count_tiles,
                                     &ti->count_edges);
    return NULL;
  }

  template <typename TVertexIdType>
  void RMATTileManager<TVertexIdType>::generateAndWriteTiles() {
    int64_t global_count_tiles = 0;
    int64_t global_count_edges = 0;

    uint64_t hilbert_start_of_current_block = 0;
    uint64_t hilbert_end_of_current_block = 0;

    bool more_edges_to_be_collected = true;
    int round = 0;

    size_t global_count_partitions = pow(config_.count_rows_partitions, 2);
    size_t partitions_per_partition_manager =
        global_count_partitions / config_.count_partition_managers;

    while (more_edges_to_be_collected) {
      sg_log("Start round %d\n", round);

      // swap hilbert-start and end:
      hilbert_start_of_current_block = hilbert_end_of_current_block;

      // incrementally active partition-managers until exceeding the maximum
      // number of edges per round
      size_t count_edges_current_round = 0;
      int count_partition_managers_current_round = 0;
      while (
          (count_edges_current_round <= RMAT_TILER_MAX_EDGES_PER_ROUND) &&
          (hilbert_end_of_current_block < config_.count_partition_managers)) {
        // convert hilbert-start-point into partition-manager-partition, then
        // convert to partition-manager-index:
        int64_t x, y = 0;

        traversal::hilbert::d2xy(config_.count_rows_partition_managers,
                                 hilbert_end_of_current_block, &x, &y);
        partition_t partition_manager_partition = {(uint64_t)x, (uint64_t)y};
        int partition_manager_index = core::getIndexOfPartitionManager(
            partition_manager_partition, config_);

        partition_managers_[partition_manager_index]->initTiling();
        partition_managers_[partition_manager_index]->start();
        sg_log("Activate partition-manager %d\n", partition_manager_index);

        count_edges_current_round +=
            partition_managers_[partition_manager_index]->count_edges_;

        ++hilbert_end_of_current_block;
        ++count_partition_managers_current_round;
      }

      // set end-flag if all partitions have been scanned:
      more_edges_to_be_collected =
          (hilbert_end_of_current_block < config_.count_partition_managers);

      sg_log(
          "Partition-managers set up for round %d, start receiving %lu edges\n",
          round, count_edges_current_round);

      // signal all edge-receivers to start receiving:
      for (int i = 0; i < config_.count_edge_generators; ++i) {
        pthread_barrier_wait(edge_receiver_barrier_[i]);
      }

      for (int i = 0; i < config_.count_partition_managers; ++i) {
        if (partition_managers_[i]->active_this_round_) {
          partition_managers_[i]->join();
        }
      }

      sg_log("All edges received for round %d, start tiling from %lu to %lu "
             "with %lu edges\n",
             round, hilbert_start_of_current_block,
             hilbert_end_of_current_block, count_edges_current_round);

      int64_t partitions_current_round = partitions_per_partition_manager *
                                         count_partition_managers_current_round;

      int nthread = calcProperNumThreads(config_.nthreads);

      int64_t partitions_per_thread = partitions_current_round / nthread;
      uint64_t hilbert_start_index =
          partitions_per_partition_manager * hilbert_start_of_current_block;

      std::vector<ThreadInfo<TVertexIdType>*> threads;
      int rc = 0;

      sg_log("Generating tiles with %d threads concurrently.\n", nthread);

      // lanunch tiling threads
      for (int i = 0; i < (nthread - 1); ++i) {
        auto ti = new ThreadInfo<TVertexIdType>;
        ti->tm = this;
        ti->start = hilbert_start_index + partitions_per_thread * i;
        ti->end = ti->start + partitions_per_thread;
        ti->count_tiles = 0;
        ti->count_edges = 0;

        int rc = pthread_create(&ti->thr, NULL,
                                RMATTileManager<TVertexIdType>::threadMain, ti);
        threads.push_back(ti);
      }

      // do my job
      int64_t count_tiles;
      int64_t count_edges;

      uint64_t start_index =
          hilbert_start_index + partitions_per_thread * (nthread - 1);
      uint64_t end_index = hilbert_start_index + partitions_current_round;

      processPartitionsInRange(start_index, end_index, &count_tiles,
                               &count_edges);

      // wait for parsing threads
      for (const auto& it : threads) {
        int rc2 = pthread_join(it->thr, NULL);
        if (rc2)
          rc = rc2;
        count_tiles += it->count_tiles;
        count_edges += it->count_edges;
        delete it;
      }
    check_err:
      if (rc)
        util::die(1);

      // done tiling, shutdown all the partition-managers involved:
      for (int i = 0; i < config_.count_partition_managers; ++i) {
        if (partition_managers_[i]->active_this_round_) {
          partition_managers_[i]->cleanUpTiling();
        }
      }
      // update the global edge/tile-count:
      global_count_edges += count_edges;
      global_count_tiles += count_tiles;

      sg_log("Generated %lu tiles with %lu edges\n", global_count_tiles,
             global_count_edges);

      ++round;
    }

    // write the vertex -> tile-count file:
    // std::string vertex_to_tile_count_file_name =
    //     core::getVertexToTileCountFileName(config_);
    // size_t size_vertex_to_tile_count_file =
    //     sizeof(uint32_t) * config_.count_vertices;

    // util::writeDataToFile(
    //     vertex_to_tile_count_file_name,
    //     reinterpret_cast<const void*>(vertex_to_tiles_per_vertices_),
    //     size_vertex_to_tile_count_file);

    // size_t count_vertex_to_tiles = 0;
    // for (int i = 0; i < config_.count_vertices; ++i) {
    //   count_vertex_to_tiles += vertex_to_tiles_per_vertices_[i];
    // }
    // size_t index_size = count_vertex_to_tiles * sizeof(uint32_t) +
    //                     sizeof(size_t) * config_.count_vertices;

    // sg_log("Count vertex-to-tiles: %lu\n", count_vertex_to_tiles);
    // sg_log("Size Index: %lu\n", index_size);
    sg_log("Count Edges: %lu\n", global_count_edges);

    // write the global stats file:
    std::string global_stats_file_name = core::getGlobalStatFileName(config_);

    scenario_stats_t stat;
    stat.count_tiles = global_count_tiles;
    stat.count_vertices = config_.count_vertices;
    stat.is_weighted_graph = false;

    if (stat.count_vertices < UINT32_MAX) {
      stat.is_index_32_bits = true;
      stat.index_33_bit_extension = false;
    } else {
      stat.is_index_32_bits = false;
      stat.index_33_bit_extension = true;
    }

    util::writeDataToFile(global_stats_file_name,
                          reinterpret_cast<const void*>(&stat), sizeof(stat));
    sg_log("Statistics: Tiles: %lu Vertices: %lu\n", stat.count_tiles,
           stat.count_vertices);
  }

  template <typename TVertexIdType>
  bool RMATTileManager<TVertexIdType>::needToStartNewEdgeBlock(
      RMATTileManager<TVertexIdType>::Context& ctx, const edge_t& edge) {
    size_t src_size = ctx.src_set_.size();
    size_t tgt_size = ctx.tgt_set_.size();

    // intuition: Addition of this edge results in an increase of total
    // vertices for the current block.
    if (ctx.src_set_.find(edge.src) == ctx.src_set_.end()) {
      ++src_size;
    }

    if (ctx.tgt_set_.find(edge.tgt) == ctx.tgt_set_.end()) {
      ++tgt_size;
    }

    if (src_size > MAX_VERTICES_PER_TILE || tgt_size > MAX_VERTICES_PER_TILE) {
      return true;
    }

    sg_assert(ctx.src_set_.size() <= MAX_VERTICES_PER_TILE, "");
    sg_assert(ctx.tgt_set_.size() <= MAX_VERTICES_PER_TILE, "");
    return false;
  }

  template <typename TLocalEdgeType>
  inline void addEdgeWeightToBlock(const TLocalEdgeType& edge,
                                   float& destination) {}

  template <>
  inline void
  addEdgeWeightToBlock<local_edge_weighted_t>(const local_edge_weighted_t& edge,
                                              float& destination) {
    destination = edge.weight;
  }

  template <typename TVertexIdType>
  void RMATTileManager<TVertexIdType>::writeTile(
      RMATTileManager<TVertexIdType>::Context& ctx) {
    // Sort edges, generate global mapping, write to file
    // sort edges by tgt: Works as higher 16-bits are local tgt-id:
    std::sort(ctx.edge_set_.begin(), ctx.edge_set_.end());
    // sort((uint32_t*) ctx.edge_set_.data(), ctx.edge_set_.size(), 1);
    // Malloc here:
    size_t edge_count = ctx.edge_set_.size();
    size_t src_size = ctx.src_set_.size();
    size_t tgt_size = ctx.tgt_set_.size();

    ++ctx.count_tiles_;
    ctx.count_edges_ += edge_count;

    sg_dbg("%lu\n", edge_count);
    sg_dbg("%lu, %lu\n", src_size, tgt_size);

    sg_assert(src_size <= MAX_VERTICES_PER_TILE, "");
    sg_assert(tgt_size <= MAX_VERTICES_PER_TILE, "");

    // only use RLE if the edge-count is larger then 2-times the tgt-count,
    // otherwise the RLE will actually increase the tile-size!
    bool use_rle = config_.use_rle && (edge_count >= 2 * tgt_size);

    size_t size_edge_src_block = sizeof(local_vertex_id_t) * edge_count;
    size_t size_edge_tgt_block = size_edge_src_block;

    // if using rle, take the tgt-block as the size times the
    // vertex-count-struct
    if (use_rle) {
      size_edge_tgt_block = sizeof(vertex_count_t) * tgt_size;
    }

    // only include weight-block if necessary
    size_t size_edge_weights_block = 0;
    size_t malloc_edge_block_size = sizeof(edge_block_t) + size_edge_src_block +
                                    size_edge_tgt_block +
                                    size_edge_weights_block;

    edge_block_t* block = (edge_block_t*)malloc(malloc_edge_block_size);

    block->block_id = ctx.block_id;
    block->offset_src = sizeof(edge_block_t);
    block->offset_tgt = block->offset_src + size_edge_src_block;
    block->offset_weight = block->offset_tgt + size_edge_tgt_block;

    // prepare src/tgt-blocks:
    local_vertex_id_t* edge_src_block =
        get_array(local_vertex_id_t*, block, block->offset_src);

    // two different representations of the tgt-block, once with and without
    // the
    // rle-representation
    local_vertex_id_t* edge_tgt_block =
        get_array(local_vertex_id_t*, block, block->offset_tgt);
    vertex_count_t* edge_tgt_block_rle =
        get_array(vertex_count_t*, block, block->offset_tgt);

    local_vertex_id_t active_tgt = ctx.edge_set_[0].tgt;
    uint32_t current_count = 0;
    // signifies the current position in the rle-encoded field
    size_t rle_position = 0;

    for (size_t i = 0; i < edge_count; ++i) {
      edge_src_block[i] = ctx.edge_set_[i].src;
      if (use_rle) {
        local_vertex_id_t current_tgt = ctx.edge_set_[i].tgt;
        // check if we need to either increment the current count
        if (current_tgt == active_tgt) {
          ++current_count;
        }
        // or start a new field
        else {
          active_tgt = current_tgt;
          current_count = 1;
          ++rle_position;
        }
        edge_tgt_block_rle[rle_position].count = current_count;
        edge_tgt_block_rle[rle_position].id = current_tgt;
      } else {
        edge_tgt_block[i] = ctx.edge_set_[i].tgt;
      }
    }

    // assert that everything went right:
    sg_assert(block->block_id == ctx.block_id, "");

#ifdef SCALABLE_GRAPHS_DEBUG
    // check if calculated correctly:
    for (size_t i = 0; i < edge_count; ++i) {
      local_vertex_id_t src = edge_src_block[i];
      sg_assert(src == ctx.edge_set_[i].src, "");
      if (!use_rle) {
        local_vertex_id_t tgt = edge_tgt_block[i];
        sg_assert(tgt == ctx.edge_set_[i].tgt, "");
      }
    }
#endif

    // write index:
    bool use_extended_index_bits = config_.count_vertices >= UINT32_MAX;

    size_t src_size_bytes = src_size * sizeof(uint32_t);
    size_t tgt_size_bytes = tgt_size * sizeof(uint32_t);
    size_t src_extended_size_bytes = 0;
    size_t tgt_extended_size_bytes = 0;
    if (use_extended_index_bits) {
      src_extended_size_bytes = size_bool_array(src_size);
      tgt_extended_size_bytes = size_bool_array(tgt_size);
    }

    size_t size_edge_block_index = sizeof(edge_block_index_t) + src_size_bytes +
                                   tgt_size_bytes + src_extended_size_bytes +
                                   tgt_extended_size_bytes;

    edge_block_index_t* edge_block_index =
        (edge_block_index_t*)malloc(size_edge_block_index);

    edge_block_index->block_id = block->block_id;
    edge_block_index->count_src_vertices = src_size;
    edge_block_index->count_tgt_vertices = tgt_size;

    edge_block_index->offset_src_index = sizeof(edge_block_index_t);
    edge_block_index->offset_tgt_index =
        edge_block_index->offset_src_index + src_size_bytes;

    edge_block_index->offset_src_index_bit_extension =
        edge_block_index->offset_tgt_index + tgt_size_bytes;
    edge_block_index->offset_tgt_index_bit_extension =
        edge_block_index->offset_src_index_bit_extension +
        src_extended_size_bytes;

    uint32_t* src_index_block = get_array(uint32_t*, edge_block_index,
                                          edge_block_index->offset_src_index);
    uint32_t* tgt_index_block = get_array(uint32_t*, edge_block_index,
                                          edge_block_index->offset_tgt_index);

    char* src_index_extended_block =
        get_array(char*, edge_block_index,
                  edge_block_index->offset_src_index_bit_extension);
    char* tgt_index_extended_block =
        get_array(char*, edge_block_index,
                  edge_block_index->offset_tgt_index_bit_extension);

    // if using the extended index we need to and the bits out:
    if (use_extended_index_bits) {
      for (uint32_t i = 0; i < src_size; ++i) {
        src_index_block[i] = ctx.src_local_to_global_[i] & UINT32_MAX;
        set_bool_array(src_index_extended_block, i,
                       (ctx.src_local_to_global_[i] & (1ul << 32)));
      }
      for (uint32_t i = 0; i < tgt_size; ++i) {
        tgt_index_block[i] = ctx.tgt_local_to_global_[i] & UINT32_MAX;
        set_bool_array(tgt_index_extended_block, i,
                       (ctx.tgt_local_to_global_[i] & (1ul << 32)));
      }
    } else {
      memcpy(src_index_block, ctx.src_local_to_global_.data(), src_size_bytes);
      memcpy(tgt_index_block, ctx.tgt_local_to_global_.data(), tgt_size_bytes);
    }

#ifdef SCALABLE_GRAPHS_DEBUG
    for (int i = 0; i < src_size; ++i) {
      sg_assert(src_index_block[i] == ctx.src_local_to_global_[i], "");
    }
    for (int i = 0; i < tgt_size; ++i) {
      sg_assert(tgt_index_block[i] == ctx.tgt_local_to_global_[i], "");
    }

    // assert the edge_block-indices as well:
    sg_assert(edge_block_index->block_id == block->block_id, "");
    sg_assert(edge_block_index->count_src_vertices == src_size, "");
    sg_assert(edge_block_index->count_tgt_vertices == tgt_size, "");
#endif

    std::string edge_block_index_src_file_name =
        core::getEdgeTileIndexFileName(config_, block->block_id);

    util::writeDataToFile(edge_block_index_src_file_name,
                          reinterpret_cast<const void*>(edge_block_index),
                          size_edge_block_index);

    // // calculate vertex-to-tile statistics based on tgt-index-blocks
    // for (int i = 0; i < edge_block_index->count_tgt_vertices; ++i) {
    //   ++ctx.vertex_to_tiles_per_vertices_[tgt_index_block[i]];
    // }

    free(edge_block_index);

    // write stat file for current tile
    tile_stats_t* stat = (tile_stats_t*)malloc(sizeof(tile_stats_t));
    stat->count_edges = edge_count;
    stat->count_vertex_src = src_size;
    stat->count_vertex_tgt = tgt_size;
    stat->block_id = block->block_id;
    stat->use_rle = use_rle;

    std::string stat_file_name =
        core::getEdgeTileStatFileName(config_, block->block_id);
    util::writeDataToFile(stat_file_name, reinterpret_cast<const void*>(stat),
                          sizeof(tile_stats_t));

    free(stat);

    // clear all intermediate information:
    ctx.local_vertex_id_src_base_ = 0;
    ctx.local_vertex_id_tgt_base_ = 0;
    ctx.edge_set_.clear();
    ctx.src_set_.clear();
    ctx.tgt_set_.clear();
    ctx.src_global_to_local_.clear();
    ctx.tgt_global_to_local_.clear();
    ctx.src_local_to_global_.clear();
    ctx.tgt_local_to_global_.clear();

    // now write the block to disk:
    std::string file_name = core::getEdgeTileFileName(config_, block->block_id);
    util::writeDataToFile(file_name, reinterpret_cast<const void*>(block),
                          malloc_edge_block_size);

    // sg_log("Wrote block %d\n", block->block_id);
    // and clean up:
    free(block);
  }

  template <typename TVertexIdType>
  RMATTileManager<TVertexIdType>::~RMATTileManager() {
    pthread_spin_destroy(&gv_lock);
  }
}
}
