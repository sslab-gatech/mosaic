#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "tile-manager.h"
#endif

#include "partition-manager.h"

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
#include <util/column_first.h>
#include <util/row_first.h>
#include <core/datatypes.h>
#include <util/util.h>
#include <util/arch.h>
#include <core/util.h>

namespace scalable_graphs {
namespace graph_load {

  struct partition_range_per_thread {
    size_t start;
    size_t end;
  };

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  struct ThreadInfo {
    pthread_t thr;
    TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>* tm;
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

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::TileManager(
      const tile_manager_arguments_t& arguments)
      : partition_managers_(arguments.partition_managers),
        config_(arguments.config), partition_range_per_thread_(NULL) {
    vertex_to_tiles_per_vertices_ =
        (uint32_t*)calloc(1, sizeof(uint32_t) * config_.count_vertices);
    pthread_spin_init(&gv_lock, PTHREAD_PROCESS_PRIVATE);
  }

  template <typename TLocalEdgeType>
  inline void initEdge(TLocalEdgeType& edge) {}

  template <>
  inline void initEdge<local_edge_weighted_t>(local_edge_weighted_t& edge) {
    static unsigned int seed = rand32_seedless();
    edge.weight = rand32(&seed) / (float)UINT32_MAX;
  }

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  void TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::processPartition(
      TileManager::Context& ctx, int src, int tgt) {
    // load partition
    partition_edge_t* partition = getEdges({(uint64_t)src, (uint64_t)tgt});

    // process partition
    for (int i = 0; i < partition->count_edges; ++i) {
      TEdgeType edge;
      edge.src = partition->edges[i].src;
      edge.tgt = partition->edges[i].tgt;

      sg_assert(edge.src < config_.count_vertices, "src < count-vertices");
      sg_assert(edge.tgt < config_.count_vertices, "tgt < count-vertices");

      if (needToStartNewEdgeBlock(ctx, edge)) {
        writeTile(ctx);
        uint64_t traversal_id;
        switch (config_.traversal) {
        case grc_tile_traversals_t::Hilbert: {
          traversal_id = ::traversal::hilbert::xy2d(
              config_.count_rows_partitions, src, tgt);
          break;
        }
        case grc_tile_traversals_t::ColumnFirst: {
          traversal_id = ::traversal::column_first::xy2d(
              config_.count_rows_partitions, src, tgt);
          break;
        }
        case grc_tile_traversals_t::RowFirst: {
          traversal_id = ::traversal::row_first::xy2d(
              config_.count_rows_partitions, src, tgt);
          break;
        }
        default:
          sg_log("Wrong traveral given: %d\n", config_.traversal);
          util::die(1);
        }
        ctx.block_id = traversal_id;
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

      TLocalEdgeType local_edge;
      local_edge.src = src_local;
      local_edge.tgt = tgt_local;
      // this allows the weighted-version to add the weight
      initEdge<TLocalEdgeType>(local_edge);
      ctx.edge_set_.push_back(local_edge);
    }

    // clean up
    free(partition);
  }

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::Context::Context(
      int64_t start_id, uint64_t count_partitions, uint64_t count_vertices,
      const std::vector<std::string>& paths_to_partitions)
      : vertex_to_tiles_per_vertices_(NULL), local_vertex_id_src_base_(0),
        local_vertex_id_tgt_base_(0), start_id_(start_id), count_tiles_(0),
        count_edges_(0) {
    vertex_to_tiles_per_vertices_ =
        (uint32_t*)calloc(1, sizeof(uint32_t) * count_vertices);
  }

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::Context::~Context() {
    free(vertex_to_tiles_per_vertices_);
  }

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  partition_edge_t*
  TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::getEdges(
      const partition_t& partition) {
    partition_t partition_partition_manager =
        core::getPartitionManagerOfPartition(partition, config_);

    size_t offset_partition_manager =
        core::getIndexOfPartitionManager(partition_partition_manager, config_);

    return partition_managers_[offset_partition_manager]->getPartition(
        partition);
  }

  template<typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  size_t
  TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::getSize(
      const partition_t& partition) {
    partition_t partition_partition_manager =
        core::getPartitionManagerOfPartition(partition, config_);

    size_t offset_partition_manager =
        core::getIndexOfPartitionManager(partition_partition_manager, config_);

    return partition_managers_[offset_partition_manager]->getSize(partition);
  }

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  void TileManager<TVertexIdType, TLocalEdgeType,
                   TEdgeType>::processPartitionsInRange(int64_t start,
                                                        int64_t end,
                                                        int64_t* count_tiles,
                                                        int64_t* count_edges) {
    Context ctx(start, config_.count_rows_partitions, config_.count_vertices,
                config_.paths_to_partition);
    ctx.block_id = start;

    // tiling a range in a Hilbert order
    for (int64_t i = start; i < end; ++i) {
      int64_t x, y;
      switch (config_.traversal) {
      case grc_tile_traversals_t::Hilbert: {
        ::traversal::hilbert::d2xy(config_.count_rows_partitions, i, &x, &y);
        break;
      }
      case grc_tile_traversals_t::ColumnFirst: {
        ::traversal::column_first::d2xy(config_.count_rows_partitions, i, &x,
                                        &y);
        break;
      }
      case grc_tile_traversals_t::RowFirst: {
        ::traversal::row_first::d2xy(config_.count_rows_partitions, i, &x, &y);
        break;
      }
      default:
        sg_log("Wrong traveral given: %d\n", config_.traversal);
        util::die(1);
      }
      processPartition(ctx, x, y);
    }

    // one final write, if something to write:
    if (ctx.edge_set_.size() > 0) {
      writeTile(ctx);
    }

    pthread_spin_lock(&gv_lock);
    {
      for (uint64_t id = 0; id < config_.count_vertices; ++id) {
        vertex_to_tiles_per_vertices_[id] +=
            ctx.vertex_to_tiles_per_vertices_[id];
      }
    }
    pthread_spin_unlock(&gv_lock);

    // return the number of written tiles
    *count_tiles = ctx.count_tiles_;
    *count_edges = ctx.count_edges_;
  }

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  int TileManager<TVertexIdType, TLocalEdgeType,
                  TEdgeType>::calcProperNumThreads(int max_thread) {
    size_t p2 = config_.count_rows_partitions * config_.count_rows_partitions;
    size_t num_chunk = p2 / 4L;
    int nthread = (int) std::min(num_chunk, (const size_t&) max_thread);
    return nthread;
  }

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  void* TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::threadMain(void* arg) {
    auto ti = static_cast<ThreadInfo<TVertexIdType, TLocalEdgeType, TEdgeType>*>(arg);
    ti->tm->processPartitionsInRange(ti->start, ti->end, &ti->count_tiles, &ti->count_edges);
    return NULL;
  }

  template<typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  size_t TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::getCountEdges() {
    size_t count_edges = 0;
    for (int x = 0; x < config_.count_rows_partitions; ++x) {
      for (int y = 0; y < config_.count_rows_partitions; ++y) {
        count_edges += getSize({(uint64_t) x, (uint64_t) y});
      }
    }
    return count_edges;
  }

  template<typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  void TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::computePartitionsPerThread(int num_threads) {
    partition_range_per_thread_ = new partition_range_per_thread[num_threads];

    uint64_t time_start = util::get_time_nsec();
    size_t count_edges = this->getCountEdges();
    sg_log("Time taken for count Edges: %f\n", ((double) util::get_time_nsec() - time_start) / 1000000000.);

    size_t max_edges_per_thread = count_edges / num_threads;

    sg_log("Count Edges: %lu, Edges per thread: %lu\n", count_edges, max_edges_per_thread);

    size_t edges_processed = 0;
    size_t edges_current_thread = 0;
    size_t current_start = 0;
    uint32_t current_thread = 0;
    uint64_t current_partition_count = 0;

    size_t p2 = (size_t) pow(config_.count_rows_partitions, 2);

    for (size_t i = 0; i < p2; ++i) {
      int64_t x = 0, y = 0;
      switch (config_.traversal) {
        case grc_tile_traversals_t::Hilbert: {
          ::traversal::hilbert::d2xy(config_.count_rows_partitions, i, &x, &y);
          break;
        }
        case grc_tile_traversals_t::ColumnFirst: {
          ::traversal::column_first::d2xy(config_.count_rows_partitions, i, &x, &y);
          break;
        }
        case grc_tile_traversals_t::RowFirst: {
          ::traversal::row_first::d2xy(config_.count_rows_partitions, i, &x, &y);
          break;
        }
        default:
          sg_log("Wrong traveral given: %d\n", config_.traversal);
          util::die(1);
      }
      size_t edges_partition = getSize({(uint64_t) x, (uint64_t) y});
      edges_processed += edges_partition;

      // Check if current thread would cross edge-boundary with this partition.
      // If so, assign partition to next thread, but only if the current thread has at least one partition assigned
      // already or the current thread is not the last thread.
      if ((current_thread != num_threads - 1) && (current_partition_count > 0) &&
          (edges_current_thread + edges_partition > max_edges_per_thread)) {
        // If the edges in the current partition are more than the threshold, finish the current thread and start a new
        // one.
        partition_range_per_thread_[current_thread].start = current_start;
        partition_range_per_thread_[current_thread].end = i;

        sg_log("Thread %d: Start %lu, End %lu, Edges %lu\n",
               current_thread,
               partition_range_per_thread_[current_thread].start,
               partition_range_per_thread_[current_thread].end,
               edges_current_thread);

        // Let next start from here.
        current_start = i;
        current_thread++;
        edges_current_thread = 0;
        current_partition_count = 0;

        // Recalculate maximum edges per thread to cater to remaining edges more evenly.
        max_edges_per_thread = (count_edges - edges_processed) / (num_threads - current_thread);

        assert(current_thread < num_threads);
      }
      edges_current_thread += edges_partition;
      ++current_partition_count;
    }

    // Assign left-over partitions to last thread.
    partition_range_per_thread_[current_thread].start = current_start;
    partition_range_per_thread_[current_thread].end = p2;
    sg_log("Thread %d: Start %lu, End %lu, Edges %lu\n",
           current_thread,
           partition_range_per_thread_[current_thread].start,
           partition_range_per_thread_[current_thread].end,
           edges_current_thread);

    // Make sure the remaining threads get start/end assigned even if it is empty.
    for (uint32_t i = current_thread + 1; i < num_threads; ++i) {
      partition_range_per_thread_[i].start = 0;
      partition_range_per_thread_[i].end = 0;
    }
  }

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  void
  TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::generateAndWriteTiles(
      int max_thread) {
    int nthread = calcProperNumThreads(max_thread);

    uint64_t time_start = util::get_time_nsec();
    this->computePartitionsPerThread(nthread);
    sg_log("Time taken for compute Partitions: %f\n", ((double) util::get_time_nsec() - time_start) / 1000000000.);

    sg_log("Generating tiles with %d threads concurrently.\n", nthread);

    // Launch tiling threads.
    std::vector<ThreadInfo<TVertexIdType, TLocalEdgeType, TEdgeType>*> threads;
    for (int i = 0; i < (nthread - 1); ++i) {
      size_t start = partition_range_per_thread_[i].start;
      size_t end = partition_range_per_thread_[i].end;

      // Skip threads with empty partition count.
      if (start == end) {
        continue;
      }

      auto ti = new ThreadInfo<TVertexIdType, TLocalEdgeType, TEdgeType>;
      ti->tm = this;
      ti->start = start;
      ti->end = end;
      ti->count_tiles = 0;
      ti->count_edges = 0;

      int rc = pthread_create(
          &ti->thr, NULL,
          TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::threadMain,
          ti);
      threads.push_back(ti);
    }

    // Do my job.
    int64_t count_tiles = 0;
    int64_t count_edges = 0;

    size_t start_last_thread = partition_range_per_thread_[nthread - 1].start;
    size_t end_last_thread = partition_range_per_thread_[nthread - 1].end;
    // Only start last thread if any partitions left to work on.
    if (start_last_thread != end_last_thread) {
      processPartitionsInRange(start_last_thread, end_last_thread, &count_tiles, &count_edges);
    }

    int rc = 0;
    // Wait for parsing threads.
    for (const auto& it : threads) {
      int rc2 = pthread_join(it->thr, NULL);
      if (rc2)
        rc = rc2;
      count_tiles += it->count_tiles;
      count_edges += it->count_edges;
      delete it;
    }

    if (rc)
      util::die(1);

    // write the vertex -> tile-count file:
    std::string vertex_to_tile_count_file_name =
        core::getVertexToTileCountFileName(config_);
    size_t size_vertex_to_tile_count_file =
        sizeof(uint32_t) * config_.count_vertices;

    util::writeDataToFile(
        vertex_to_tile_count_file_name,
        reinterpret_cast<const void*>(vertex_to_tiles_per_vertices_),
        size_vertex_to_tile_count_file);

    size_t count_vertex_to_tiles = 0;
    for (int i = 0; i < config_.count_vertices; ++i) {
      count_vertex_to_tiles += vertex_to_tiles_per_vertices_[i];
    }
    size_t index_size = count_vertex_to_tiles * sizeof(uint32_t) +
                        sizeof(size_t) * config_.count_vertices;

    sg_log("Count vertex-to-tiles: %lu\n", count_vertex_to_tiles);
    sg_log("Size Index: %lu\n", index_size);
    sg_log("Count Edges: %lu\n", count_edges);

    // first, read the global stats file from the partitioner:
    std::string partitioner_global_stats_file_name =
        core::getGlobalStatFileName(config_);

    scenario_stats_t partitioner_stats;
    util::readDataFromFile(partitioner_global_stats_file_name.c_str(),
                           sizeof(scenario_stats_t), &partitioner_stats);

    // write the global stats file:
    std::string global_stats_file_name = core::getGlobalStatFileName(config_);

    scenario_stats_t stat;
    stat.count_tiles = count_tiles;
    stat.count_vertices = partitioner_stats.count_vertices;
    stat.is_weighted_graph = config_.output_weighted;

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

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  bool TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::
      needToStartNewEdgeBlock(
          TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::Context& ctx,
          const TEdgeType& edge) {
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

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  void TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::writeTile(
      TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::Context& ctx) {
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
    if (config_.output_weighted) {
      size_edge_weights_block = sizeof(float) * edge_count;
    }

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
    // the rle-representation
    local_vertex_id_t* edge_tgt_block =
        get_array(local_vertex_id_t*, block, block->offset_tgt);
    vertex_count_t* edge_tgt_block_rle =
        get_array(vertex_count_t*, block, block->offset_tgt);

    // only include weight if this graph shall be output as a weighted version
    float* edge_weight_block =
        config_.output_weighted ? get_array(float*, block, block->offset_weight)
                                : NULL;

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

      if (config_.output_weighted) {
        addEdgeWeightToBlock<TLocalEdgeType>(ctx.edge_set_[i],
                                             edge_weight_block[i]);
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

    // calculate vertex-to-tile statistics based on tgt-index-blocks
    for (int i = 0; i < edge_block_index->count_tgt_vertices; ++i) {
      ++ctx.vertex_to_tiles_per_vertices_[tgt_index_block[i]];
    }

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

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::~TileManager() {
    pthread_spin_destroy(&gv_lock);
    if (partition_range_per_thread_ != NULL) {
      delete partition_range_per_thread_;
    }
  }
}
}
