#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <pthread.h>

#include <core/datatypes.h>
#include "partition-manager.h"
#include "abstract-partition-manager.h"

namespace scalable_graphs {
namespace graph_load {

  struct tile_manager_arguments_t {
    AbstractPartitionManager** partition_managers;
    config_tiler_t config;
  };

  struct partition_range_per_thread;

  template <typename TVertexIdType, typename TLocalEdgeType, typename TEdgeType>
  class TileManager {
  public:
    TileManager(const tile_manager_arguments_t& arguments);

    void generateAndWriteTiles(int max_thread);

    ~TileManager();

  private:
    class Context {
    public:
      uint32_t block_id;
      std::vector<TLocalEdgeType> edge_set_;
      uint32_t* vertex_to_tiles_per_vertices_;

      local_vertex_id_t local_vertex_id_src_base_;
      local_vertex_id_t local_vertex_id_tgt_base_;

      // Intuition: Use the set for lookups, the map for mapping
      // vertex_id->local_id,
      // the vector for mapping local_id->vertex_id
      std::unordered_set<TVertexIdType> src_set_;
      std::unordered_set<TVertexIdType> tgt_set_;
      std::unordered_map<TVertexIdType, local_vertex_id_t> src_global_to_local_;
      std::unordered_map<TVertexIdType, local_vertex_id_t> tgt_global_to_local_;
      std::vector<TVertexIdType> src_local_to_global_;
      std::vector<TVertexIdType> tgt_local_to_global_;

      int64_t start_id_;
      int64_t count_tiles_;
      int64_t count_edges_;

    public:
      Context(int64_t start_id, uint64_t count_partitions,
              uint64_t count_vertices,
              const std::vector<std::string>& paths_to_partitions);
      ~Context();
    };

  private:
    void processPartitionsInRange(int64_t start, int64_t end,
                                  int64_t* count_tiles, int64_t* count_edges);

    partition_edge_t* getEdges(const partition_t& partition);

    size_t getSize(const partition_t& partition);

    size_t getCountEdges();

    void processPartition(Context& ctx, int src, int tgt);

    int calcProperNumThreads(int max_thread);

    bool needToStartNewEdgeBlock(
        TileManager<TVertexIdType, TLocalEdgeType, TEdgeType>::Context& ctx,
        const TEdgeType& edge);

    void writeTile(Context& ctx);

    void computePartitionsPerThread(int num_threads);

    static void* threadMain(void* arg);

    int64_t count_tiles_;

    partition_range_per_thread* partition_range_per_thread_;

    config_tiler_t config_;
    AbstractPartitionManager** partition_managers_;

    uint32_t* vertex_to_tiles_per_vertices_;

    pthread_spinlock_t gv_lock;

    const static int64_t min_partitions_ = 2;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "tile-manager.cc"
#endif
