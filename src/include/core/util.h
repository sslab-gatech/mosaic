#pragma once

#include <cmath>
#include <string>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <util/util.h>
#include <core/datatypes.h>

namespace scalable_graphs {
namespace core {
  std::string
  getVertexOutputFileName(const std::string& fault_tolerance_ouput_path,
                          const int iteration);

  std::string getVertexDegreeFileName(const std::string& path_to_global);
  std::string getVertexDegreeFileName(const config_t& config);
  std::string getVertexDegreeFileName(const config_grc_t& config);

  std::string getGlobalToOrigIDFileName(const std::string& path_to_global);
  std::string getGlobalToOrigIDFileName(const config_t& config);
  std::string getGlobalToOrigIDFileName(const config_grc_t& config);

  std::string getGlobalStatFileName(const std::string& path_to_global);
  std::string getGlobalStatFileName(const config_t& config);
  std::string getGlobalStatFileName(const config_grc_t& config);

  std::string getVertexToTileCountFileName(const std::string& path_to_global);
  std::string getVertexToTileCountFileName(const config_t& config);
  std::string getVertexToTileCountFileName(const config_tiler_t& config);

  std::string getVertexToTileIndexFileName(const std::string& path_to_global);
  std::string getVertexToTileIndexFileName(const config_t& config);

  std::string getGlobalTileStatsFileName(const std::string& path_to_meta);
  std::string getGlobalTileStatsFileName(const config_t& config,
                                         const int mic_index);

  std::string getEdgeTileStatFileName(const std::string& path_to_meta,
                                      uint64_t index);
  std::string getEdgeTileStatFileName(const config_tiler_t& config,
                                      uint64_t index);
  std::string getEdgeTileStatFileName(const config_t& config, uint64_t index);

  std::string getEdgeTileFileName(const std::string& path_to_tile,
                                  uint64_t index);
  std::string getEdgeTileFileName(const config_tiler_t& config, uint64_t index);

  std::string getEdgeTileFileName(const config_t& config, int meta_index);

  std::string getEdgeTileIndexFileName(const std::string& path_to_meta,
                                       uint64_t index);
  std::string getEdgeTileIndexFileName(const config_tiler_t& config,
                                       uint64_t index);

  std::string getEdgeTileIndexFileName(const config_t& config, int meta_index);

  std::string getResultFileName(const std::string& path_to_output,
                                int iteration);

  std::string
  getPartitionFileName(const std::vector<std::string>& paths_to_partitions,
                       int pt_idx, int src, int tgt);

  std::string getMetaPartitionFileName(const config_grc_t& config,
                                       const partition_t& partition);

  std::string getMetaPartitionInfoFileName(const config_tiler_t& config,
                                           const partition_t& partition);

  int countTilesPerMic(const config_t& config, const int mic_index);

  int countTilesLowerMics(const config_t& config, const int mic_index);

  void initGrcConfig(config_grc_t* config, uint64_t count_vertices,
                     const command_line_args_grc_t cmd_args);

  partition_manager_arguments_t
  getPartitionManagerArguments(const int index_partition_manager,
                               const config_grc_t& config);

  meta_partition_meta_t getGlobalPartitionInfoForPartitionStore(
      const config_grc_t& config,
      const meta_partition_meta_t& meta_partition_manager,
      const meta_partition_meta_t& partition_store_local_partition_info,
      int index_partition_store);

  meta_partition_meta_t
  getLocalPartitionInfoForPartitionStore(const config_grc_t& config,
                                         int index_partition_store);

  size_t getMetaPartitionFileSize();

  size_t getMaxMetaPartitionEdgeCount();

  size_t getSizeTileBlock(const vertex_edge_tiles_block_sizes_t& sizes);

  void fillTileBlockHeader(vertex_edge_tiles_block_t* tile_block,
                           uint64_t block_id, const tile_stats_t& tile_stats,
                           const vertex_edge_tiles_block_sizes_t& sizes,
                           uint32_t num_tile_partition,
                           uint32_t tile_partition_id);

  template <class APP, typename TVertexType>
  vertex_edge_tiles_block_sizes_t getMaxTileBlockSizes() {
    vertex_edge_tiles_block_sizes_t sizes;

    sizes.count_active_vertex_src_block =
        APP::need_active_source_input ? size_bool_array(MAX_VERTICES_PER_TILE)
                                      : 0;
    sizes.count_active_vertex_tgt_block =
        APP::need_active_target_block ? size_bool_array(MAX_VERTICES_PER_TILE)
                                      : 0;
    sizes.size_active_vertex_src_block =
        APP::need_active_source_input
            ? sizeof(char) * size_bool_array(MAX_VERTICES_PER_TILE)
            : 0;
    sizes.size_active_vertex_tgt_block =
        APP::need_active_target_block
            ? sizeof(char) * size_bool_array(MAX_VERTICES_PER_TILE)
            : 0;
    sizes.size_src_degree_block =
        APP::need_degrees_source_block
            ? sizeof(vertex_degree_t) * MAX_VERTICES_PER_TILE
            : 0;
    sizes.size_tgt_degree_block =
        APP::need_degrees_target_block
            ? sizeof(vertex_degree_t) * MAX_VERTICES_PER_TILE
            : 0;
    sizes.size_source_vertex_block =
        sizeof(TVertexType) * MAX_VERTICES_PER_TILE;
    sizes.size_extension_fields_vertex_block =
        APP::max_size_extension_fields_vertex_block;

    return sizes;
  }

  template <class APP, typename TVertexType>
  vertex_edge_tiles_block_sizes_t
  getTileBlockSizes(const tile_stats_t& tile_stats) {
    vertex_edge_tiles_block_sizes_t sizes;

    sizes.count_active_vertex_src_block =
        APP::need_active_source_input
            ? size_bool_array(tile_stats.count_vertex_src)
            : 0;
    sizes.count_active_vertex_tgt_block =
        APP::need_active_target_block
            ? size_bool_array(tile_stats.count_vertex_tgt)
            : 0;
    sizes.size_active_vertex_src_block =
        APP::need_active_source_input
            ? sizeof(char) * sizes.count_active_vertex_src_block
            : 0;
    sizes.size_active_vertex_tgt_block =
        APP::need_active_target_block
            ? sizeof(char) * sizes.count_active_vertex_tgt_block
            : 0;
    sizes.size_src_degree_block =
        APP::need_degrees_source_block
            ? sizeof(vertex_degree_t) * tile_stats.count_vertex_src
            : 0;
    sizes.size_tgt_degree_block =
        APP::need_degrees_target_block
            ? sizeof(vertex_degree_t) * tile_stats.count_vertex_tgt
            : 0;
    sizes.size_source_vertex_block =
        sizeof(TVertexType) * tile_stats.count_vertex_src;
    sizes.size_extension_fields_vertex_block =
        APP::sizeExtensionFieldsVertexBlock(tile_stats);

    return sizes;
  }

  template <typename K, typename V>
  void writeMapToFile(const std::string& file_name,
                      const std::unordered_map<K, V>& map) {
    sg_dbg("Write map to %s\n", file_name.c_str());
    FILE* file = fopen(file_name.c_str(), "wb");
    if (!file) {
      sg_dbg("File %s couldn't be written!\n", file_name.c_str());
      scalable_graphs::util::die(1);
    }
    for (auto& it : map) {
      fwrite(&it.first, sizeof(K), 1, file);
      fwrite(&it.second, sizeof(V), 1, file);
    }
    fclose(file);
  }

  template <typename K, typename V>
  void readMapFromFile(const std::string& file_name,
                       std::unordered_map<K, V>& map) {
    sg_dbg("Read %s\n", file_name.c_str());
    FILE* file = fopen(file_name.c_str(), "rb");
    if (!file) {
      sg_dbg("File %s couldn't be read!\n", file_name.c_str());
      scalable_graphs::util::die(1);
    }
    K key;
    // while there are more keys, read one key and value:
    while (fread(&key, sizeof(K), 1, file) == 1) {
      V value;
      if (fread(&value, sizeof(V), 1, file) != 1) {
        sg_dbg("Error reading value in %s!\n", file_name.c_str());
        scalable_graphs::util::die(1);
      }
      map[key] = value;
    }
    fclose(file);
  }

  template <typename T, typename TVertexIdTypeKey, typename TVertexIdTypeValue>
  void writeOutput(const std::unordered_map<TVertexIdTypeKey,
                                            TVertexIdTypeValue>& global_to_orig,
                   const std::string& path, int iteration,
                   const size_t count_vertices, const T* vertices) {
    std::string output_file_name = getResultFileName(path, iteration);
    sg_dbg("Write output to %s\n", output_file_name.c_str());

    // iterate vertices, translate back to original ids and write as
    // space-separated values to file, one pair per line
    std::ofstream stream(output_file_name.c_str(),
                         std::fstream::out | std::fstream::trunc);
    if (stream) {
      if (!stream.good()) {
        sg_dbg("File couldn't be written: %s\n", output_file_name.c_str());
        scalable_graphs::util::die(1);
      }
      for (uint64_t i = 0; i < count_vertices; ++i) {
        auto it = global_to_orig.find(i);
        if (it == global_to_orig.end()) {
          sg_dbg("Global id not found for id %lu\n", i);
          scalable_graphs::util::die(1);
        }
        vertex_id_t global_id = it->second;
        stream << global_id << " " << vertices[i] << std::endl;
      }
    }
    stream.close();
  }

  // used by edge-reader/creator, input in global coordinates, output in global
  // partition-store-coordinates
  template <typename T>
  inline partition_t getMetaPartitionOfEdge(const T& edge) {
    return {(size_t)edge.src / (size_t)VERTICES_PER_SPARSE_FILE,
            (size_t)edge.tgt / (size_t)VERTICES_PER_SPARSE_FILE};
  }

  // used by edge-reader/creator, input in global coordinates, output in global
  // partition-manager-coordinates
  template <typename T>
  inline partition_t getPartitionManagerOfEdge(const T& edge,
                                               const config_grc_t& config) {
    return {(size_t)edge.src / config.count_rows_vertices_per_partition_manager,
            (size_t)edge.tgt /
                config.count_rows_vertices_per_partition_manager};
  }

  // used by edge-reader/creator, input in global coordinates, output in global
  // partition-store-coordinates
  inline partition_t getMetaPartitionOfPartition(const partition_t& partition) {
    return {partition.i / PARTITION_COLS_PER_SPARSE_FILE,
            partition.j / PARTITION_COLS_PER_SPARSE_FILE};
  }

  // used by edge-reader/creator, input in global coordinates, output in global
  // partition-manager-coordinates
  inline partition_t
  getPartitionManagerOfPartition(const partition_t& partition,
                                 const config_grc_t& config) {
    return {partition.i /
                (config.count_rows_partition_stores_per_partition_manager *
                 PARTITION_COLS_PER_SPARSE_FILE),
            partition.j /
                (config.count_rows_partition_stores_per_partition_manager *
                 PARTITION_COLS_PER_SPARSE_FILE)};
  }

  // used by PartitionManager, input in global coordinates, output in
  // partition-manager-local coordinates
  template <typename T>
  inline partition_t
  getPartitionStoreOfEdge(const T& edge, const meta_partition_meta_t& meta) {
    // return the meta-partition inside a partition-manager, subtract edge from
    // meta-vertex-start
    return getMetaPartitionOfEdge<T>(
        {(size_t)edge.src - meta.vertex_partition_index_start.i,
         (size_t)edge.tgt - meta.vertex_partition_index_start.j});
  }

  // used by PartitionManager, input in global coordinates, output in
  // partition-manager-local coordinates
  inline partition_t
  getPartitionStoreOfPartition(const partition_t& partition,
                               const meta_partition_meta_t& meta) {
    return {(partition.i -
             (meta.meta_partition_index.i * PARTITION_COLS_PER_SPARSE_FILE)) /
                PARTITION_COLS_PER_SPARSE_FILE,
            (partition.j -
             (meta.meta_partition_index.j * PARTITION_COLS_PER_SPARSE_FILE)) /
                PARTITION_COLS_PER_SPARSE_FILE};
  }

  // used by PartitionStore, input in global coordinates, output in
  // partition-store-local coordinates,
  template <typename T>
  inline partition_t
  getPartitionInsidePartitionStoreOfEdge(const T& edge,
                                         const meta_partition_meta_t& meta) {
    return {((size_t)edge.src - meta.vertex_partition_index_start.i) /
                MAX_VERTICES_PER_TILE,
            ((size_t)edge.tgt - meta.vertex_partition_index_start.j) /
                MAX_VERTICES_PER_TILE};
  }

  inline partition_t getPartitionInsidePartitionStoreOfPartition(
      const partition_t& partition, const meta_partition_meta_t& meta) {
    return {partition.i -
                (meta.meta_partition_index.i * PARTITION_COLS_PER_SPARSE_FILE),
            partition.j -
                (meta.meta_partition_index.j * PARTITION_COLS_PER_SPARSE_FILE)};
  }

  inline size_t getPartitionIndex(const partition_t partition,
                                  size_t items_per_row) {
    return partition.i * (size_t)items_per_row + partition.j;
  }

  inline size_t
  getIndexOfPartitionStore(const partition_t& partition_store_partition,
                           const config_grc_t& config) {
    return core::getPartitionIndex(
        partition_store_partition,
        config.count_rows_partition_stores_per_partition_manager);
  }

  inline size_t getIndexOfPartitionInFile(const partition_t& partition) {
    return core::getPartitionIndex(partition, PARTITION_COLS_PER_SPARSE_FILE);
  }

  inline size_t getIndexOfPartitionManager(const partition_t& partition,
                                           const config_grc_t& config) {
    return core::getPartitionIndex(partition,
                                   config.count_rows_partition_managers);
  }

  inline size_t
  getOffsetOfPartition(const partition_t internal_partition_of_edge) {
    // row-first unrolling
    // also consider:
    //  - partition-meta-info at the beginning of the file
    //  - per-partition meta-info at the beginning of the file
    // then, add up all components:
    // $meta + (i * rows + j) * max_tile_size
    return sizeof(meta_partition_file_info_t) +
           PARTITIONS_PER_SPARSE_FILE * sizeof(partition_edge_compact_t) +
           (getIndexOfPartitionInFile(internal_partition_of_edge) *
            MAX_EDGES_PER_TILE * sizeof(local_edge_t));
  }

  inline size_t
  getEdgeOffsetOfPartition(const partition_t internal_partition_of_edge) {
    return (getIndexOfPartitionInFile(internal_partition_of_edge) *
            MAX_EDGES_PER_TILE_IN_MEMORY * sizeof(local_edge_t));
  }

  inline partition_t getGlobalVertexIdsForPartitionInsidePartitionStore(
      const meta_partition_meta_t& global_partition_info,
      const partition_t& partition) {
    partition_t global_vertex_ids;

    global_vertex_ids.i = global_partition_info.vertex_partition_index_start.i +
                          MAX_VERTICES_PER_TILE * partition.i;
    global_vertex_ids.j = global_partition_info.vertex_partition_index_start.j +
                          MAX_VERTICES_PER_TILE * partition.j;

    return global_vertex_ids;
  }

  inline partition_t getPartitionOfIndexInsidePartitionStore(const int& id) {
    partition_t partition;

    partition.i = id / PARTITION_COLS_PER_SPARSE_FILE;
    partition.j = id - PARTITION_COLS_PER_SPARSE_FILE * partition.i;

    return partition;
  }

  inline int getPartitionOfVertex(const vertex_id_t vertex_id,
                                  const int count_global_reducers) {
    return (vertex_id / VERTICES_PER_PARTITION_STRIPE) % count_global_reducers;
  }

  inline bool isEdgeInTargetArea(const vertex_area_t& area,
                                 const edge_t& edge) {
    bool src_check = (edge.src >= area.src_min && edge.src < area.src_max);
    bool tgt_check = (edge.tgt >= area.tgt_min && edge.tgt < area.tgt_max);
    return src_check && tgt_check;
  }

  inline bool isPartitionManagerInTargetArea(const vertex_area_t& area,
                                             const partition_t& partition) {
    bool src_check =
        (partition.i >= area.src_min && partition.i < area.src_max);
    bool tgt_check =
        (partition.j >= area.tgt_min && partition.j < area.tgt_max);
    return src_check && tgt_check;
  }

  inline int getEdgeEngineIndexFromTile(const config_vertex_domain_t& config,
                                        uint32_t tile_id) {
    return tile_id % config.count_edge_processors;
  }

  inline int getLocalTileId(const config_vertex_domain_t& config,
                            uint32_t tile_id) {
    return tile_id / config.count_edge_processors;
  }

  inline void advance_rle_offset_once(uint32_t* tgt_count, uint32_t* rle_offset,
                                      const vertex_count_t* tgt_block_rle) {
    // increase rle-offset-counter if current tgt exhausted
    ++*(tgt_count);

    // potentially, the tgt_count wraps around from 65535 to 0, to
    // indicate a total of 65536 targets, that is fine as the count 0 is
    // never being used.
    if (static_cast<uint16_t>(*tgt_count) == tgt_block_rle[*rle_offset].count) {
      *tgt_count = 0;
      ++*(rle_offset);
    }
  }

  inline void advance_rle_offset(uint32_t advance, uint32_t* tgt_count,
                                 uint32_t* rle_offset,
                                 const vertex_count_t* tgt_block_rle) {
    for (int i = 0; i < advance; ++i) {
      advance_rle_offset_once(tgt_count, rle_offset, tgt_block_rle);
    }
  }

  inline pthread_spinlock_t*
  getSpinlockForVertex(const vertex_id_t& vertex_id,
                       const vertex_lock_table_t& vertex_lock_table) {
    uint32_t seed = vertex_id ^ vertex_lock_table.salt;
    uint32_t rand = rand32(&seed);
    uint32_t offset = rand % VERTEX_LOCK_TABLE_SIZE;
    return &vertex_lock_table.locks[offset];
  }
}
}
