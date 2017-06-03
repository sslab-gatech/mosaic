#include <core/util.h>

#include <sstream>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <algorithm>
#include <stdio.h>
#include <string.h>
#include <util/arch.h>

namespace scalable_graphs {
namespace core {
  enum { num_dir = 512 };

  static int getMetaDirIndex(const config_t& config, uint64_t block_id) {
    int ndir = config.paths_to_meta.size();
    double files_per_dir = config.count_tiles / ndir;
    int dir_idx = block_id / files_per_dir;
    return std::min(dir_idx, ndir - 1);
  }

  static int getTileDirIndex(const config_t& config, uint64_t block_id) {
    int ndir = config.paths_to_tile.size();
    double files_per_dir = config.count_tiles / ndir;
    int dir_idx = block_id / files_per_dir;
    return std::min(dir_idx, ndir - 1);
  }

  static int getMetaDirIndexTiler(const config_tiler_t& config,
                                  uint64_t block_id) {
    int ndir = config.paths_to_meta.size();
    return block_id % ndir;
  }

  static int getTileDirIndexTiler(const config_tiler_t& config,
                                  uint64_t block_id) {
    int ndir = config.paths_to_tile.size();
    return block_id % ndir;
  }

  static int getPartitionDirIndex(const config_grc_t& config,
                                  const partition_t& partition) {
    int ndir = config.paths_to_partition.size();
    size_t partition_index = partition.i + partition.j;
    return partition_index % ndir;
  }

  int countTilesPerMic(const config_t& config, const int mic_index) {
    int count_tiles_per_mic = config.count_tiles / config.count_edge_processors;
    if (mic_index < (config.count_tiles % config.count_edge_processors)) {
      ++count_tiles_per_mic;
    }
    // // bunch up all $k tiles at last mic
    // if (mic_index == (config.count_edge_processors - 1)) {
    //   count_tiles_per_mic += (config.count_tiles % config.count_edge_processors);
    // }
    return count_tiles_per_mic;
  }

  int countTilesLowerMics(const config_t& config, const int mic_index) {
    int count = 0;
    for (int i = 0; i < mic_index; ++i) {
      count += countTilesPerMic(config, i);
    }
    return count;
  }

  std::string
  getVertexOutputFileName(const std::string& fault_tolerance_ouput_path,
                          const int iteration) {
    return fault_tolerance_ouput_path + "vertex-ouput-" +
           std::to_string(iteration) + ".data";
  }

  std::string getVertexDegreeFileName(const std::string& path_to_global) {
    return path_to_global + "vertex_deg.dat";
  }

  std::string getVertexDegreeFileName(const config_t& config) {
    return getVertexDegreeFileName(config.path_to_globals);
  }

  std::string getVertexDegreeFileName(const config_grc_t& config) {
    return getVertexDegreeFileName(config.path_to_globals);
  }

  std::string getGlobalToOrigIDFileName(const std::string& path_to_global) {
    return path_to_global + "vertex_global_to_orig.dat";
  }

  std::string getGlobalToOrigIDFileName(const config_t& config) {
    return getGlobalToOrigIDFileName(config.path_to_globals);
  }

  std::string getGlobalToOrigIDFileName(const config_grc_t& config) {
    return getGlobalToOrigIDFileName(config.path_to_globals);
  }

  std::string getGlobalStatFileName(const std::string& path_to_global) {
    return path_to_global + "stat.dat";
  }

  std::string getGlobalStatFileName(const config_t& config) {
    return getGlobalStatFileName(config.path_to_globals);
  }

  std::string getGlobalStatFileName(const config_grc_t& config) {
    return getGlobalStatFileName(config.path_to_globals);
  }

  std::string getVertexToTileCountFileName(const std::string& path_to_global) {
    return path_to_global + "vertex_to_tile_count.dat";
  }

  std::string getVertexToTileCountFileName(const config_t& config) {
    return getVertexToTileCountFileName(config.path_to_globals);
  }

  std::string getVertexToTileCountFileName(const config_tiler_t& config) {
    return getVertexToTileCountFileName(config.path_to_globals);
  }

  std::string getVertexToTileIndexFileName(const std::string& path_to_global) {
    return path_to_global + "vertex_to_tile_index.dat";
  }

  std::string getVertexToTileIndexFileName(const config_t& config) {
    return getVertexToTileIndexFileName(config.path_to_globals);
  }

  std::string getGlobalTileStatsFileName(const std::string& path_to_meta) {
    return path_to_meta + "tile_stats.dat";
  }

  std::string getGlobalTileStatsFileName(const config_t& config,
                                         const int mic_index) {
    return getGlobalTileStatsFileName(config.paths_to_meta[mic_index]);
  }

  std::string getEdgeTileStatFileName(const std::string& path_to_meta,
                                      uint64_t index) {
    std::stringstream ss;
    ss << path_to_meta << std::setfill('0') << std::setw(3) << std::hex
       << index % num_dir << "/ebs-" << std::setfill('0') << std::setw(8)
       << std::hex << index;
    return ss.str();
  }

  std::string getEdgeTileStatFileName(const config_tiler_t& config,
                                      uint64_t index) {
    int meta_dir = getMetaDirIndexTiler(config, index);
    return getEdgeTileStatFileName(config.paths_to_meta[meta_dir], index);
  }

  std::string getEdgeTileStatFileName(const config_t& config, uint64_t index) {
    int meta_idx = getMetaDirIndex(config, index);
    return getEdgeTileStatFileName(config.paths_to_meta[meta_idx], index);
  }

  std::string getEdgeTileFileName(const std::string& path_to_tile,
                                  uint64_t index) {
    std::stringstream ss;
    ss << path_to_tile << std::setfill('0') << std::setw(3) << std::hex
       << index % num_dir << "/eb-" << std::setfill('0') << std::setw(8)
       << std::hex << index;
    return ss.str();
  }

  std::string getEdgeTileFileName(const config_tiler_t& config,
                                  uint64_t index) {
    int tile_dir = getTileDirIndexTiler(config, index);
    return getEdgeTileFileName(config.paths_to_tile[tile_dir], index);
  }

  std::string getEdgeTileFileName(const config_t& config, int meta_index) {
    return config.paths_to_tile[meta_index] + "tiles.dat";
  }

  std::string getEdgeTileIndexFileName(const std::string& path_to_meta,
                                       uint64_t index) {
    std::stringstream ss;
    ss << path_to_meta << std::setfill('0') << std::setw(3) << std::hex
       << index % num_dir << "/ebi-" << std::setfill('0') << std::setw(8)
       << std::hex << index;
    return ss.str();
  }

  std::string getEdgeTileIndexFileName(const config_tiler_t& config,
                                       uint64_t index) {
    int meta_dir = getMetaDirIndexTiler(config, index);
    return getEdgeTileIndexFileName(config.paths_to_meta[meta_dir], index);
  }

  std::string getEdgeTileIndexFileName(const config_t& config, int meta_index) {
    return config.paths_to_meta[meta_index] + "meta.dat";
  }

  std::string getResultFileName(const std::string& path_to_output,
                                int iteration) {
    std::stringstream ss;
    ss << path_to_output + "result-" << std::setfill('0') << std::setw(8)
       << iteration << ".dat";
    return ss.str();
  }

  std::string
  getPartitionFileName(const std::vector<std::string>& paths_to_partitions,
                       int pt_idx, int src, int tgt) {
    std::stringstream ss;
    ss << paths_to_partitions[pt_idx] << std::setfill('0') << std::setw(3)
       << std::hex << (src + tgt) % num_dir << "/pt-" << std::setfill('0')
       << std::setw(8) << std::hex << src << "-" << std::setfill('0')
       << std::setw(8) << std::hex << tgt << ".dat";
    return ss.str();
  }

  std::string getMetaPartitionFileName(const config_grc_t& config,
                                       const partition_t& partition) {
    int dir_index = getPartitionDirIndex(config, partition);
    std::stringstream ss;
    ss << config.paths_to_partition[dir_index] << std::setfill('0')
       << std::setw(3) << std::hex << (partition.i + partition.j) % num_dir
       << "/meta-pt-" << std::setfill('0') << std::setw(8) << std::hex
       << partition.i << "-" << std::setfill('0') << std::setw(8) << std::hex
       << partition.j << ".dat";
    return ss.str();
  }

  std::string getMetaPartitionInfoFileName(const config_tiler_t& config,
                                           const partition_t& partition) {
    int dir_index = getPartitionDirIndex(config, partition);
    std::stringstream ss;
    ss << config.paths_to_partition[dir_index] << std::setfill('0')
       << std::setw(3) << std::hex << (partition.i + partition.j) % num_dir
       << "/meta-pt-info-" << std::setfill('0') << std::setw(8) << std::hex
       << partition.i << "-" << std::setfill('0') << std::setw(8) << std::hex
       << partition.j << ".dat";
    return ss.str();
  }

  void initGrcConfig(config_grc_t* config, uint64_t count_vertices,
                     const command_line_args_grc_t cmd_args) {
    uint64_t count_partitions = pow(
        2, ceil(log2(ceil((double)count_vertices / MAX_VERTICES_PER_TILE))));

    size_t count_meta_partitions =
        ceil((double)count_partitions / PARTITION_COLS_PER_SPARSE_FILE);
    int adjusted_count_partition_managers =
        std::min((size_t)pow(count_meta_partitions, 2),
                 cmd_args.count_partition_managers);

    int count_partition_managers_per_row =
        sqrt(adjusted_count_partition_managers);
    // ceil the number of vertices per partition manager with the maximum
    // power-of-2-bounded number
    size_t count_vertices_per_partition_manager =
        count_partitions / count_partition_managers_per_row *
        MAX_VERTICES_PER_TILE;
    size_t count_meta_partitions_per_partition_manager =
        std::ceil((double)count_vertices_per_partition_manager /
                  VERTICES_PER_SPARSE_FILE);

    config->nthreads = cmd_args.nthreads;

    config->count_partition_managers = adjusted_count_partition_managers;
    config->count_rows_partition_managers = count_partition_managers_per_row;

    config->count_rows_partitions = count_partitions;

    config->count_rows_meta_partitions = count_meta_partitions;

    config->count_rows_partition_stores_per_partition_manager =
        count_meta_partitions_per_partition_manager;

    config->count_vertices = count_vertices;
    config->count_rows_vertices_per_partition_manager =
        count_vertices_per_partition_manager;

    config->graphname = cmd_args.graphname;
    config->input_weighted = cmd_args.input_weighted;
    config->paths_to_partition = cmd_args.paths_to_partition;
    config->path_to_globals = cmd_args.path_to_globals;
  }

  partition_manager_arguments_t
  getPartitionManagerArguments(const int index_partition_manager,
                               const config_grc_t& config) {
    int index_pm_i =
        index_partition_manager / config.count_rows_partition_managers;
    int index_pm_j = index_partition_manager -
                     index_pm_i * config.count_rows_partition_managers;

    partition_manager_arguments_t arguments;
    arguments.thread_index.id = index_partition_manager;
    arguments.thread_index.count = config.count_partition_managers;

    arguments.meta.meta_partition_index.i =
        index_pm_i * config.count_rows_partition_stores_per_partition_manager;
    arguments.meta.meta_partition_index.j =
        index_pm_j * config.count_rows_partition_stores_per_partition_manager;

    arguments.meta.vertex_partition_index_start.i =
        arguments.meta.meta_partition_index.i * VERTICES_PER_SPARSE_FILE;
    arguments.meta.vertex_partition_index_start.j =
        arguments.meta.meta_partition_index.j * VERTICES_PER_SPARSE_FILE;

    arguments.partitions = config.paths_to_partition;

    return arguments;
  }

  meta_partition_meta_t getGlobalPartitionInfoForPartitionStore(
      const config_grc_t& config,
      const meta_partition_meta_t& meta_partition_manager,
      const meta_partition_meta_t& partition_store_local_partition_info,
      int index_partition_store) {
    meta_partition_meta_t global_partition_info;

    global_partition_info.meta_partition_index.i =
        meta_partition_manager.meta_partition_index.i +
        partition_store_local_partition_info.meta_partition_index.i;
    global_partition_info.meta_partition_index.j =
        meta_partition_manager.meta_partition_index.j +
        partition_store_local_partition_info.meta_partition_index.j;

    global_partition_info.vertex_partition_index_start.i =
        global_partition_info.meta_partition_index.i * VERTICES_PER_SPARSE_FILE;
    global_partition_info.vertex_partition_index_start.j =
        global_partition_info.meta_partition_index.j * VERTICES_PER_SPARSE_FILE;

    return global_partition_info;
  }

  meta_partition_meta_t
  getLocalPartitionInfoForPartitionStore(const config_grc_t& config,
                                         int index_partition_store) {
    meta_partition_meta_t local_partition_info;

    local_partition_info.meta_partition_index.i =
        index_partition_store /
        config.count_rows_partition_stores_per_partition_manager;
    local_partition_info.meta_partition_index.j =
        index_partition_store -
        local_partition_info.meta_partition_index.i *
            config.count_rows_partition_stores_per_partition_manager;

    local_partition_info.vertex_partition_index_start.i =
        local_partition_info.meta_partition_index.i * VERTICES_PER_SPARSE_FILE;
    local_partition_info.vertex_partition_index_start.j =
        local_partition_info.meta_partition_index.j * VERTICES_PER_SPARSE_FILE;

    return local_partition_info;
  }

  size_t getMetaPartitionFileSize() {
    // consider:
    //  - partition-meta-info at the beginning of the file
    //  - partition-edge-counts at the beginning of the file
    //  - up to $cols**2 many partitions
    return sizeof(meta_partition_file_info_t) +
           (PARTITIONS_PER_SPARSE_FILE * sizeof(partition_edge_compact_t)) +
           (PARTITIONS_PER_SPARSE_FILE * MAX_EDGES_PER_TILE *
            sizeof(edge_compact_t));
  }

  size_t getMaxMetaPartitionEdgeCount() {
    return (MAX_EDGES_PER_TILE_IN_MEMORY * PARTITIONS_PER_SPARSE_FILE);
  }

  size_t getSizeTileBlock(const vertex_edge_tiles_block_sizes_t& sizes) {
    return sizeof(vertex_edge_tiles_block_t) +
           sizes.size_active_vertex_src_block +
           sizes.size_active_vertex_tgt_block + sizes.size_src_degree_block +
           sizes.size_tgt_degree_block + sizes.size_source_vertex_block +
           sizes.size_extension_fields_vertex_block;
  }

  void fillTileBlockHeader(vertex_edge_tiles_block_t* tile_block,
                           uint64_t block_id, const tile_stats_t& tile_stats,
                           const vertex_edge_tiles_block_sizes_t& sizes,
                           uint32_t num_tile_partition,
                           uint32_t tile_partition_id) {
    tile_block->shutdown = false;
    tile_block->magic_identifier = MAGIC_IDENTIFIER;
    tile_block->block_id = block_id;
    tile_block->count_active_vertex_src_block =
        sizes.count_active_vertex_src_block;
    tile_block->count_active_vertex_tgt_block =
        sizes.count_active_vertex_tgt_block;
    tile_block->count_src_vertex_block = tile_stats.count_vertex_src;
    tile_block->count_tgt_vertex_block = tile_stats.count_vertex_tgt;

    tile_block->num_tile_partition = num_tile_partition;
    tile_block->tile_partition_id = tile_partition_id;

    tile_block->offset_active_vertices_src = sizeof(vertex_edge_tiles_block_t);
    tile_block->offset_active_vertices_tgt =
        tile_block->offset_active_vertices_src +
        sizes.size_active_vertex_src_block;

    tile_block->offset_src_degrees = tile_block->offset_active_vertices_tgt +
                                     sizes.size_active_vertex_tgt_block;

    tile_block->offset_tgt_degrees =
        tile_block->offset_src_degrees + sizes.size_src_degree_block;

    tile_block->offset_source_vertex_block =
        tile_block->offset_tgt_degrees + sizes.size_tgt_degree_block;

    tile_block->offset_extensions =
        tile_block->offset_source_vertex_block + sizes.size_source_vertex_block;
  }
}
}
