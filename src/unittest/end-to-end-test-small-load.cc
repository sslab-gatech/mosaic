#include "test.h"
#include <core/util.h>
#include <core/datatypes.h>

namespace core = scalable_graphs::core;
namespace util = scalable_graphs::util;

int main(int argc, char** argv) {
  if (argc != 3) {
    printf("Usage: test_end_to_end_small [path_to_tiles] [path_to_meta]\n");
    return 1;
  }

  std::string path_to_tiles_string(argv[1]);
  std::string path_to_meta_string(argv[2]);

  std::vector<std::string> paths_to_tiles = {path_to_tiles_string};
  std::vector<std::string> paths_to_meta = {path_to_meta_string};

  config_t config;
  config.paths_to_tile = paths_to_tiles;
  config.paths_to_meta = paths_to_meta;

  int expected_count_vertices = 4;
  int expected_count_src = 3;
  int expected_count_tgt = 4;
  int expected_count_edges = 6;
  int expected_count_tiles = 1;

  scenario_stats_t global_stats;
  std::string global_stat_file_name = core::getGlobalStatFileName(config);
  util::readDataFromFile(global_stat_file_name, sizeof(scenario_stats_t),
                         &global_stats);

  sg_test(global_stats.count_tiles == expected_count_tiles, "count_tiles");
  sg_test(global_stats.count_vertices == expected_count_vertices,
          "count_vertices");

  // test the degrees now
  vertex_degree_t* degrees = new vertex_degree_t[expected_count_vertices];
  size_t degree_filesize = sizeof(vertex_degree_t) * expected_count_vertices;
  std::string degree_filename = core::getVertexDegreeFileName(config);
  util::readDataFromFile(degree_filename, degree_filesize, degrees);

  std::string id_translation_file_name =
      core::getGlobalToOrigIDFileName(config);
  std::unordered_map<uint32_t, uint64_t> global_to_orig;
  core::readMapFromFile<uint32_t, uint64_t>(id_translation_file_name,
                                            global_to_orig);

  sg_test(global_to_orig[0] == 1, "orig_to_global");
  sg_test(global_to_orig[1] == 2, "orig_to_global");
  sg_test(global_to_orig[2] == 4, "orig_to_global");
  sg_test(global_to_orig[3] == 3, "orig_to_global");

  // assert degrees:
  sg_test(degrees[0].in_degree == 1, "in_degree");
  sg_test(degrees[1].in_degree == 2, "in_degree");
  sg_test(degrees[2].in_degree == 2, "in_degree");
  sg_test(degrees[3].in_degree == 1, "in_degree");

  sg_test(degrees[0].out_degree == 2, "out_degree");
  sg_test(degrees[1].out_degree == 2, "out_degree");
  sg_test(degrees[2].out_degree == 0, "out_degree");
  sg_test(degrees[3].out_degree == 2, "out_degree");

  // assert tile-stats
  tile_stats_t* tile_stats = new tile_stats_t[expected_count_tiles];
  std::string global_tile_stats_file_name =
      core::getGlobalTileStatsFileName(config, 0);
  util::readDataFromFile(global_tile_stats_file_name,
                         sizeof(tile_stats_t) * expected_count_tiles,
                         tile_stats);

  sg_test(tile_stats[0].block_id == 0, "tile_stats");
  sg_test(tile_stats[0].count_vertex_tgt == expected_count_tgt, "tile_stats");
  sg_test(tile_stats[0].count_vertex_src == expected_count_src, "tile_stats");
  sg_test(tile_stats[0].count_edges == expected_count_edges, "tile_stats");

  // assert edge-index
  // first src
  std::string edge_block_index_name = core::getEdgeTileIndexFileName(config, 0);
  size_t size_edge_index_src_vertex_block =
      sizeof(vertex_id_t) * expected_count_src;
  size_t size_edge_index_tgt_vertex_block =
      sizeof(vertex_id_t) * expected_count_tgt;
  size_t size_edge_block_index = sizeof(edge_block_index_t) +
                                 size_edge_index_src_vertex_block +
                                 size_edge_index_tgt_vertex_block;

  edge_block_index_t* edge_block_index =
      (edge_block_index_t*)malloc(size_edge_block_index);

  util::readDataFromFile(edge_block_index_name, size_edge_block_index,
                         edge_block_index);

  uint32_t* vertex_index_src = get_array(uint32_t*, edge_block_index,
                                         edge_block_index->offset_src_index);
  uint32_t* vertex_index_tgt = get_array(uint32_t*, edge_block_index,
                                         edge_block_index->offset_tgt_index);

  // test src-index
  sg_test(vertex_index_src[0] == 0, "edge_index_src");
  sg_test(vertex_index_src[1] == 1, "edge_index_src");
  sg_test(vertex_index_src[2] == 3, "edge_index_src");

  // then tgt-index
  sg_test(vertex_index_tgt[0] == 1, "edge_index_tgt");
  sg_test(vertex_index_tgt[1] == 2, "edge_index_tgt");
  sg_test(vertex_index_tgt[2] == 3, "edge_index_tgt");
  sg_test(vertex_index_tgt[3] == 0, "edge_index_tgt");

  // assert tile
  size_t size_edge_block =
      sizeof(edge_block_t) + sizeof(local_edge_t) * expected_count_edges;
  edge_block_t* edge_block = (edge_block_t*)malloc(size_edge_block);
  std::string tile_file_name = core::getEdgeTileFileName(config, 0);
  util::readDataFromFile(tile_file_name, size_edge_block, edge_block);

  sg_test(edge_block->block_id == 0, "edge_block");
  // edges
  local_vertex_id_t* edge_src =
      get_array(local_vertex_id_t*, edge_block, edge_block->offset_src);
  local_vertex_id_t* edge_tgt =
      get_array(local_vertex_id_t*, edge_block, edge_block->offset_tgt);

  sg_test(edge_src[0] == 0, "edge_block_edges");
  sg_test(edge_tgt[0] == 0, "edge_block_edges");

  sg_test(edge_src[1] == 2, "edge_block_edges");
  sg_test(edge_tgt[1] == 0, "edge_block_edges");

  sg_test(edge_src[2] == 0, "edge_block_edges");
  sg_test(edge_tgt[2] == 1, "edge_block_edges");

  sg_test(edge_src[3] == 1, "edge_block_edges");
  sg_test(edge_tgt[3] == 1, "edge_block_edges");

  sg_test(edge_src[4] == 1, "edge_block_edges");
  sg_test(edge_tgt[4] == 2, "edge_block_edges");

  sg_test(edge_src[5] == 2, "edge_block_edges");
  sg_test(edge_tgt[5] == 3, "edge_block_edges");

  return 0;
}
