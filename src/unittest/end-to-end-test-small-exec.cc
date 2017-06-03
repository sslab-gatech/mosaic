#include "test.h"
#include <core/util.h>
#include <core/datatypes.h>

namespace core = scalable_graphs::core;
namespace util = scalable_graphs::util;

int main(int argc, char** argv) {
  if (argc != 2) {
    printf("Usage: test_end_to_end_small [path_to_tiles]\n");
    return 1;
  }

  std::string path_to_tiles(argv[1]);
  int expected_num_vertices = 4;
  int expected_num_src = 3;
  int expected_num_tgt = 4;
  int expected_num_edges = 6;
  int expected_num_tiles = 1;

  scenario_stats_t global_stats;
  std::string global_stat_file_name =
      core::getGlobalStatFileName(path_to_tiles);
  util::readDataFromFile(global_stat_file_name, sizeof(scenario_stats_t),
                         &global_stats);

  sg_test(global_stats.num_tiles == expected_num_tiles, "num_tiles");
  sg_test(global_stats.num_vertices == expected_num_vertices, "num_vertices");

  // test the degrees now
  vertex_degree_t* degrees = new vertex_degree_t[expected_num_vertices];
  size_t degree_filesize = sizeof(vertex_degree_t) * expected_num_vertices;
  std::string degree_filename = core::getVertexDegreeFileName(path_to_tiles);
  util::readDataFromFile(degree_filename, degree_filesize, degrees);

  std::string id_translation_file_name =
      core::getGlobalToOrigIDFileName(path_to_tiles);
  std::unordered_map<vertex_id_t, vertex_id_t> global_to_orig;
  core::readMapFromFile<vertex_id_t, vertex_id_t>(id_translation_file_name,
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
  std::unordered_map<uint64_t, tile_stats_t> tile_stats;
  for (int i = 0; i < expected_num_tiles; ++i) {
    std::string tile_stats_file_name =
        core::getEdgeTileStatFileName(path_to_tiles, i);

    tile_stats_t tile_stat;
    util::readDataFromFile(tile_stats_file_name, sizeof(tile_stats_t),
                           &tile_stat);

    tile_stats[i] = tile_stat;
  }
  sg_test(tile_stats[0].block_id == 0, "tile_stats");
  sg_test(tile_stats[0].count_vertex_tgt == expected_num_tgt, "tile_stats");
  sg_test(tile_stats[0].count_vertex_src == expected_num_src, "tile_stats");
  sg_test(tile_stats[0].count_edges == expected_num_edges, "tile_stats");

  // assert edge-index
  // first src
  std::string edge_block_index_src_name =
      core::getEdgeTileSrcIndexFileName(path_to_tiles, 0);
  size_t size_edge_index_src_vertex_block =
      sizeof(vertex_id_t) * expected_num_src;
  size_t size_edge_block_index_src =
      sizeof(edge_block_index_t) + size_edge_index_src_vertex_block;
  edge_block_index_t* edge_block_index_src =
      (edge_block_index_t*)malloc(size_edge_block_index_src);

  util::readDataFromFile(edge_block_index_src_name, size_edge_block_index_src,
                         edge_block_index_src);

  sg_test(edge_block_index_src->vertices[0] == 0, "edge_index_src");
  sg_test(edge_block_index_src->vertices[1] == 1, "edge_index_src");
  sg_test(edge_block_index_src->vertices[2] == 3, "edge_index_src");

  // then tgt
  std::string edge_block_index_tgt_name =
      core::getEdgeTileTgtIndexFileName(path_to_tiles, 0);
  size_t size_edge_index_tgt_vertex_block =
      sizeof(vertex_id_t) * expected_num_tgt;
  size_t size_edge_block_index_tgt =
      sizeof(edge_block_index_t) + size_edge_index_tgt_vertex_block;
  edge_block_index_t* edge_block_index_tgt =
      (edge_block_index_t*)malloc(size_edge_block_index_tgt);

  util::readDataFromFile(edge_block_index_tgt_name, size_edge_block_index_tgt,
                         edge_block_index_tgt);

  sg_test(edge_block_index_tgt->vertices[0] == 1, "edge_index_tgt");
  sg_test(edge_block_index_tgt->vertices[1] == 2, "edge_index_tgt");
  sg_test(edge_block_index_tgt->vertices[2] == 3, "edge_index_tgt");
  sg_test(edge_block_index_tgt->vertices[3] == 0, "edge_index_tgt");

  // assert tile
  size_t size_edge_block =
      sizeof(edge_block_t) + sizeof(local_edge_t) * expected_num_edges;
  edge_block_t* edge_block = (edge_block_t*)malloc(size_edge_block);
  std::string tile_file_name = core::getEdgeTileFileName(path_to_tiles, 0);
  util::readDataFromFile(tile_file_name, size_edge_block, edge_block);

  sg_test(edge_block->block_id == 0, "edge_block");
  sg_test(edge_block->count_vertex_src == expected_num_src, "edge_block");
  sg_test(edge_block->count_vertex_tgt == expected_num_tgt, "edge_block");
  sg_test(edge_block->count_edges == expected_num_edges, "edge_block");
  // edges
  sg_test(edge_block->edges[0].src == 0, "edge_block_edges");
  sg_test(edge_block->edges[0].tgt == 0, "edge_block_edges");

  sg_test(edge_block->edges[1].src == 2, "edge_block_edges");
  sg_test(edge_block->edges[1].tgt == 0, "edge_block_edges");

  sg_test(edge_block->edges[2].src == 0, "edge_block_edges");
  sg_test(edge_block->edges[2].tgt == 1, "edge_block_edges");

  sg_test(edge_block->edges[3].src == 1, "edge_block_edges");
  sg_test(edge_block->edges[3].tgt == 1, "edge_block_edges");

  sg_test(edge_block->edges[4].src == 1, "edge_block_edges");
  sg_test(edge_block->edges[4].tgt == 2, "edge_block_edges");

  sg_test(edge_block->edges[5].src == 2, "edge_block_edges");
  sg_test(edge_block->edges[5].tgt == 3, "edge_block_edges");

  return 0;
}
