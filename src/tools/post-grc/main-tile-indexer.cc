#define __STDC_FORMAT_MACROS

#include <inttypes.h>
#include <cmath>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <getopt.h>
#include <errno.h>

#include <core/datatypes.h>
#include <core/util.h>

#include "index-reader.h"

namespace util = scalable_graphs::util;
namespace post_grc = scalable_graphs::post_grc;
namespace core = scalable_graphs::core;

struct command_line_args_t {
  std::vector<std::string> paths_to_meta;
  std::string path_to_global;
  int nthreads;
};

static int parseOption(int argc, char* argv[], command_line_args_t& cmd_args) {
  static struct option options[] = {
      {"path-globals", required_argument, 0, 'g'},
      {"paths-meta", required_argument, 0, 'm'},
      {"nthreads", required_argument, 0, 'n'},
      {0, 0, 0, 0},
  };
  int arg_cnt;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    int c, idx = 0;
    c = getopt_long(argc, argv, "g:m:n:", options, &idx);
    if (c == -1)
      break;

    switch (c) {
    case 'g':
      cmd_args.path_to_global = util::prepareDirPath(std::string(optarg));
      break;
    case 'm':
      cmd_args.paths_to_meta = util::splitDirPaths(std::string(optarg));
      break;
    case 'n':
      cmd_args.nthreads = std::stoi(std::string(optarg));
      break;
    default:
      return -EINVAL;
    }
  }
  return arg_cnt;
}

static void usage(FILE* out) {
  extern const char* __progname;

  fprintf(out, "Usage: %s\n", __progname);
  fprintf(out, "  --path-global             = path to global data\n");
  fprintf(out, "  --paths-meta              = output paths to metadata\n");
  fprintf(out, "  --nthreads                = number of concurrent threads\n");
}

int main(int argc, char** argv) {
  command_line_args_t cmd_args;

  // parse command line options
  if (parseOption(argc, argv, cmd_args) != 3) {
    usage(stderr);
    return 1;
  }

  // load graph info
  scenario_stats_t global_stats;
  std::string global_stat_file_name =
      core::getGlobalStatFileName(cmd_args.path_to_global);
  util::readDataFromFile(global_stat_file_name, sizeof(scenario_stats_t),
                         &global_stats);

  sg_log("Tile-Indexer with %lu tiles and %lu vertices\n",
         global_stats.count_tiles, global_stats.count_vertices);

  std::vector<uint32_t>** vertex_to_tiles_index_vector =
      new std::vector<uint32_t>*[global_stats.count_vertices];

  uint32_t* count_tiles_per_vertex = new uint32_t[global_stats.count_vertices];
  size_t* tiles_per_vertex_offset = new size_t[global_stats.count_vertices];

  for (size_t i = 0; i < global_stats.count_vertices; ++i) {
    vertex_to_tiles_index_vector[i] = new std::vector<uint32_t>;
    count_tiles_per_vertex[i] = 0;
  }

  sg_log2("Allocated!\n");

  std::vector<post_grc::IndexReader*> index_readers;

  size_t count_paths = cmd_args.paths_to_meta.size();
#if 1
  for (int i = 0; i < count_paths; ++i) {
    thread_index_t ti;
    ti.count = count_paths;
    ti.id = i;
    post_grc::IndexReader* ir = new post_grc::IndexReader(
        cmd_args.path_to_global, cmd_args.paths_to_meta,
        global_stats.count_vertices, global_stats.count_tiles,
        global_stats.is_index_32_bits, ti);

    index_readers.push_back(ir);
    ir->start();
  }

  sg_log2("All threads started!\n");

  for (post_grc::IndexReader* ir : index_readers) {
    ir->join();
    for (size_t i = 0; i < global_stats.count_vertices; ++i) {
      for (uint32_t tile_id : *(ir->vertex_to_tiles_index_)[i]) {
        vertex_to_tiles_index_vector[i]->push_back(tile_id);
      }
      count_tiles_per_vertex[i] += ir->vertex_to_tiles_index_[i]->size();
    }
    delete ir;
  }
#else

  size_t count_outgoing = 0;
  for (int i = 0; i < count_paths; ++i) {
    thread_index_t ti;
    ti.count = count_paths;
    ti.id = i;
    post_grc::IndexReader* ir = new post_grc::IndexReader(
        cmd_args.path_to_global, cmd_args.paths_to_meta,
        global_stats.count_vertices, global_stats.count_tiles,
        global_stats.is_index_32_bits, ti);

    ir->start();
    index_readers.push_back(ir);
    // for (size_t i = 0; i < global_stats.count_vertices; ++i) {
    //   for (uint32_t tile_id : *(ir->vertex_to_tiles_index_)[i]) {
    //     vertex_to_tiles_index_vector[i]->push_back(tile_id);
    //   }
    //   count_tiles_per_vertex[i] += ir->vertex_to_tiles_index_[i]->size();
    // }
  }

  for (post_grc::IndexReader* ir : index_readers) {
    ir->join();
    count_outgoing += ir->count;
    delete ir;
  }

  sg_log2("All threads started!\n");
  sg_log("Count outgoing: %lu\n", count_outgoing);
  return 1;
#endif

  sg_log2("All threads joined!\n");

  size_t global_count_tiles_per_vertices = 0;
  for (size_t i = 0; i < global_stats.count_vertices; ++i) {
    global_count_tiles_per_vertices += count_tiles_per_vertex[i];
  }

  tiles_per_vertex_offset[0] = 0;

  for (size_t i = 1; i < global_stats.count_vertices; ++i) {
    tiles_per_vertex_offset[i] =
        tiles_per_vertex_offset[i - 1] + count_tiles_per_vertex[i];
  }

  uint32_t* vertex_to_tiles_index =
      new uint32_t[global_count_tiles_per_vertices];

  sg_dbg("global count tiles per vertices : %lu\n",
         global_count_tiles_per_vertices);

  for (size_t i = 0; i < global_stats.count_vertices; ++i) {
    size_t offset = tiles_per_vertex_offset[i];
    size_t size = count_tiles_per_vertex[i] * sizeof(uint32_t);
    memcpy(&vertex_to_tiles_index[offset],
           vertex_to_tiles_index_vector[i]->data(), size);
  }

  size_t size_vertex_to_tile_count =
      global_stats.count_vertices * sizeof(uint32_t);
  size_t size_vertex_to_tiles_index =
      global_count_tiles_per_vertices * sizeof(uint32_t);

  sg_log("Size tile count: %lu \nSize tile index: %lu\n",
         size_vertex_to_tile_count, size_vertex_to_tiles_index);

  std::string vertex_to_tile_count_filename =
      core::getVertexToTileCountFileName(cmd_args.path_to_global);
  util::writeDataToFile(vertex_to_tile_count_filename, count_tiles_per_vertex,
                        size_vertex_to_tile_count);

  std::string vertex_to_tiles_index_filename =
      core::getVertexToTileIndexFileName(cmd_args.path_to_global);
  util::writeDataToFile(vertex_to_tiles_index_filename, vertex_to_tiles_index,
                        size_vertex_to_tiles_index);

  return 0;
}
