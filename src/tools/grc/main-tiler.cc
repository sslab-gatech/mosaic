#define __STDC_FORMAT_MACROS
#include "tile-manager.h"

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

#include "partition-manager.h"

namespace gl = scalable_graphs::graph_load;
namespace util = scalable_graphs::util;
namespace core = scalable_graphs::core;

struct command_line_args_t : public command_line_args_grc_t {
  std::vector<std::string> paths_to_meta;
  std::vector<std::string> paths_to_tile;
  grc_tile_traversals_t traversal;
  bool output_weighted;
  bool use_rle;
};

static int parseOption(int argc, char* argv[], command_line_args_t& cmd_args) {
  static struct option options[] = {
      {"count-vertices", required_argument, 0, 'v'},
      {"graphname", required_argument, 0, 'g'},
      {"paths-partition", required_argument, 0, 'p'},
      {"path-globals", required_argument, 0, 'l'},
      {"paths-meta", required_argument, 0, 'm'},
      {"paths-tile", required_argument, 0, 't'},
      {"nthreads", required_argument, 0, 'n'},
      {"npartition-managers", required_argument, 0, 'a'},
      {"input-weighted", required_argument, 0, 'i'},
      {"output-weighted", required_argument, 0, 'o'},
      {"use-run-length-encoding", required_argument, 0, 'r'},
      {"traversal", required_argument, 0, 'e'},
      {0, 0, 0, 0},
  };
  int arg_cnt;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    int c, idx = 0;
    c = getopt_long(argc, argv, "g:p:l:m:t:n:a:i:o:r:v:e:", options, &idx);
    if (c == -1)
      break;

    switch (c) {
    case 'g':
      cmd_args.graphname = std::string(optarg);
      break;
    case 'p':
      cmd_args.paths_to_partition = util::splitDirPaths(std::string(optarg));
      break;
    case 'l':
      cmd_args.path_to_globals = util::prepareDirPath(std::string(optarg));
      break;
    case 'm':
      cmd_args.paths_to_meta = util::splitDirPaths(std::string(optarg));
      break;
    case 't':
      cmd_args.paths_to_tile = util::splitDirPaths(std::string(optarg));
      break;
    case 'n':
      cmd_args.nthreads = std::stoi(std::string(optarg));
      break;
    case 'a':
      cmd_args.count_partition_managers = std::stoi(std::string(optarg));
      break;
    case 'i':
      cmd_args.input_weighted =
          (std::stoi(std::string(optarg)) == 1) ? true : false;
      break;
    case 'o':
      cmd_args.output_weighted =
          (std::stoi(std::string(optarg)) == 1) ? true : false;
      break;
    case 'r':
      cmd_args.use_rle = (std::stoi(std::string(optarg)) == 1) ? true : false;
      break;
    case 'v':
      cmd_args.count_vertices = std::stoull(std::string(optarg));
      break;
    case 'e':
      if (std::string(optarg) == "hilbert") {
        cmd_args.traversal = grc_tile_traversals_t::Hilbert;
      } else if (std::string(optarg) == "column_first") {
        cmd_args.traversal = grc_tile_traversals_t::ColumnFirst;
      } else if (std::string(optarg) == "row_first") {
        cmd_args.traversal = grc_tile_traversals_t::RowFirst;
      } else {
        sg_log("Wrong traversal supplied: %s", optarg);
        util::die(1);
      }
      break;
    default:
      return -EINVAL;
    }
  }
  return arg_cnt;
}

template <typename TVertexIdType, typename TLocalEdgeType>
static void
innerRunTM(const gl::tile_manager_arguments_t& tile_manager_arguments,
           const command_line_args_t& cmd_args) {
  if (cmd_args.input_weighted) {
    gl::TileManager<TVertexIdType, TLocalEdgeType, edge_weighted_t> tl(
        tile_manager_arguments);
    tl.generateAndWriteTiles(cmd_args.nthreads);
  } else {
    gl::TileManager<TVertexIdType, TLocalEdgeType, edge_t> tl(
        tile_manager_arguments);
    tl.generateAndWriteTiles(cmd_args.nthreads);
  }
}

template <typename TVertexIdType>
static void
outerRunTM(const gl::tile_manager_arguments_t& tile_manager_arguments,
           const command_line_args_t& cmd_args) {
  if (cmd_args.output_weighted) {
    sg_log2("Generating weighted graph\n");
    innerRunTM<TVertexIdType, local_edge_weighted_t>(tile_manager_arguments,
                                                     cmd_args);
  } else {
    sg_log2("Generating non-weighted graph\n");
    innerRunTM<TVertexIdType, local_edge_t>(tile_manager_arguments, cmd_args);
  }
}

static void usage(FILE* out) {
  extern const char* __progname;

  fprintf(out, "Usage: %s\n", __progname);
  fprintf(out, "  --graphname               = graph name\n");
  fprintf(out, "  --count-vertices          = graph name\n");
  fprintf(out,
          "  --paths-partition         = paths to interim partition data\n");
  fprintf(out, "  --path-global             = path to global data\n");
  fprintf(out, "  --paths-meta              = output paths to metadata\n");
  fprintf(out, "  --paths-tile              = output paths to tiledata\n");
  fprintf(out, "  --nthreads                = number of concurrent threads\n");
  fprintf(out, "  --npartition-managers     = number of partition managers\n");
  fprintf(out,
          "  --input-weighted          = whether to read a weighted graph\n");
  fprintf(
      out,
      "  --output-weighted         = whether to generate a weighted graph\n");
  fprintf(
      out,
      "  --use-run-length-encoding = whether to generate tiles using rle\n");
  fprintf(out, "  --traversal = whether to use the hilbert ordering or a "
               "column_first or the row_first approach.\n");
}

int main(int argc, char** argv) {
  command_line_args_t cmd_args;

  // parse command line options
  if (parseOption(argc, argv, cmd_args) != 12) {
    usage(stderr);
    return 1;
  }

  bool is_index_32_bits = cmd_args.count_vertices < UINT32_MAX;

  config_tiler_t config;
  core::initGrcConfig(&config, cmd_args.count_vertices, cmd_args);

  config.output_weighted = cmd_args.output_weighted;
  config.paths_to_meta = cmd_args.paths_to_meta;
  config.paths_to_tile = cmd_args.paths_to_tile;
  config.use_rle = cmd_args.use_rle;
  config.traversal = cmd_args.traversal;
  config.partition_mode = PartitionMode::PM_FileBackedMode;

  gl::AbstractPartitionManager** partition_managers =
      new gl::AbstractPartitionManager*[config.count_partition_managers];

  for (int i = 0; i < config.count_partition_managers; ++i) {
    partition_manager_arguments_t arguments =
        core::getPartitionManagerArguments(i, config);

    gl::PartitionManager* partition_manager =
        new gl::PartitionManager(config, arguments);
    partition_manager->initRead();

    partition_managers[i] = partition_manager;
  }

  // tiling
  gl::tile_manager_arguments_t tile_manager_arguments;
  tile_manager_arguments.partition_managers = partition_managers;
  tile_manager_arguments.config = config;

  // initialize rand with deterministic seed to consistently generate the same
  // weighted edges
  srand(0);

  if (is_index_32_bits) {
    outerRunTM<uint32_t>(tile_manager_arguments, cmd_args);
  } else {
    outerRunTM<uint64_t>(tile_manager_arguments, cmd_args);
  }

  for (int i = 0; i < config.count_partition_managers; ++i) {
    sg_log("Delete partition-manager %d\n", i);
    delete partition_managers[i];
  }

  return 0;
}
