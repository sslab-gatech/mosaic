#define __STDC_FORMAT_MACROS

#include <getopt.h>

#include <core/datatypes.h>
#include <core/util.h>

#include "tile-manager.h"
#include "iedges-reader.h"
#include "delim-edges-reader.h"
#include "binary-edge-reader.h"
#include "rmat-edge-generator.h"
#include "partition-manager.h"

namespace gl = scalable_graphs::graph_load;
namespace util = scalable_graphs::util;
namespace core = scalable_graphs::core;

struct command_line_args_t : public command_line_args_grc_t {
  std::vector<std::string> paths_to_meta;
  std::vector<std::string> paths_to_tile;
  grc_tile_traversals_t traversal;
  std::string source;
  std::string graph_generator;
  std::string delimiter;
  uint64_t rmat_count_edges;
  bool output_weighted;
  bool use_rle;
  bool use_original_ids;
};

static int parseOption(int argc,
                       char* argv[],
                       command_line_args_t& cmd_args) {
  static struct option options[] = {
      {"source",                  required_argument, 0, 'a'},
      {"count-vertices",          required_argument, 0, 'b'},
      {"generator",               required_argument, 0, 'c'},
      {"graphname",               required_argument, 0, 'd'},
      {"path-globals",            required_argument, 0, 'e'},
      {"paths-meta",              required_argument, 0, 'f'},
      {"paths-tile",              required_argument, 0, 'g'},
      {"nthreads",                required_argument, 0, 'h'},
      {"npartition-managers",     required_argument, 0, 'i'},
      {"input-weighted",          required_argument, 0, 'j'},
      {"output-weighted",         required_argument, 0, 'k'},
      {"rmat-count-edges",        required_argument, 0, 'l'},
      {"use-run-length-encoding", required_argument, 0, 'm'},
      {"use-original-ids",        required_argument, 0, 'n'},
      {"traversal",               required_argument, 0, 'o'},
      {"delimiter",               required_argument, 0, 'p'},
      {0, 0,                                         0, 0},
  };
  int arg_cnt;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    int c, idx = 0;
    c = getopt_long(argc, argv, "g:p:l:m:t:n:a:i:o:r:v:e:", options, &idx);
    if (c == -1) {
      break;
    }

    switch (c) {
      case 'a':
        cmd_args.source = std::string(optarg);
        break;
      case 'b':
        cmd_args.count_vertices = (uint64_t) std::stoi(std::string(optarg));
        break;
      case 'c':
        cmd_args.graph_generator = std::string(optarg);
        break;
      case 'd':
        cmd_args.graphname = std::string(optarg);
        break;
      case 'e':
        cmd_args.path_to_globals = util::prepareDirPath(std::string(optarg));
        break;
      case 'f':
        cmd_args.paths_to_meta = util::splitDirPaths(std::string(optarg));
        break;
      case 'g':
        cmd_args.paths_to_tile = util::splitDirPaths(std::string(optarg));
        break;
      case 'h':
        cmd_args.nthreads = (uint64_t) std::stoi(std::string(optarg));
        break;
      case 'i':
        cmd_args.count_partition_managers = (uint64_t) std::stoi(std::string(
            optarg));
        break;
      case 'j':
        cmd_args.input_weighted =
            (std::stoi(std::string(optarg)) == 1) ? true : false;
        break;
      case 'k':
        cmd_args.output_weighted =
            (std::stoi(std::string(optarg)) == 1) ? true : false;
        break;
      case 'l':
        cmd_args.rmat_count_edges = std::stoull(std::string(optarg));
        break;
      case 'm':
        cmd_args.use_rle = (std::stoi(std::string(optarg)) == 1) ? true : false;
        break;
      case 'n':
        cmd_args.use_original_ids =
            (std::stoi(std::string(optarg)) == 1) ? true : false;
        break;
      case 'o':
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
      case 'p':
        cmd_args.delimiter = std::string(optarg);
        break;
      default:
        return -EINVAL;
    }
  }
  return arg_cnt;
}

template<typename TVertexIdType, typename TLocalEdgeType>
static void
innerRunTM(const gl::tile_manager_arguments_t& tile_manager_arguments,
           const command_line_args_t& cmd_args) {
  if (cmd_args.input_weighted) {
    gl::TileManager <TVertexIdType, TLocalEdgeType, edge_weighted_t> tl(
        tile_manager_arguments);
    tl.generateAndWriteTiles(cmd_args.nthreads);
  } else {
    gl::TileManager <TVertexIdType, TLocalEdgeType, edge_t> tl(
        tile_manager_arguments);
    tl.generateAndWriteTiles(cmd_args.nthreads);
  }
}

template<typename TVertexIdType>
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

static void
initPartitionManagers(gl::AbstractPartitionManager** partition_managers,
                      const config_partitioner_t& config) {
  for (int i = 0; i < config.count_partition_managers; ++i) {
    partition_manager_arguments_t arguments =
        core::getPartitionManagerArguments(i, config);

    gl::PartitionManager* partition_manager =
        new gl::PartitionManager(config, arguments);
    partition_manager->initWrite();
    partition_manager->start();

    partition_managers[i] = partition_manager;
  }
}

static void
shutdownPartitionManagers(gl::AbstractPartitionManager** partition_managers,
                          const config_partitioner_t& config) {
  // Shutdown-procedure:
  // - Shutdown PartitionManagers (and PartitionStores/**/)
  // - Join PartitionManagers
  // - Destroy PartitionManagers

  // shutdown all partition-managers by sending specially crafted
  // "shutdown"-message
  for (int i = 0; i < config.count_partition_managers; ++i) {
    gl::PartitionManager* partition_manager = (gl::PartitionManager*) partition_managers[i];
    // send shutdown-message to partition-manager
    ring_buffer_req_t request;
    ring_buffer_put_req_init(&request, BLOCKING, sizeof(partition_edge_list_t));
    ring_buffer_put(partition_manager->edges_rb_, &request);
    sg_rb_check(&request);

    partition_edge_list_t* edge_list_in_rb = (partition_edge_list_t*) request.data;
    edge_list_in_rb->shutdown_indicator = true;

    // set element ready, done!
    ring_buffer_elm_set_ready(partition_manager->edges_rb_, request.data);

    // then wait for shutdown and delete
    partition_manager->join();
    // trigger write of partitions-information
    partition_manager->cleanupWrite();
  }
}

template<typename TVertexIdType>
static void runEdgesReader(const std::string& generator,
                           const config_partitioner_t& config,
                           gl::PartitionManager** partition_managers) {
  if (config.input_weighted) {
    if (generator == "delim_edges") {
      gl::DelimEdgesReader <edge_weighted_t, TVertexIdType> reader(
          config, partition_managers);
      reader.readEdges(config.nthreads);
    } else if (generator == "binary") {
      gl::BinaryEdgeReader <file_edge_t, TVertexIdType> reader(
          config, partition_managers);
      reader.readEdges(config.nthreads);
    } else if (generator == "rmat") {
      gl::RMATEdgeGenerator <edge_weighted_t, TVertexIdType> reader(
          config, partition_managers);
      reader.readEdges(config.nthreads);
    } else {
      sg_err("Generator %s not supported, aborting!\n", generator.c_str());
      util::die(1);
    }
  } else {
    if (generator == "delim_edges") {
      gl::DelimEdgesReader <edge_t, TVertexIdType> reader(config,
                                                          partition_managers);
      reader.readEdges(config.nthreads);
    } else if (generator == "binary") {
      gl::BinaryEdgeReader <file_edge_t, TVertexIdType> reader(config,
                                                               partition_managers);
      reader.readEdges(config.nthreads);
    } else if (generator == "rmat") {
      gl::RMATEdgeGenerator <edge_t, TVertexIdType> reader(config,
                                                           partition_managers);
      reader.readEdges(config.nthreads);
    } else {
      sg_err("Generator %s not supported, aborting!\n", generator.c_str());
      util::die(1);
    }
  }
}

static void usage(FILE* out) {
  extern const char* __progname;

  fprintf(out, "Usage: %s\n", __progname);
  fprintf(out, "  --graphname               = graph name\n");
  fprintf(out, "  --count-vertices          = graph name\n");
  fprintf(out,
          "  --paths-partition         = paths to interim partition data\n");
  fprintf(out, "  --generator           = graph generator ('rmat' or "
      "'delim_edges' or 'binary')\n");
  fprintf(out, "  --source              = graph raw data (for "
      "'delim_edges' or 'binary')\n");
  fprintf(out, "  --path-global             = path to global data\n");
  fprintf(out, "  --paths-partition     = paths to interim partition data\n");
  fprintf(out, "  --paths-meta              = output paths to metadata\n");
  fprintf(out, "  --paths-tile              = output paths to tiledata\n");
  fprintf(out, "  --nthreads                = number of concurrent threads\n");
  fprintf(out, "  --npartition-managers     = number of partition managers\n");
  fprintf(out,
          "  --input-weighted          = whether to read a weighted graph\n");
  fprintf(out,
          "  --output-weighted         = whether to generate a weighted graph\n");
  fprintf(out,
          "  --use-run-length-encoding = whether to generate tiles using rle\n");
  fprintf(out, "  --rmat-count-edges    = count edges for rmat-generator\n");
  fprintf(out, "  --use-original-ids    = use the original id's of the file\n");
  fprintf(out, "  --traversal = whether to use the hilbert ordering or a "
      "column_first or the row_first approach.\n");
}

int main(int argc,
         char** argv) {
  command_line_args_t cmd_args;

  // Parse command line options, return if not correct count.
  if (parseOption(argc, argv, cmd_args) != 16) {
    usage(stderr);
    return 1;
  }

  // load graph info
  scenario_settings_t current_settings;
  current_settings.count_vertices = cmd_args.count_vertices;
  if (cmd_args.graph_generator == "delim_edges") {
    if (cmd_args.delimiter == "tab") {
      current_settings.delimiter = '\t';
    } else if (cmd_args.delimiter == "space") {
      current_settings.delimiter = ' ';
    } else if (cmd_args.delimiter == "comma") {
      current_settings.delimiter = ',';
    } else if (cmd_args.delimiter == "semicolon") {
      current_settings.delimiter = ';';
    } else {
      sg_err("Wrong delimiter given: %s\n", cmd_args.delimiter.c_str());
      util::die(1);
    }
  } else if (cmd_args.graph_generator == "rmat") {
    current_settings.rmat_count_edges = cmd_args.rmat_count_edges;
    sg_log("Generating RMAT-graph with %lu vertices and %lu edges\n",
           current_settings.count_vertices, current_settings.rmat_count_edges);
  } else if (cmd_args.graph_generator == "binary") {
    sg_log("Generating graph from binary with %lu vertices\n",
           current_settings.count_vertices);
  } else {
    sg_err("Wrong generator passed: %s\n", cmd_args.graph_generator.c_str());
    util::die(1);
  }

  bool is_index_32_bits = current_settings.count_vertices < UINT32_MAX;

  config_tiler_t config_tiler;
  config_partitioner_t config_partitioner;

  core::initGrcConfig(&config_tiler, cmd_args.count_vertices, cmd_args);
  core::initGrcConfig(&config_partitioner, cmd_args.count_vertices, cmd_args);

  config_tiler.output_weighted = cmd_args.output_weighted;
  config_tiler.paths_to_meta = cmd_args.paths_to_meta;
  config_tiler.paths_to_tile = cmd_args.paths_to_tile;
  config_tiler.use_rle = cmd_args.use_rle;
  config_tiler.traversal = cmd_args.traversal;
  config_tiler.partition_mode = PartitionMode::PM_InMemoryMode;

  config_partitioner.settings = current_settings;
  config_partitioner.source = cmd_args.source;
  config_partitioner.rmat_count_edges = cmd_args.rmat_count_edges;
  config_partitioner.use_original_ids = cmd_args.use_original_ids;
  config_partitioner.partition_mode = PartitionMode::PM_InMemoryMode;

  // Overall structure:
  // 1) Start PartitionManagers
  // 2) Parse into partitions
  // 3) Wait for PartitionManagers to signal all clear
  // 3) Use partitions to generate tiles
  // 4) Shutdown everything

  gl::AbstractPartitionManager** partition_managers =
      new gl::AbstractPartitionManager* [config_tiler.count_partition_managers];

  initPartitionManagers(partition_managers, config_partitioner);

  uint64_t start_time = util::get_time_nsec();

  sg_log2("Read edges into partitions.\n");
  // Parse into partitions.
  if (is_index_32_bits) {
    runEdgesReader<uint32_t>(cmd_args.graph_generator, config_partitioner,
                             (gl::PartitionManager**) partition_managers);
  } else {
    runEdgesReader<uint64_t>(cmd_args.graph_generator, config_partitioner,
                             (gl::PartitionManager**) partition_managers);
  }

  uint64_t end_time = util::get_time_nsec();
  double diff = (double) (end_time - start_time) / 1000000000L;

  sg_log("Partitioning time: %f\n", diff);

  // Wait for all edges to be partitioned.
  shutdownPartitionManagers(partition_managers, config_partitioner);
  sg_log2("All edges partitioned.\n");

  // Do tiling with pre-partitioned edges.
  gl::tile_manager_arguments_t tile_manager_arguments;
  tile_manager_arguments.partition_managers = partition_managers;
  tile_manager_arguments.config = config_tiler;

  // initialize rand with deterministic seed to consistently generate the same
  // weighted edges
  srand(0);

  start_time = util::get_time_nsec();
  if (is_index_32_bits) {
    outerRunTM<uint32_t>(tile_manager_arguments, cmd_args);
  } else {
    outerRunTM<uint64_t>(tile_manager_arguments, cmd_args);
  }
  end_time = util::get_time_nsec();
  diff = (double) (end_time - start_time) / 1000000000L;

  sg_log("Tiling time: %f\n", diff);

  sg_log2("All edges are tiled.\n");
  // Tiling is done, delete everything.
  for (int i = 0; i < config_tiler.count_partition_managers; ++i) {
    sg_log("Delete partition-manager %d\n", i);
    delete partition_managers[i];
  }


  return 0;
}
