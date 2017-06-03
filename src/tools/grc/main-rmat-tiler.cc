#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <getopt.h>
#include <errno.h>

#include <core/datatypes.h>
#include <core/util.h>

#include "in-memory-partition-manager.h"
#include "rmat-tile-manager.h"
#include "rmat-edge-receiver.h"

namespace gl = scalable_graphs::graph_load;
namespace util = scalable_graphs::util;
namespace core = scalable_graphs::core;

static int parseOption(int argc, char* argv[], config_rmat_tiler_t& config) {
  static struct option options[] = {
      {"count-vertices", required_argument, 0, 'v'},
      {"count-edges", required_argument, 0, 'e'},
      {"count-edge-generators", required_argument, 0, 'g'},
      {"count-partition-managers", required_argument, 0, 'p'},
      {"count-threads", required_argument, 0, 's'},
      {"base-port", required_argument, 0, 'o'},
      {"graphname", required_argument, 0, 'n'},
      {"path-globals", required_argument, 0, 'l'},
      {"paths-meta", required_argument, 0, 'm'},
      {"paths-tile", required_argument, 0, 't'},
      {"paths-partition", required_argument, 0, 'q'},
      {"use-run-length-encoding", required_argument, 0, 'c'},
      {"generator-phase", required_argument, 0, 'a'},
      {"run-on-mic", required_argument, 0, 'i'},
      {0, 0, 0, 0},
  };
  int arg_cnt;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    int c, idx = 0;
    c = getopt_long(argc, argv, "v:e:g:p:s:r:o:n:l:w:m:t:q:c:a:i:", options,
                    &idx);
    if (c == -1)
      break;

    switch (c) {
    case 'v':
      config.count_vertices = std::stoull(std::string(optarg));
      break;
    case 'e':
      config.count_edges = std::stoull(std::string(optarg));
      break;
    case 'g':
      config.count_edge_generators = std::stoull(std::string(optarg));
      break;
    case 'p':
      config.count_partition_managers = std::stoi(std::string(optarg));
      break;
    case 's':
      config.nthreads = std::stoi(std::string(optarg));
      break;
    case 'o':
      config.base_port = std::stoi(std::string(optarg));
      break;
    case 'n':
      config.graphname = std::string(optarg);
      break;
    case 'l':
      config.path_to_globals = util::prepareDirPath(std::string(optarg));
      break;
    case 'm':
      config.paths_to_meta = util::splitDirPaths(std::string(optarg));
      break;
    case 't':
      config.paths_to_tile = util::splitDirPaths(std::string(optarg));
      break;
    case 'q':
      config.paths_to_partition = util::splitDirPaths(std::string(optarg));
      break;
    case 'c':
      config.use_rle = (std::stoi(std::string(optarg)) == 1) ? true : false;
      break;
    case 'a': {
      std::string gen_phase = std::string(optarg);
      if (gen_phase == "generate_tiles") {
        config.generator_phase = RmatGeneratorPhase::RGP_GenerateTiles;

      } else if (gen_phase == "generate_vertex_degrees") {
        config.generator_phase = RmatGeneratorPhase::RGP_GenerateVertexDegrees;
      } else {
        sg_err("Wrong option passed for generator-phase: %s\n",
               gen_phase.c_str());
        util::die(1);
      }
      break;
    }
    case 'i':
      config.run_on_mic = (std::stoi(std::string(optarg)) == 1) ? true : false;
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
  fprintf(out, "  --graphname               = graph name\n");
  fprintf(out, "  --count-vertices          = count vertices\n");
  fprintf(out, "  --count-edges             = count edges\n");
  fprintf(out, "  --base-port               = the base port\n");
  fprintf(out, "  --path-global             = path to global data\n");
  fprintf(out, "  --paths-partition         = output paths to partition\n");
  fprintf(out, "  --paths-meta              = output paths to metadata\n");
  fprintf(out, "  --paths-tile              = output paths to tiledata\n");
  fprintf(out, "  --count-partition-managers= number of partition managers\n");
  fprintf(out, "  --count-threads           = number of concurrent threads\n");
  fprintf(out, "  --run-on-mic              = whether or not to use MIC\n");
  fprintf(out, "  --generator-phase         = generate_tiles or "
               "generate_vertex_degrees\n");
  fprintf(out, "  --count-edge-generators   = number of remote edge "
               "generators\n");
  fprintf(out, "  --use-run-length-encoding = whether to generate tiles using "
               "rle\n");
}

int main(int argc, char** argv) {
  config_rmat_tiler_t config;

  // parse command line options
  if (parseOption(argc, argv, config) != 14) {
    usage(stderr);
    return 1;
  }

  bool is_index_32_bits = (config.count_vertices - 1) <= UINT32_MAX;

  uint64_t count_partitions = pow(
      2,
      ceil(log2(ceil((double)config.count_vertices / MAX_VERTICES_PER_TILE))));

  size_t count_meta_partitions =
      ceil((double)count_partitions / PARTITION_COLS_PER_SPARSE_FILE);
  int adjusted_count_partition_managers = std::min(
      (size_t)pow(count_meta_partitions, 2), config.count_partition_managers);

  int count_partition_managers_per_row =
      sqrt(adjusted_count_partition_managers);
  // ceil the number of vertices per partition manager with the maximum
  // power-of-2-bounded number
  size_t count_vertices_per_partition_manager =
      count_partitions / count_partition_managers_per_row *
      MAX_VERTICES_PER_TILE;
  size_t count_meta_partitions_per_partition_manager = std::ceil(
      (double)count_vertices_per_partition_manager / VERTICES_PER_SPARSE_FILE);

  config.count_partition_managers = adjusted_count_partition_managers;
  config.count_rows_partition_managers = count_partition_managers_per_row;
  config.count_rows_partitions = count_partitions;
  config.count_rows_meta_partitions = count_meta_partitions;
  config.count_rows_partition_stores_per_partition_manager =
      count_meta_partitions_per_partition_manager;
  config.count_rows_vertices_per_partition_manager =
      count_vertices_per_partition_manager;

  gl::InMemoryPartitionManager** partition_managers =
      new gl::InMemoryPartitionManager*[config.count_partition_managers];

  for (int i = 0; i < config.count_partition_managers; ++i) {
    partition_manager_arguments_t arguments =
        core::getPartitionManagerArguments(i, config);

    gl::InMemoryPartitionManager* partition_manager =
        new gl::InMemoryPartitionManager(config, arguments);

    partition_managers[i] = partition_manager;
  }

  // switch between tiling and generating vertex degrees:
  if (config.generator_phase == RmatGeneratorPhase::RGP_GenerateTiles) {
    gl::rmat_tile_manager_arguments_t tile_manager_arguments;
    tile_manager_arguments.partition_managers = partition_managers;
    tile_manager_arguments.config = config;
    tile_manager_arguments.edge_receiver_barrier =
        new pthread_barrier_t*[config.count_edge_generators];

    gl::RMATEdgeReceiver** edge_receivers =
        new gl::RMATEdgeReceiver*[config.count_edge_generators];
    for (int i = 0; i < config.count_edge_generators; ++i) {
      gl::RMATEdgeReceiver* edge_receiver =
          new gl::RMATEdgeReceiver(config, partition_managers, i);
      edge_receivers[i] = edge_receiver;
      edge_receiver->start();
      // pass barrier to tile-manager for signaling the beginning of each round
      tile_manager_arguments.edge_receiver_barrier[i] =
          &edge_receiver->receiver_barrier_;
    }

    if (is_index_32_bits) {
      sg_log2("Generating graph with 32-bit-indices\n");
      gl::RMATTileManager<uint32_t> tl(tile_manager_arguments);
      tl.generateAndWriteTiles();
    } else {
      sg_log2("Generating graph with 64-bit-indices\n");
      gl::RMATTileManager<uint64_t> tl(tile_manager_arguments);
      tl.generateAndWriteTiles();
    }
  } else {
    // init partition-managers for this phase:
    for (int i = 0; i < config.count_partition_managers; ++i) {
      partition_managers[i]->start();
      partition_managers[i]->initCounting();
    }

    // generate vertex-degrees:
    vertex_degree_t* vertex_degrees = (vertex_degree_t*)calloc(
        sizeof(vertex_degree_t), config.count_vertices);
    gl::RMATEdgeReceiver** edge_receivers =
        new gl::RMATEdgeReceiver*[config.count_edge_generators];
    for (int i = 0; i < config.count_edge_generators; ++i) {
      gl::RMATEdgeReceiver* edge_receiver =
          new gl::RMATEdgeReceiver(config, partition_managers, i);
      edge_receivers[i] = edge_receiver;
      edge_receiver->start();
    }

    // wait for all edges to be scanned and accounted for:
    for (int i = 0; i < config.count_edge_generators; ++i) {
      edge_receivers[i]->join();
      edge_receivers[i]->reduceVertexDegrees(vertex_degrees);
    }

    // finish partition-managers in this phase:
    for (int i = 0; i < config.count_partition_managers; ++i) {
      // send shutdown-message to partition-manager
      ring_buffer_req_t request;
      ring_buffer_put_req_init(&request, BLOCKING, sizeof(partition_edge_list_t));
      ring_buffer_put(partition_managers[i]->edges_rb_, &request);
      sg_rb_check(&request);

      partition_edge_list_t* edge_list_in_rb = (partition_edge_list_t*) request.data;
      edge_list_in_rb->shutdown_indicator = true;

      // set element ready, done!
      ring_buffer_elm_set_ready(partition_managers[i]->edges_rb_, request.data);

      // then wait for shutdown and delete
      partition_managers[i]->join();
      partition_managers[i]->cleanUpCouting();
    }

    // write the vertex-degree-file:
    std::string vertex_degree_file_name = core::getVertexDegreeFileName(config);

    size_t size_vertex_degree_file =
        sizeof(vertex_degree_t) * config.count_vertices;

    util::writeDataToFile(vertex_degree_file_name,
                          reinterpret_cast<const void*>(vertex_degrees),
                          size_vertex_degree_file);
  }

  for (int i = 0; i < config.count_partition_managers; ++i) {
    sg_log("Delete partition-manager %d\n", i);
    delete partition_managers[i];
  }

  return 0;
}
