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
#include <ring_buffer.h>

#include "iedges-reader.h"
#include "delim-edges-reader.h"
#include "binary-edge-reader.h"
#include "rmat-edge-generator.h"
#include "partition-manager.h"
#include "write-worker.h"

namespace gl = scalable_graphs::graph_load;
namespace util = scalable_graphs::util;
namespace core = scalable_graphs::core;

struct command_line_args_t : public command_line_args_grc_t {
  uint32_t count_write_threads;
  uint64_t rmat_count_edges;
  std::string source;
  std::string graph_generator;
  std::string delimiter;
  bool use_original_ids;
};

static int parseOption(int argc, char* argv[], command_line_args_t& cmd_args) {
  static struct option options[] = {
      {"source", required_argument, 0, 's'},
      {"count-vertices", required_argument, 0, 'v'},
      {"delimiter", required_argument, 0, 'd'},
      {"generator", required_argument, 0, 'a'},
      {"graphname", required_argument, 0, 'g'},
      {"paths-partition", required_argument, 0, 'p'},
      {"path-global", required_argument, 0, 'l'},
      {"nthreads", required_argument, 0, 'n'},
      {"nwritethreads", required_argument, 0, 'w'},
      {"npartition-managers", required_argument, 0, 'm'},
      {"input-weighted", required_argument, 0, 'i'},
      {"rmat-count-edges", required_argument, 0, 'e'},
      {"use-original-ids", required_argument, 0, 'o'},
      {0, 0, 0, 0},
  };
  int arg_cnt;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    int c, idx = 0;
    c = getopt_long(argc, argv, "s:v:d:a:g:p:l:n:w:m:t:e:o:", options, &idx);
    if (c == -1)
      break;

    switch (c) {
    case 's':
      cmd_args.source = std::string(optarg);
      break;
    case 'a':
      cmd_args.graph_generator = std::string(optarg);
      break;
    case 'd':
      cmd_args.delimiter = std::string(optarg);
      break;
    case 'g':
      cmd_args.graphname = std::string(optarg);
      break;
    case 'p':
      cmd_args.paths_to_partition = util::splitDirPaths(std::string(optarg));
      break;
    case 'l':
      cmd_args.path_to_globals = util::prepareDirPath(std::string(optarg));
      break;
    case 'n':
      cmd_args.nthreads = std::stoi(std::string(optarg));
      break;
    case 'w':
      cmd_args.count_write_threads = std::stoi(std::string(optarg));
      break;
    case 'm':
      cmd_args.count_partition_managers = std::stoi(std::string(optarg));
      break;
    case 'i':
      cmd_args.input_weighted =
          (std::stoi(std::string(optarg)) == 1) ? true : false;
      break;
    case 'e':
      cmd_args.rmat_count_edges = std::stoull(std::string(optarg));
      break;
    case 'o':
      cmd_args.use_original_ids =
          (std::stoi(std::string(optarg)) == 1) ? true : false;
      break;
    case 'v':
      cmd_args.count_vertices = std::stoull(std::string(optarg));
      break;
    default:
      return -EINVAL;
    }
  }
  return arg_cnt;
}

template <typename TVertexIdType>
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
      gl::RMATEdgeGenerator<edge_weighted_t, TVertexIdType> reader(
          config, partition_managers);
      reader.readEdges(config.nthreads);
    } else {
      sg_err("Generator %s not supported, aborting!\n", generator.c_str());
      util::die(1);
    }
  } else {
    if (generator == "delim_edges") {
      gl::DelimEdgesReader<edge_t, TVertexIdType> reader(config,
                                                         partition_managers);
      reader.readEdges(config.nthreads);
    } else if (generator == "binary") {
      gl::BinaryEdgeReader <file_edge_t, TVertexIdType> reader(config, partition_managers);
      reader.readEdges(config.nthreads);
    } else if (generator == "rmat") {
      gl::RMATEdgeGenerator<edge_t, TVertexIdType> reader(config,
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
  fprintf(out, "  --graphname           = graph name\n");
  fprintf(out, "  --count-vertices      = count vertices for rmat-generator\n");
  fprintf(out, "  --generator           = graph generator ('rmat' or "
               "'delim_edges')\n");
  fprintf(out, "  --source              = graph raw data (for "
               "'delim_edges')\n");
  fprintf(out, "  --delimiter           = delimiter for reading graph from "
               "disk, options are 'tab', 'comma', 'space', 'semicolon'\n");
  fprintf(out, "  --paths-partition     = paths to interim partition data\n");
  fprintf(out, "  --path-globals        = path to global data\n");
  fprintf(out, "  --nthreads            = number of concurrent threads\n");
  fprintf(out, "  --nwritethreads       = number of write-threads\n");
  fprintf(out, "  --npartition-managers = number of partition managers\n");
  fprintf(out, "  --input-weighted      = whether to read a weighted graph \n");
  fprintf(out, "  --rmat-count-edges    = count edges for rmat-generator\n");
  fprintf(out, "  --use-original-ids    = use the original id's of the file\n");
}

int main(int argc, char** argv) {
  command_line_args_t cmd_args;

  // parse command line options
  if (parseOption(argc, argv, cmd_args) != 13) {
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

  config_partitioner_t config;
  core::initGrcConfig(&config, current_settings.count_vertices, cmd_args);

  config.settings = current_settings;
  config.source = cmd_args.source;
  config.rmat_count_edges = cmd_args.rmat_count_edges;
  config.use_original_ids = cmd_args.use_original_ids;
  config.count_write_threads = cmd_args.count_write_threads;
  config.partition_mode = PartitionMode::PM_FileBackedMode;

  // create ring-buffer for write-threads
  ring_buffer_t* write_request_rb;
  size_t write_request_rb_size = 1ul * GB;
  int rc =
      ring_buffer_create(write_request_rb_size, L1D_CACHELINE_SIZE,
                         RING_BUFFER_BLOCKING, NULL, NULL, &write_request_rb);
  if (rc) {
    scalable_graphs::util::die(1);
  }

  gl::PartitionManager** partition_managers =
      new gl::PartitionManager*[config.count_partition_managers];

  for (int i = 0; i < config.count_partition_managers; ++i) {
    partition_manager_arguments_t arguments =
        core::getPartitionManagerArguments(i, config);
    arguments.write_request_rb = write_request_rb;

    gl::PartitionManager* partition_manager =
        new gl::PartitionManager(config, arguments);
    partition_manager->initWrite();
    partition_manager->start();

    partition_managers[i] = partition_manager;
  }

  gl::WriteWorker** write_threads =
      new gl::WriteWorker*[config.count_write_threads];
  for (int i = 0; i < config.count_write_threads; ++i) {
    gl::WriteWorker* write_worker = new gl::WriteWorker(write_request_rb);
    write_worker->start();
    write_threads[i] = write_worker;
  }

  // parsing
  if (is_index_32_bits) {
    runEdgesReader<uint32_t>(cmd_args.graph_generator, config,
                             partition_managers);
  } else {
    runEdgesReader<uint64_t>(cmd_args.graph_generator, config,
                             partition_managers);
  }

  // Shutdown-procedure:
  // - Shutdown PartitionManagers (and PartitionStores)
  // - Join PartitionManagers
  // - Shutdown Write-Threads
  // - Join Write-Threads and destroy
  // - Destroy PartitionManagers
  // This order ensures that the write-threads are only shutdown after all edges
  // have been written to the buffer and that the file-descriptors stay alive
  // until all I/O ws finished.

  // shutdown all partition-managers by sending specially crafted
  // "shutdown"-message
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
    // trigger write of partitions-information
    partition_managers[i]->cleanupWrite();
  }

  // send shutdown-message to write-thread
  for (int i = 0; i < config.count_write_threads; ++i) {
    ring_buffer_req_t request;
    ring_buffer_put_req_init(&request, BLOCKING,
                             sizeof(edge_write_request_t) + sizeof(uint64_t));
    ring_buffer_put(write_request_rb, &request);
    sg_rb_check(&request);

    // write shutdown-indicator into ringbuffer
    uint64_t* shutdown_indicator =
        (uint64_t*)((uint8_t*)request.data + sizeof(edge_write_request_t));
    *shutdown_indicator = 1;

    // set element ready, done!
    ring_buffer_elm_set_ready(write_request_rb, request.data);
  }

  // now wait for all write-threads to be done and delete them:
  for (int i = 0; i < config.count_write_threads; ++i) {
    write_threads[i]->join();
    delete write_threads[i];
  }

  // clean up PartitionManagers
  for (int i = 0; i < config.count_partition_managers; ++i) {
    delete partition_managers[i];
  }

  ring_buffer_destroy(write_request_rb);

  return 0;
}
