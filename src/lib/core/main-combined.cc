#include <cmath>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/mman.h>
#include <pthread.h>
#include <getopt.h>
#include <errno.h>

#include <util/runnable.h>
#include <ring_buffer.h>
#include <core/datatypes.h>
#include <core/util.h>
#include <core/vertex-domain.h>
#include <core/edge-processor.h>

#include "algorithms/pagerank.h"
#include "algorithms/bfs.h"
#include "algorithms/cc.h"
#include "algorithms/sssp.h"
#include "algorithms/spmv.h"
#include "algorithms/tc.h"
#include "algorithms/bp.h"
#include "algorithms/kmc.h"

namespace core = scalable_graphs::core;
namespace util = scalable_graphs::util;

static int parseOption(int argc, char* argv[], config_vertex_domain_t& config_vertex,
                       config_edge_processor_t& config_edge) {
  static struct option options[] = {
      {"algorithm",                    required_argument, 0, 'a'},
      {"max-iterations",               required_argument, 0, 'b'},
      {"nmic",                         required_argument, 0, 'c'},
      {"count-applier",                required_argument, 0, 'd'},
      {"count-globalreducer",          required_argument, 0, 'e'},
      {"count-globalfetcher",          required_argument, 0, 'f'},
      {"count-indexreader",            required_argument, 0, 'g'},
      {"count-vertex-reducer",         required_argument, 0, 'h'},
      {"count-vertex-fetcher",         required_argument, 0, 'i'},
      {"in-memory-mode",               required_argument, 0, 'j'},
      {"log",                          required_argument, 0, 'k'},
      {"paths-meta",                   required_argument, 0, 'l'},
      {"paths-tile",                   required_argument, 0, 'm'},
      {"path-globals",                 required_argument, 0, 'n'},
      {"use-selective-scheduling",     required_argument, 0, 'o'},
      {"path-fault-tolerance-output",  required_argument, 0, 'p'},
      {"enable-fault-tolerance",       required_argument, 0, 'q'},
      {"enable-tile-partitioning",     required_argument, 0, 'r'},
      {"count-tile-reader",            required_argument, 0, 's'},
      {"local-fetcher-mode",           required_argument, 0, 't'},
      {"global-fetcher-mode",          required_argument, 0, 'u'},
      {"enable-perf-event-collection", required_argument, 0, 'v'},
      {"path-perf-events",             required_argument, 0, 'w'},
      {"count-tile-processors",        required_argument, 0, 'x'},
      {"use-smt",                      required_argument, 0, 'y'},
      {"host-tiles-rb-size",           required_argument, 0, 'z'},
      {"local-reducer-mode",           required_argument, 0, 'A'},
      {"processed-rb-size",            required_argument, 0, 'B'},
      {"read-tiles-rb-size",           required_argument, 0, 'C'},
      {"tile-processor-mode",          required_argument, 0, 'D'},
      {"tile-processor-input-mode",    required_argument, 0, 'E'},
      {"tile-processor-output-mode",   required_argument, 0, 'F'},
      {"count-followers",              required_argument, 0, 'G'},
      {0, 0,                                              0, 0},
  };
  int arg_cnt;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    int c, idx = 0;
    c = getopt_long(
        argc, argv,
        "a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:s:t:u:v:w:x:y:z:A:B:C:D:E:F:G:",
        options, &idx);
    if (c == -1) {
      break;
    }

    config_vertex.do_perfmon = false;
    config_edge.do_perfmon = false;

    switch (c) {
      case 'a':
        config_vertex.algorithm = std::string(optarg);
        config_edge.algorithm = std::string(optarg);
        break;
      case 'b':
        config_vertex.max_iterations = std::stoi(std::string(optarg));
        config_edge.max_iterations = std::stoi(std::string(optarg));
        break;
      case 'c':
        config_vertex.count_edge_processors = std::stoi(std::string(optarg));
        config_edge.count_edge_processors = std::stoi(std::string(optarg));
        break;
      case 'd':
        config_vertex.count_vertex_appliers = std::stoi(std::string(optarg));
        break;
      case 'e':
        config_vertex.count_global_reducers = std::stoi(std::string(optarg));
        config_edge.count_global_reducers = std::stoi(std::string(optarg));
        break;
      case 'f':
        config_vertex.count_global_fetchers = std::stoi(std::string(optarg));
        config_edge.count_global_fetchers = std::stoi(std::string(optarg));
        break;
      case 'g':
        config_vertex.count_index_readers = std::stoi(std::string(optarg));
        config_edge.count_index_readers = std::stoi(std::string(optarg));
        break;
      case 'h':
        config_vertex.count_vertex_reducers = std::stoi(std::string(optarg));
        config_edge.count_vertex_reducers = std::stoi(std::string(optarg));
        break;
      case 'i':
        config_vertex.count_vertex_fetchers = std::stoi(std::string(optarg));
        config_edge.count_vertex_fetchers = std::stoi(std::string(optarg));
        break;
      case 'j':
        config_vertex.in_memory_mode = (std::stoi(std::string(optarg)) == 1);
        config_edge.in_memory_mode = (std::stoi(std::string(optarg)) == 1);
        break;
      case 'k':
        config_vertex.path_to_log = std::string(optarg);
        if (config_vertex.path_to_log.back() != '/') {
          config_vertex.path_to_log += "/";
        }
        --arg_cnt;
        break;
      case 'l':
        config_vertex.paths_to_meta = util::splitDirPaths(std::string(optarg));
        config_edge.paths_to_meta = util::splitDirPaths(std::string(optarg));
        break;
      case 'm':
        config_vertex.paths_to_tile = util::splitDirPaths(std::string(optarg));
        config_edge.paths_to_tile = util::splitDirPaths(std::string(optarg));
        break;
      case 'n':
        config_vertex.path_to_globals = util::prepareDirPath(std::string(optarg));
        config_edge.path_to_globals = util::prepareDirPath(std::string(optarg));
        break;
      case 'o':
        config_vertex.use_selective_scheduling = (std::stoi(std::string(optarg)) == 1);
        config_edge.use_selective_scheduling = (
            std::stoi(std::string(optarg)) == 1);
        break;
      case 'p':
        config_vertex.fault_tolerance_ouput_path =
            util::prepareDirPath(std::string(optarg));
        break;
      case 'q':
        config_vertex.enable_fault_tolerance = (std::stoi(std::string(optarg)) == 1);
        break;
      case 'r':
        config_vertex.enable_tile_partitioning = (std::stoi(std::string(optarg)) == 1);
        break;
      case 's':
        config_vertex.count_tile_readers = std::stoi(std::string(optarg));
        config_edge.count_tile_readers = std::stoi(std::string(optarg));
        break;
      case 't': {
        std::string arg(optarg);
        if (arg == "GlobalFetcher") {
          config_vertex.local_fetcher_mode = LocalFetcherMode::LFM_GlobalFetcher;
        } else if (arg == "DirectAccess") {
          config_vertex.local_fetcher_mode = LocalFetcherMode::LFM_DirectAccess;
        } else if (arg == "ConstantValue") {
          config_vertex.local_fetcher_mode = LocalFetcherMode::LFM_ConstantValue;
        } else if (arg == "Fake") {
          config_vertex.local_fetcher_mode = LocalFetcherMode::LFM_Fake;
        } else {
          sg_log("Invalid value for local fetcher mode supplied: %s\n",
                 arg.c_str());
          util::die(1);
        }
        config_edge.local_fetcher_mode = config_vertex.local_fetcher_mode;
        break;
      }
      case 'u': {
        std::string arg(optarg);
        if (arg == "Active") {
          config_vertex.global_fetcher_mode = GlobalFetcherMode::GFM_Active;
        } else if (arg == "ConstantValue") {
          config_vertex.global_fetcher_mode = GlobalFetcherMode::GFM_ConstantValue;
        } else {
          sg_log("Invalid value for global fetcher mode supplied: %s\n",
                 arg.c_str());
          util::die(1);
        }
        break;
      }
      case 'v':
        config_vertex.enable_perf_event_collection = (
            std::stoi(std::string(optarg)) == 1);
        config_edge.enable_perf_event_collection = (
            std::stoi(std::string(optarg)) == 1);
        break;
      case 'w':
        config_vertex.path_to_perf_events = util::prepareDirPath(std::string(optarg));
        config_edge.path_to_perf_events = util::prepareDirPath(std::string(
            optarg));
        break;
      case 'x':
        config_vertex.count_tile_processors = std::stoi(std::string(optarg));
        config_edge.count_tile_processors = std::stoi(std::string(optarg));
        break;
      case 'y':
        config_vertex.use_smt = (std::stoi(std::string(optarg)) == 1);
        config_edge.use_smt = (std::stoi(std::string(optarg)) == 1);
        break;
      case 'z':
        config_vertex.host_tiles_rb_size = std::stoull(std::string(optarg));
        config_edge.host_tiles_rb_size = std::stoull(std::string(optarg));
        break;
      case 'A': {
        std::string arg(optarg);
        if (arg == "GlobalReducer") {
          config_vertex.local_reducer_mode = LocalReducerMode::LRM_GlobalReducer;
        } else if (arg == "Locking") {
          config_vertex.local_reducer_mode = LocalReducerMode::LRM_Locking;
        } else if (arg == "Atomic") {
          config_vertex.local_reducer_mode = LocalReducerMode::LRM_Atomic;
        } else {
          sg_log("Invalid value for local reducer mode supplied: %s\n",
                 arg.c_str());
          util::die(1);
        }
        break;
      }
      case 'B':
        config_edge.processed_rb_size = std::stoull(std::string(optarg));
        break;
      case 'C':
        config_edge.read_tiles_rb_size = std::stoull(std::string(optarg));
        break;
      case 'D': {
        std::string arg(optarg);
        if (arg == "Active") {
          config_edge.tile_processor_mode = TileProcessorMode::TPM_Active;
        } else if (arg == "Noop") {
          config_edge.tile_processor_mode = TileProcessorMode::TPM_Noop;
        } else {
          sg_log("Invalid value for tile processor mode supplied: %s\n",
                 arg.c_str());
          util::die(1);
        }
        break;
      }
      case 'E': {
        std::string arg(optarg);
        if (arg == "VertexFetcher") {
          config_edge.tile_processor_input_mode =
              TileProcessorInputMode::TPIM_VertexFetcher;
        } else if (arg == "FakeVertexFetcher") {
          config_edge.tile_processor_input_mode =
              TileProcessorInputMode::TPIM_FakeVertexFetcher;
        } else if (arg == "ConstantValue") {
          config_edge.tile_processor_input_mode =
              TileProcessorInputMode::TPIM_ConstantValue;
        } else {
          sg_log("Invalid value for tile processor input mode supplied: %s\n",
                 arg.c_str());
          util::die(1);
        }
        break;
      }
      case 'F': {
        std::string arg(optarg);
        if (arg == "VertexReducer") {
          config_edge.tile_processor_output_mode =
              TileProcessorOutputMode::TPOM_VertexReducer;
        } else if (arg == "FakeVertexReducer") {
          config_edge.tile_processor_output_mode =
              TileProcessorOutputMode::TPOM_FakeVertexReducer;
        } else if (arg == "Noop") {
          config_edge.tile_processor_output_mode = TileProcessorOutputMode::TPOM_Noop;
        } else {
          sg_log("Invalid value for tile processor output mode supplied: %s\n",
                 arg.c_str());
          util::die(1);
        }
        break;
      }
      case 'G':
        config_edge.count_followers = std::stoi(std::string(optarg));
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
  fprintf(out, "  --algorithm      = graph raw data\n");
  fprintf(out, "  --max-iterations       = maximum iterations\n");
  fprintf(out, "  --nmic           = number of MICs; mic{0} to mic{nmic - 1} "
      "will be used\n");
  fprintf(out, "  --count-applier       = number of vertex appliers\n");
  fprintf(out, "  --count-globalreducer = number of global vertex reducers\n");
  fprintf(out, "  --count-globalfetchers= number of global fetchers\n");
  fprintf(out, "  --count-indexreader   = number of index readers\n");
  fprintf(out, "  --count-vertex-reducer = number of vertex reducers\n");
  fprintf(out, "  --count-vertex-fetcher = number of vertex updaters\n");
  fprintf(out, "  --in-memory-mode       = whether to run from memory "
      "instead of disk\n");
  fprintf(out, "  --port           = base port number\n");
  fprintf(out, "  --run-on-mic     = run using MIC-support (1 == enabled)\n");
  fprintf(out, "  --log            = log directory (optional)\n");
  fprintf(out, "  --paths-meta           = path to metadata\n");
  fprintf(out, "  --paths-tile           = output paths to tiledata "
      "separated by :\n");
  fprintf(out, "  --path-globals   = path to global data\n");
  fprintf(out,
          "  --edge-engine-to-mic   = list which engine runs on which MIC\n");
  fprintf(out, "  --use-selective-scheduling   = whether to active the "
      "selective scheduling\n");
  fprintf(out, "  --path-fault-tolerance-output   = path to output fault "
      "tolerance data\n");
  fprintf(out, "  --enable-fault-tolerance       = whether to enable the fault "
      "tolerant mode\n");
  fprintf(out,
          "  --enable-tile-partitioning  = enables the tile partitioning.\n");
  fprintf(out, "  --do-perfmon  = enabled performance monitoring\n");
  fprintf(out,
          "  --count-tile-reader   = number of tile readers per edge-engine\n");
  fprintf(out, "  --local-fetcher-mode  = the mode for the local fetcher to "
      "run in, options are: GlobalFetcher, DirectAccess, "
      "ConstantValue and Fake.\n");
  fprintf(out, "  --global-fetcher-mode  = the mode for the global fetcher to "
      "run in, options are: Active and ConstantValue.\n");
  fprintf(out, "  --enable-perf-event-collection  = enable collection of "
      "traces to be output to JSON.\n");
  fprintf(out,
          "  --path-perf-events  = Where to output the JSON perf events to.\n");
  fprintf(out, "  --count-tile-processors   = number of tile processors per "
      "edge-engine\n");
  fprintf(out, "  --use-smt   = use smt\n");
  fprintf(out, "  --host-tiles-rb-size  = The size of the ringbuffer to save "
      "tiles received from the host into.\n");
  fprintf(out, "  --local-reducer-mode  = the mode for the local reducer to "
      "run in, options are: GlobalReducer, Locking, "
      "and Atomic.\n");
}

template<class APP, typename TVertexType, typename TVertexIdType, bool is_weighted>
static void executeEngine(config_vertex_domain_t& config_vertex,
                          const config_edge_processor_t& config_edge) {
  // Start up all edge engines, then start vertex engine.
  auto edge_processors = new
      core::EdgeProcessor <APP, TVertexType, is_weighted>*
      [config_vertex.count_edge_processors];

  // Start all edge processors.
  for (int i = 0; i < config_vertex.count_edge_processors; ++i) {
    config_edge_processor_t local_config_edge = config_edge;
    local_config_edge.mic_index = i;
    local_config_edge.paths_to_meta = {config_edge.paths_to_meta[i]};
    local_config_edge.paths_to_tile = {config_edge.paths_to_tile[i]};
    auto edge_processor = new core::EdgeProcessor<APP, TVertexType, is_weighted>(
        local_config_edge);
    edge_processors[i] = edge_processor;

    edge_processor->init();

    ringbuffer_config_t ringbuffer_config = edge_processor->getRingbufferConfig();
    config_vertex.ringbuffer_configs[i] = ringbuffer_config;

    sg_log("Started edge processor %d.\n", i);
  }
  sg_log2("Started all edge processors.\n");

  core::VertexDomain <APP, TVertexType, TVertexIdType> vertex_domain(
      config_vertex);
  vertex_domain.init();
  sg_log2("Init done\n");
  vertex_domain.start();
  sg_log2("Start done\n");

  for (int i = 0; i < config_vertex.count_edge_processors; ++i) {
    edge_processors[i]->initActiveTiles();
    edge_processors[i]->start();
  }

  sg_log2("Started vertex processor.\n");

  // Join all edge processors.
  for (int i = 0; i < config_vertex.count_edge_processors; ++i) {
    edge_processors[i]->join();
  }
  sg_log2("Joined all edge processors.\n");

  vertex_domain.join();

  sg_log2("Joined vertex processor.\n");
}

template<typename TVertexIdType>
static void runUnweighted(config_vertex_domain_t& config_vertex,
                          const config_edge_processor_t& config_edge) {
  if (config_vertex.algorithm == "pagerank") {
    executeEngine<core::PageRank, core::PageRank::VertexType, TVertexIdType, false>(
        config_vertex, config_edge);
  } else if (config_vertex.algorithm == "bfs") {
    executeEngine<core::BFS, core::BFS::VertexType, TVertexIdType, false>(
        config_vertex,
        config_edge);
  } else if (config_vertex.algorithm == "cc") {
    executeEngine<core::CC, core::CC::VertexType, TVertexIdType, false>(
        config_vertex,
        config_edge);
  } else if (config_vertex.algorithm == "spmv") {
    executeEngine<core::SPMV, core::SPMV::VertexType, TVertexIdType, false>(
        config_vertex,
        config_edge);
  } else if (config_vertex.algorithm == "tc") {
    executeEngine<core::TC, core::TC::VertexType, TVertexIdType, false>(
        config_vertex,
        config_edge);
  } else {
    sg_log2("No algorithm selected, will exit now!\n");
  }
}

template<typename TVertexIdType>
static void runWeighted(config_vertex_domain_t& config_vertex,
                        const config_edge_processor_t& config_edge) {
  if (config_vertex.algorithm == "sssp") {
    executeEngine<core::SSSP, core::SSSP::VertexType, TVertexIdType, true>(
        config_vertex,
        config_edge);
  } else if (config_vertex.algorithm == "bp") {
    executeEngine<core::BP, core::BP::VertexType, TVertexIdType, true>(
        config_vertex,
        config_edge);
  } else {
    sg_log2("No algorithm selected, will exit now!\n");
  }
}

template<typename TVertexIdType>
static void run(config_vertex_domain_t& config_vertex,
                const config_edge_processor_t& config_edge) {
  if (config_vertex.is_graph_weighted) {
    runWeighted<TVertexIdType>(config_vertex, config_edge);
  } else {
    runUnweighted<TVertexIdType>(config_vertex, config_edge);
  }
}

int main(int argc, char** argv) {
  config_vertex_domain_t config_vertex;
  config_edge_processor_t config_edge;

  // parse command line options
  if (parseOption(argc, argv, config_vertex, config_edge) != 32) {
    usage(stderr);
    return 1;
  }

  // load graph info
  scenario_stats_t global_stats;
  std::string global_stat_file_name =
      core::getGlobalStatFileName(config_vertex.paths_to_meta[0]);
  util::readDataFromFile(global_stat_file_name, sizeof(scenario_stats_t),
                         &global_stats);

  // set up vertex domain
  config_vertex.count_vertices = global_stats.count_vertices;
  config_vertex.count_tiles = global_stats.count_tiles;
  config_vertex.is_graph_weighted = global_stats.is_weighted_graph;
  config_vertex.is_index_32_bits = global_stats.is_index_32_bits;
  config_vertex.run_on_mic = false;

  config_edge.count_tiles = global_stats.count_tiles;
  config_edge.is_graph_weighted = global_stats.is_weighted_graph;
  config_edge.is_index_32_bits = global_stats.is_index_32_bits;
  config_edge.run_on_mic = false;

  /*to supress gcc warning change print datatype */
  sg_log("Running with %lu vertices, %lu tiles, 32-bit-index: %s\n",
         global_stats.count_vertices, global_stats.count_tiles,
         global_stats.is_index_32_bits == true ? "true" : "false");

  // run!
  if (global_stats.is_index_32_bits) {
    run<uint32_t>(config_vertex, config_edge);
  } else {
    run<uint64_t>(config_vertex, config_edge);
  }

  sg_log2("Exit Mosaic!\n");

  return 0;
}
