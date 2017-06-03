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

static int parseOption(int argc, char* argv[], config_vertex_domain_t& config) {
  static struct option options[] = {
      {"algorithm", required_argument, 0, 'a'},
      {"max-iterations", required_argument, 0, 'b'},
      {"nmic", required_argument, 0, 'c'},
      {"count-applier", required_argument, 0, 'd'},
      {"count-globalreducer", required_argument, 0, 'e'},
      {"count-globalfetcher", required_argument, 0, 'f'},
      {"count-indexreader", required_argument, 0, 'g'},
      {"count-vertex-reducer", required_argument, 0, 'h'},
      {"count-vertex-fetcher", required_argument, 0, 'i'},
      {"in-memory-mode", required_argument, 0, 'j'},
      {"port", required_argument, 0, 'k'},
      {"run-on-mic", required_argument, 0, 'l'},
      {"log", required_argument, 0, 'm'},
      {"paths-meta", required_argument, 0, 'n'},
      {"paths-tile", required_argument, 0, 'o'},
      {"path-globals", required_argument, 0, 'p'},
      {"edge-engine-to-mic", required_argument, 0, 'q'},
      {"use-selective-scheduling", required_argument, 0, 'r'},
      {"path-fault-tolerance-output", required_argument, 0, 's'},
      {"enable-fault-tolerance", required_argument, 0, 't'},
      {"enable-tile-partitioning", required_argument, 0, 'u'},
      {"do-perfmon", required_argument, 0, 'v'},
      {"count-tile-reader", required_argument, 0, 'w'},
      {"local-fetcher-mode", required_argument, 0, 'x'},
      {"global-fetcher-mode", required_argument, 0, 'y'},
      {"enable-perf-event-collection", required_argument, 0, 'z'},
      {"path-perf-events", required_argument, 0, 'A'},
      {"count-tile-processors", required_argument, 0, 'B'},
      {"use-smt", required_argument, 0, 'C'},
      {"host-tiles-rb-size", required_argument, 0, 'D'},
      {"local-reducer-mode", required_argument, 0, 'E'},
      {0, 0, 0, 0},
  };
  int arg_cnt;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    int c, idx = 0;
    c = getopt_long(
        argc, argv,
        "a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:s:t:u:v:w:x:y:z:A:B:C:D:E",
        options, &idx);
    if (c == -1)
      break;

    switch (c) {
    case 'a':
      config.algorithm = std::string(optarg);
      break;
    case 'b':
      config.max_iterations = std::stoi(std::string(optarg));
      break;
    case 'c':
      config.count_edge_processors = std::stoi(std::string(optarg));
      break;
    case 'd':
      config.count_vertex_appliers = std::stoi(std::string(optarg));
      break;
    case 'e':
      config.count_global_reducers = std::stoi(std::string(optarg));
      break;
    case 'f':
      config.count_global_fetchers = std::stoi(std::string(optarg));
      break;
    case 'g':
      config.count_index_readers = std::stoi(std::string(optarg));
      break;
    case 'h':
      config.count_vertex_reducers = std::stoi(std::string(optarg));
      break;
    case 'i':
      config.count_vertex_fetchers = std::stoi(std::string(optarg));
      break;
    case 'j':
      config.in_memory_mode = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'k':
      config.port = std::stoi(std::string(optarg));
      break;
    case 'l':
      config.run_on_mic = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'm':
      config.path_to_log = std::string(optarg);
      if (config.path_to_log.back() != '/') {
        config.path_to_log += "/";
      }
      --arg_cnt;
      break;
    case 'n':
      config.paths_to_meta = util::splitDirPaths(std::string(optarg));
      break;
    case 'o':
      config.paths_to_tile = util::splitDirPaths(std::string(optarg));
      break;
    case 'p':
      config.path_to_globals = util::prepareDirPath(std::string(optarg));
      break;
    case 'q':
      config.edge_engine_to_mic = util::splitToIntVector(std::string(optarg));
      break;
    case 'r':
      config.use_selective_scheduling = (std::stoi(std::string(optarg)) == 1);
      break;
    case 's':
      config.fault_tolerance_ouput_path =
          util::prepareDirPath(std::string(optarg));
      break;
    case 't':
      config.enable_fault_tolerance = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'u':
      config.enable_tile_partitioning = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'v':
      config.do_perfmon = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'w':
      config.count_tile_readers = std::stoi(std::string(optarg));
      break;
    case 'x': {
      std::string arg(optarg);
      if (arg == "GlobalFetcher") {
        config.local_fetcher_mode = LocalFetcherMode::LFM_GlobalFetcher;
      } else if (arg == "DirectAccess") {
        config.local_fetcher_mode = LocalFetcherMode::LFM_DirectAccess;
      } else if (arg == "ConstantValue") {
        config.local_fetcher_mode = LocalFetcherMode::LFM_ConstantValue;
      } else if (arg == "Fake") {
        config.local_fetcher_mode = LocalFetcherMode::LFM_Fake;
      } else {
        sg_log("Invalid value for local fetcher mode supplied: %s\n",
               arg.c_str());
        util::die(1);
      }
      break;
    }
    case 'y': {
      std::string arg(optarg);
      if (arg == "Active") {
        config.global_fetcher_mode = GlobalFetcherMode::GFM_Active;
      } else if (arg == "ConstantValue") {
        config.global_fetcher_mode = GlobalFetcherMode::GFM_ConstantValue;
      } else {
        sg_log("Invalid value for global fetcher mode supplied: %s\n",
               arg.c_str());
        util::die(1);
      }
      break;
    }
    case 'z':
      config.enable_perf_event_collection =
          (std::stoi(std::string(optarg)) == 1);
      break;
    case 'A':
      config.path_to_perf_events = util::prepareDirPath(std::string(optarg));
      break;
    case 'B':
      config.count_tile_processors = std::stoi(std::string(optarg));
      break;
    case 'C':
      config.use_smt = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'D':
      config.host_tiles_rb_size = std::stoull(std::string(optarg));
      break;
    case 'E': {
      std::string arg(optarg);
      if (arg == "GlobalReducer") {
        config.local_reducer_mode = LocalReducerMode::LRM_GlobalReducer;
      } else if (arg == "Locking") {
        config.local_reducer_mode = LocalReducerMode::LRM_Locking;
      } else if (arg == "Atomic") {
        config.local_reducer_mode = LocalReducerMode::LRM_Atomic;
      } else {
        sg_log("Invalid value for local reducer mode supplied: %s\n",
               arg.c_str());
        util::die(1);
      }
      break;
    }
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

template <class APP, typename TVertexType, typename TVertexIdType>
static void executeEngine(const config_vertex_domain_t& config) {
  core::VertexDomain<APP, TVertexType, TVertexIdType> vertex_domain(config);
  vertex_domain.init();
  vertex_domain.start();
  vertex_domain.join();
}

template <typename TVertexIdType>
static void run(const config_vertex_domain_t& config) {
  if (config.algorithm == "pagerank") {
    executeEngine<core::PageRank, core::PageRank::VertexType, TVertexIdType>(
        config);
  } else if (config.algorithm == "bfs") {
    executeEngine<core::BFS, core::BFS::VertexType, TVertexIdType>(config);
  } else if (config.algorithm == "cc") {
    executeEngine<core::CC, core::CC::VertexType, TVertexIdType>(config);
  } else if (config.algorithm == "sssp") {
    executeEngine<core::SSSP, core::SSSP::VertexType, TVertexIdType>(config);
  } else if (config.algorithm == "spmv") {
    executeEngine<core::SPMV, core::SPMV::VertexType, TVertexIdType>(config);
  } else if (config.algorithm == "tc") {
    executeEngine<core::TC, core::TC::VertexType, TVertexIdType>(config);
  } else if (config.algorithm == "bp") {
    executeEngine<core::BP, core::BP::VertexType, TVertexIdType>(config);
  } else {
    sg_log2("No algorithm selected, will exit now!\n");
  }
}

int main(int argc, char** argv) {
  config_vertex_domain_t config;
  // parse command line options
  if (parseOption(argc, argv, config) != 30) {
    usage(stderr);
    return 1;
  }

  // load graph info
  scenario_stats_t global_stats;
  std::string global_stat_file_name =
      core::getGlobalStatFileName(config.paths_to_meta[0]);
  util::readDataFromFile(global_stat_file_name, sizeof(scenario_stats_t),
                         &global_stats);

  // set up vertex domain
  config.count_vertices = global_stats.count_vertices;
  config.count_tiles = global_stats.count_tiles;
  config.is_graph_weighted = global_stats.is_weighted_graph;
  config.is_index_32_bits = global_stats.is_index_32_bits;

  /*to supress gcc warning change print datatype */
  sg_log("Running with %lu vertices, %lu tiles, 32-bit-index: %s\n",
         global_stats.count_vertices, global_stats.count_tiles,
         global_stats.is_index_32_bits == true ? "true" : "false");

  // run!
  if (global_stats.is_index_32_bits) {
    run<uint32_t>(config);
  } else {
    run<uint64_t>(config);
  }

  sg_log2("Exit vertex-engine!\n");

  return 0;
}
