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

static int parseOption(int argc, char* argv[],
                       config_edge_processor_t& config) {
  static struct option options[] = {
      {"port", required_argument, 0, 'a'},
      {"algorithm", required_argument, 0, 'b'},
      {"paths-meta", required_argument, 0, 'c'},
      {"paths-tile", required_argument, 0, 'd'},
      {"max-iterations", required_argument, 0, 'e'},
      {"count-reader", required_argument, 0, 'f'},
      {"count-processor", required_argument, 0, 'g'},
      {"in-memory-mode", required_argument, 0, 'h'},
      {"mic-index", required_argument, 0, 'i'},
      {"nmic", required_argument, 0, 'j'},
      {"path-globals", required_argument, 0, 'k'},
      {"run-on-mic", required_argument, 0, 'l'},
      {"use-selective-scheduling", required_argument, 0, 'm'},
      {"do-perfmon", required_argument, 0, 'n'},
      {"processed-rb-size", required_argument, 0, 'o'},
      {"read-tiles-rb-size", required_argument, 0, 'p'},
      {"host-tiles-rb-size", required_argument, 0, 'q'},
      {"tile-processor-mode", required_argument, 0, 'r'},
      {"tile-processor-input-mode", required_argument, 0, 's'},
      {"tile-processor-output-mode", required_argument, 0, 't'},
      {"count-vertex-reducer", required_argument, 0, 'u'},
      {"enable-perf-event-collection", required_argument, 0, 'v'},
      {"path-perf-events", required_argument, 0, 'w'},
      {"local-fetcher-mode", required_argument, 0, 'x'},
      {"count-globalreducer", required_argument, 0, 'y'},
      {"count-globalfetcher", required_argument, 0, 'z'},
      {"count-indexreader", required_argument, 0, 'A'},
      {"count-vertex-fetcher", required_argument, 0, 'B'},
      {"use-smt", required_argument, 0, 'C'},
      {"count-followers", required_argument, 0, 'D'},
      {0, 0, 0, 0},
  };
  int arg_cnt;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    int c, idx = 0;
    c = getopt_long(
        argc, argv,
        "a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:s:t:u:v:w:x:y:z:A:B:C:D:", options,
        &idx);
    if (c == -1)
      break;

    switch (c) {
    case 'a':
      config.port = std::stoi(std::string(optarg));
      break;
    case 'b':
      config.algorithm = std::string(optarg);
      break;
    case 'c':
      config.paths_to_meta = util::splitDirPaths(std::string(optarg));
      break;
    case 'd':
      config.paths_to_tile = util::splitDirPaths(std::string(optarg));
      break;
    case 'e':
      config.max_iterations = std::stoi(std::string(optarg));
      break;
    case 'f':
      config.count_tile_readers = std::stoi(std::string(optarg));
      break;
    case 'g':
      config.count_tile_processors = std::stoi(std::string(optarg));
      break;
    case 'h':
      config.in_memory_mode = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'i':
      config.mic_index = std::stoi(std::string(optarg));
      break;
    case 'j':
      config.count_edge_processors = std::stoi(std::string(optarg));
      break;
    case 'k':
      config.path_to_globals = util::prepareDirPath(std::string(optarg));
      break;
    case 'l':
      config.run_on_mic = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'm':
      config.use_selective_scheduling = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'n':
      config.do_perfmon = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'o':
      config.processed_rb_size = std::stoull(std::string(optarg));
      break;
    case 'p':
      config.read_tiles_rb_size = std::stoull(std::string(optarg));
      break;
    case 'q':
      config.host_tiles_rb_size = std::stoull(std::string(optarg));
      break;
    case 'r': {
      std::string arg(optarg);
      if (arg == "Active") {
        config.tile_processor_mode = TileProcessorMode::TPM_Active;
      } else if (arg == "Noop") {
        config.tile_processor_mode = TileProcessorMode::TPM_Noop;
      } else {
        sg_log("Invalid value for tile processor mode supplied: %s\n",
               arg.c_str());
        util::die(1);
      }
      break;
    }
    case 's': {
      std::string arg(optarg);
      if (arg == "VertexFetcher") {
        config.tile_processor_input_mode =
            TileProcessorInputMode::TPIM_VertexFetcher;
      } else if (arg == "FakeVertexFetcher") {
        config.tile_processor_input_mode =
            TileProcessorInputMode::TPIM_FakeVertexFetcher;
      } else if (arg == "ConstantValue") {
        config.tile_processor_input_mode =
            TileProcessorInputMode::TPIM_ConstantValue;
      } else {
        sg_log("Invalid value for tile processor input mode supplied: %s\n",
               arg.c_str());
        util::die(1);
      }
      break;
    }
    case 't': {
      std::string arg(optarg);
      if (arg == "VertexReducer") {
        config.tile_processor_output_mode =
            TileProcessorOutputMode::TPOM_VertexReducer;
      } else if (arg == "FakeVertexReducer") {
        config.tile_processor_output_mode =
            TileProcessorOutputMode::TPOM_FakeVertexReducer;
      } else if (arg == "Noop") {
        config.tile_processor_output_mode = TileProcessorOutputMode::TPOM_Noop;
      } else {
        sg_log("Invalid value for tile processor output mode supplied: %s\n",
               arg.c_str());
        util::die(1);
      }
      break;
    }
    case 'u':
      config.count_vertex_reducers = std::stoi(std::string(optarg));
      break;
    case 'v':
      config.enable_perf_event_collection =
          (std::stoi(std::string(optarg)) == 1);
      break;
    case 'w':
      config.path_to_perf_events = util::prepareDirPath(std::string(optarg));
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
    case 'y':
      config.count_global_reducers = std::stoi(std::string(optarg));
      break;
    case 'z':
      config.count_global_fetchers = std::stoi(std::string(optarg));
      break;
    case 'A':
      config.count_index_readers = std::stoi(std::string(optarg));
      break;
    case 'B':
      config.count_vertex_fetchers = std::stoi(std::string(optarg));
      break;
    case 'C':
      config.use_smt = (std::stoi(std::string(optarg)) == 1);
      break;
    case 'D':
      config.count_followers = std::stoi(std::string(optarg));
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
  fprintf(out, "  --port        = base port number\n");
  fprintf(out, "  --algorithm   = graph raw data\n");
  fprintf(out,
          "  --paths-meta        = output paths to metadata separated by :\n");
  fprintf(out,
          "  --paths-tile        = output paths to tiledata separated by :\n");
  fprintf(out, "  --path-globals   = path to global data\n");
  fprintf(out, "  --max-iterations    = maximum iterations\n");
  fprintf(out, "  --count-globalreducer = number of global vertex reducers\n");
  fprintf(out, "  --count-globalfetchers= number of global fetchers\n");
  fprintf(out, "  --count-indexreader   = number of index readers\n");
  fprintf(out,
          "  --count-vertex-reducers  = number of vertex reducers on host.\n");
  fprintf(out, "  --count-vertex-fetchers = number of vertex fetchers\n");
  fprintf(out, "  --count-reader     = number of tile readers\n");
  fprintf(out, "  --count-processor  = number of tile processors\n");
  fprintf(out, "  --in-memoty-mode       = whether to run from memory "
               "instead of disk\n");
  fprintf(out, "  --mic-index   = index of this MIC\n");
  fprintf(out, "  --nmic        = count of all MICs\n");
  fprintf(out, "  --run-on-mic  = whether we are running on MIC or not\n");
  fprintf(out, "  --use-selective-scheduling   = whether to active the "
               "selective scheduling\n");
  fprintf(out, "  --do-perfmon  = enabled performance monitoring\n");
  fprintf(out, "  --proccessed-rb-size  = The size of the ringbuffer to save "
               "processed tiles into.\n");
  fprintf(out, "  --read-tiles-rb-size  = The size of the ringbuffer to save "
               "tiles read from disk into.\n");
  fprintf(out, "  --host-tiles-rb-size  = The size of the ringbuffer to save "
               "tiles received from the host into.\n");
  fprintf(out, "  --tile-processor-mode  = The mode of the tile processor, "
               "options are: Active, Noop.\n");
  fprintf(out, "  --tile-processor-input-mode  = The mode for the input to the "
               "tile processor, options are: VertexFetcher, FakeVertexFetcher, "
               "ConstantValue.\n");
  fprintf(out,
          "  --tile-processor-output-mode  = The mode for the ouput of the "
          "tile processor, options are: VertexReducer, FakeVertexReducer, "
          "Noop.\n");
  fprintf(out, "  --enable-perf-event-collection  = enable collection of "
               "traces to be output to JSON.\n");
  fprintf(out,
          "  --path-perf-events  = Where to output the JSON perf events to.\n");
  fprintf(out, "  --local-fetcher-mode  = the mode for the global fetcher to "
               "run in, options are: Active and ConstantValue.\n");
  fprintf(out, "  --use-smt   = use smt\n");
  fprintf(out, "  --count-followers   = the number of followers to use.\n");
}

template <class APP, typename TVertexType, bool is_weighted>
static void executeEngine(const config_edge_processor_t& config) {
  core::EdgeProcessor<APP, TVertexType, is_weighted> edge_processor(config);
  edge_processor.init();
  edge_processor.initActiveTiles();
  edge_processor.start();
  edge_processor.join();
}

static void run(const config_edge_processor_t& config) {
  if (config.algorithm == "pagerank") {
    executeEngine<core::PageRank, core::PageRank::VertexType, false>(config);
  } else if (config.algorithm == "bfs") {
    executeEngine<core::BFS, core::BFS::VertexType, false>(config);
  } else if (config.algorithm == "cc") {
    executeEngine<core::CC, core::CC::VertexType, false>(config);
  } else if (config.algorithm == "spmv") {
    executeEngine<core::SPMV, core::SPMV::VertexType, false>(config);
  } else if (config.algorithm == "tc") {
    executeEngine<core::TC, core::TC::VertexType, false>(config);
  } else {
    sg_log2("No algorithm selected, will exit now!\n");
  }
}

static void runWeighted(const config_edge_processor_t& config) {
  if (config.algorithm == "sssp") {
    executeEngine<core::SSSP, core::SSSP::VertexType, true>(config);
  } else if (config.algorithm == "bp") {
    executeEngine<core::BP, core::BP::VertexType, true>(config);
  } else {
    sg_log2("No algorithm selected, will exit now!\n");
  }
}

int main(int argc, char** argv) {
  config_edge_processor_t config;

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

  sg_log("Running with %lu tiles and %lu vertices, 32-bit index: %d, weighted: "
         "%d\n",
         global_stats.count_tiles, global_stats.count_vertices,
         global_stats.is_index_32_bits, global_stats.is_weighted_graph);

  // set up edge processor
  config.count_tiles = global_stats.count_tiles;
  config.is_graph_weighted = global_stats.is_weighted_graph;
  config.is_index_32_bits = global_stats.is_index_32_bits;

  // run!
  if (global_stats.is_weighted_graph) {
    runWeighted(config);
  } else {
    run(config);
  }

  sg_log("Exit edge-engine %d!\n", config.mic_index);

  return 0;
}
