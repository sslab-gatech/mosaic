#include <stdio.h>
#include <getopt.h>
#include <errno.h>

#include <core/datatypes.h>
#include <core/util.h>
#include <ring_buffer.h>

#include "remote-rmat-generator.h"

namespace gl = scalable_graphs::graph_load;
namespace util = scalable_graphs::util;
namespace core = scalable_graphs::core;

static int parseOption(int argc, char* argv[],
                       config_remote_rmat_generator_t& config) {
  static struct option options[] = {
      {"port", required_argument, 0, 'p'},
      {"edges-start", required_argument, 0, 's'},
      {"edges-end", required_argument, 0, 'e'},
      {"count-threads", required_argument, 0, 'c'},
      {"count-vertices", required_argument, 0, 'v'},
      {"count-partition-managers", required_argument, 0, 'm'},
      {"generator-phase", required_argument, 0, 'a'},
      {0, 0, 0, 0},
  };
  int arg_cnt;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    int c, idx = 0;
    c = getopt_long(argc, argv, "p:s:e:c:v:q:m:a:", options, &idx);
    if (c == -1)
      break;

    switch (c) {
    case 'p':
      config.port = std::stoi(std::string(optarg));
      break;
    case 's':
      config.edges_id_start = std::stoull(std::string(optarg));
      break;
    case 'e':
      config.edges_id_end = std::stoull(std::string(optarg));
      break;
    case 'c':
      config.count_threads = std::stoi(std::string(optarg));
      break;
    case 'v':
      config.count_vertices = std::stoull(std::string(optarg));
      break;
    case 'm':
      config.count_partition_managers = std::stoi(std::string(optarg));
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
    default:
      return -EINVAL;
    }
  }
  return arg_cnt;
}

static void usage(FILE* out) {
  extern const char* __progname;

  fprintf(out, "Usage: %s\n", __progname);
  fprintf(out, "  --port           = port of scif-rb\n");
  fprintf(out, "  --edges-start     = start-id of edges\n");
  fprintf(out, "  --edges-end      = end-id of edges\n");
  fprintf(out, "  --count-threads  = number of threads to start\n");
  fprintf(out, "  --count-partition-managers= number of partition managers\n");
  fprintf(out, "  --count-vertices = count of vertices\n");
  fprintf(out, "  --generator-phase         = generate_tiles or "
               "generate_vertex_degrees\n");
}

int main(int argc, char** argv) {
  config_remote_rmat_generator_t config;
  // parse command line options
  if (parseOption(argc, argv, config) != 7) {
    usage(stderr);
    return 1;
  }

  gl::RemoteRMATGenerator rmat_generator(config);
  rmat_generator.init();
  rmat_generator.generateEdges();

  return 0;
}
