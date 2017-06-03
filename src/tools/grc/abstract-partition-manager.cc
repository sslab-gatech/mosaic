#include "abstract-partition-manager.h"

#include <assert.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <util/hilbert.h>
#include <core/datatypes.h>
#include <util/util.h>
#include <util/arch.h>
#include <core/util.h>

namespace core = scalable_graphs::core;

namespace scalable_graphs {
namespace graph_load {

  AbstractPartitionManager::AbstractPartitionManager(
      const config_grc_t& config,
      const partition_manager_arguments_t& arguments)
      : config_(config), arguments_(arguments) {}

  AbstractPartitionManager::~AbstractPartitionManager() {}
}
}
