#include "ipartition-store.h"

#include <core/datatypes.h>

namespace scalable_graphs {
  namespace graph_load {

    IPartitionStore::IPartitionStore(
        const meta_partition_meta_t& local_partition_info,
        const meta_partition_meta_t& global_partition_info,
        const config_grc_t& config)
        : local_partition_info_(local_partition_info),
          global_partition_info_(global_partition_info), config_(config) {
    }

    IPartitionStore::~IPartitionStore() {
    }
  }
}
