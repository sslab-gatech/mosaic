#include "partition-manager.h"

#include <algorithm>
#include <fstream>

#include <assert.h>
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <cmath>
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

#include "list-partition-store.h"
#include "partition-store.h"

size_t scalable_graphs::graph_load::PartitionManager::count_processed_edges_ =
    0;

namespace core = scalable_graphs::core;

namespace scalable_graphs {
namespace graph_load {

  PartitionManager::PartitionManager(
      const config_grc_t& config,
      const partition_manager_arguments_t& arguments)
      : AbstractPartitionManager(config, arguments) {
    // 8-byte alignment to cater to 24 bytes of payload
    int rc = ring_buffer_create(edges_rb_size_, 8, RING_BUFFER_BLOCKING, NULL,
                                NULL, &edges_rb_);
    if (rc) {
      scalable_graphs::util::die(1);
    }

    // init array:
    count_partition_stores_ =
        (size_t) pow(config_.count_rows_partition_stores_per_partition_manager,
                     2);

    partition_stores_ = new IPartitionStore* [count_partition_stores_];
    for (int i = 0; i < count_partition_stores_; ++i) {
      // calculate meta-partition:
      meta_partition_meta_t local_partition_info =
          core::getLocalPartitionInfoForPartitionStore(config_, i);

      meta_partition_meta_t global_partition_info =
          core::getGlobalPartitionInfoForPartitionStore(
              config, arguments_.meta, local_partition_info, i);

      if (config_.partition_mode == PartitionMode::PM_FileBackedMode) {
        partition_stores_[i] =
            new PartitionStore(local_partition_info,
                               global_partition_info,
                               arguments_.write_request_rb,
                               config_);
      } else if (config_.partition_mode == PartitionMode::PM_InMemoryMode) {
        partition_stores_[i] =
            new ListPartitionStore(
                local_partition_info,
                global_partition_info,
                config_);
      }
    }
  }

  void PartitionManager::cleanupWrite() {
    for (int i = 0; i < count_partition_stores_; ++i) {
      partition_stores_[i]->cleanupWrite();
    }
  }

  void PartitionManager::initWrite() {
    for (int i = 0; i < count_partition_stores_; ++i) {
      partition_stores_[i]->initWrite();
    }
  }

  void PartitionManager::initRead() {
    for (int i = 0; i < count_partition_stores_; ++i) {
      partition_stores_[i]->initRead();
    }
  }

  void PartitionManager::run() {
    ring_buffer_req_t edges_req;
    size_t count = 0;
    while (true) {
      // wait for edges to be sent
      ring_buffer_get_req_init(&edges_req, BLOCKING);
      ring_buffer_get_nolock(edges_rb_, &edges_req);
      sg_rb_check(&edges_req);

      partition_edge_list_t* edge_list = (partition_edge_list_t*) edges_req.data;

      // First check shutdown-indicator.
      if (unlikely(edge_list->shutdown_indicator)) {
        break;
      }

      // Process all edges contained in this edge list:
      for (uint32_t i = 0; i < edge_list->count_edges; ++i) {
        edge_t edge = edge_list->edges[i];

        partition_t index_partition_stores =
            core::getPartitionStoreOfEdge(edge, arguments_.meta);

        size_t offset_partition_stores =
            core::getIndexOfPartitionStore(index_partition_stores, config_);

        partition_stores_[offset_partition_stores]->addEdge(edge);

        ++count;
        if (unlikely(count % 1000000 == 0)) {
          smp_faa(&PartitionManager::count_processed_edges_, 1000000);
          sg_log("Global: %lu local: %lu from PartitionManager %lu\n",
                 PartitionManager::count_processed_edges_, count,
                 arguments_.thread_index.id);
        }
      }

      // clean up edge, done
      ring_buffer_elm_set_done(edges_rb_, edges_req.data);
    }

    sg_log("Shutdown PartitionManager %lu\n", arguments_.thread_index.id);
  }

  partition_edge_t*
  PartitionManager::getPartition(const partition_t& partition) {
    partition_t index_partition_stores =
        core::getPartitionStoreOfPartition(partition, arguments_.meta);

    size_t offset_partition_stores =
        core::getIndexOfPartitionStore(index_partition_stores, config_);

    return partition_stores_[offset_partition_stores]->getPartition(partition);
  }

  size_t PartitionManager::getSize(const partition_t& partition) {
    partition_t index_partition_stores =
        core::getPartitionStoreOfPartition(partition, arguments_.meta);

    size_t offset_partition_stores =
        core::getIndexOfPartitionStore(index_partition_stores, config_);

    return partition_stores_[offset_partition_stores]->getSize(partition);
  }

  PartitionManager::~PartitionManager() {
    ring_buffer_destroy(edges_rb_);
    for (int i = 0; i < count_partition_stores_; ++i) {
      delete partition_stores_[i];
    }
  }
}
}
