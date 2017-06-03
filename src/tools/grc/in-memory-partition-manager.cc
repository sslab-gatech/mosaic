#include "in-memory-partition-manager.h"

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

namespace core = scalable_graphs::core;

namespace scalable_graphs {
namespace graph_load {

  InMemoryPartitionManager::InMemoryPartitionManager(
      const config_rmat_tiler_t& config,
      const partition_manager_arguments_t& arguments)
      : AbstractPartitionManager(config, arguments), config_tiler_(config),
        count_edges_(0), active_this_round_(false) {
    // 8-byte alignment to cater to 24 bytes of payload
    int rc = ring_buffer_create(edges_rb_size_, L1D_CACHELINE_SIZE,
                                RING_BUFFER_BLOCKING, NULL, NULL, &edges_rb_);
    if (rc) {
      scalable_graphs::util::die(1);
    }

    // init array:
    count_partition_stores_ =
        pow(config_.count_rows_partition_stores_per_partition_manager, 2);

    partition_stores_ = new InMemoryPartitionStore*[count_partition_stores_];
    for (int i = 0; i < count_partition_stores_; ++i) {
      // calculate meta-partition:
      meta_partition_meta_t local_partition_info =
          core::getLocalPartitionInfoForPartitionStore(config_, i);

      meta_partition_meta_t global_partition_info =
          core::getGlobalPartitionInfoForPartitionStore(
              config, arguments_.meta, local_partition_info, i);

      partition_stores_[i] = new InMemoryPartitionStore(
          local_partition_info, global_partition_info, config_tiler_);
    }
  }

  void InMemoryPartitionManager::initRead() {}

  void InMemoryPartitionManager::initCounting() {
    for (int i = 0; i < count_partition_stores_; ++i) {
      partition_stores_[i]->initCounting();
    }
    active_this_round_ = true;
  }

  void InMemoryPartitionManager::initTiling() {
    for (int i = 0; i < count_partition_stores_; ++i) {
      partition_stores_[i]->initTiling();
      count_edges_ += partition_stores_[i]->count_edges_;
    }
    active_this_round_ = true;
  }

  void InMemoryPartitionManager::cleanUpCouting() {
    for (int i = 0; i < count_partition_stores_; ++i) {
      partition_stores_[i]->cleanUpCounting();
      delete partition_stores_[i];
    }
    active_this_round_ = false;
    ring_buffer_destroy(edges_rb_);
  }

  void InMemoryPartitionManager::cleanUpTiling() {
    for (int i = 0; i < count_partition_stores_; ++i) {
      partition_stores_[i]->cleanUpTiling();
      delete partition_stores_[i];
    }
    delete[] partition_stores_;
    active_this_round_ = false;
    ring_buffer_destroy(edges_rb_);
  }

  void InMemoryPartitionManager::run() {
    ring_buffer_req_t edges_req;
    bool tiler_phase = (config_tiler_.generator_phase ==
                        RmatGeneratorPhase::RGP_GenerateTiles);

    sg_log("Init PartitionManager %lu\n", arguments_.thread_index.id);

    // we expect a shutdown from every edge-generator, wait for all of them:
    int shutdowns_received = 0;
    int shutdowns_expected = config_tiler_.count_edge_generators;

    while (true) {
      // wait for edges to be sent
      ring_buffer_get_req_init(&edges_req, BLOCKING);
      ring_buffer_get_nolock(edges_rb_, &edges_req);
      sg_rb_check(&edges_req);

      // first check shutdown-indicator which is located just after the
      // edge-struct
      uint64_t shutdown_indicator = ((uint64_t*)((uint8_t*)edges_req.data))[0];
      if (unlikely(shutdown_indicator != 0)) {
        ++shutdowns_received;
        ring_buffer_elm_set_done(edges_rb_, edges_req.data);
        if (shutdowns_received == shutdowns_expected) {
          break;
        }
        // nothing else to be done here, skip loop
        continue;
      }

      // edge-partition located behind the shutdown-indicator:
      partition_edge_t* edges =
          (partition_edge_t*)((uint8_t*)edges_req.data + sizeof(uint64_t));

      for (uint32_t i = 0; i < edges->count_edges; ++i) {
        partition_t index_partition_stores =
            core::getPartitionStoreOfEdge(edges->edges[i], arguments_.meta);

        size_t offset_partition_stores =
            core::getIndexOfPartitionStore(index_partition_stores, config_);

        if (tiler_phase) {
          partition_stores_[offset_partition_stores]->addEdge(edges->edges[i]);
        } else {
          partition_stores_[offset_partition_stores]->addEdgeCount(
              edges->edges[i]);
        }
      }

      // clean up edges, done
      ring_buffer_elm_set_done(edges_rb_, edges_req.data);
    }

    sg_log("Shutdown PartitionManager %lu\n", arguments_.thread_index.id);
  }

  void InMemoryPartitionManager::addEdge(const edge_t& edge) {
    partition_t index_partition_stores =
        core::getPartitionStoreOfEdge(edge, arguments_.meta);

    size_t offset_partition_stores =
        core::getIndexOfPartitionStore(index_partition_stores, config_);

    partition_stores_[offset_partition_stores]->addEdge(edge);
  }

  void InMemoryPartitionManager::addEdgeCount(const edge_t& edge) {
    partition_t index_partition_stores =
        core::getPartitionStoreOfEdge(edge, arguments_.meta);

    size_t offset_partition_stores =
        core::getIndexOfPartitionStore(index_partition_stores, config_);

    partition_stores_[offset_partition_stores]->addEdgeCount(edge);
  }

  partition_edge_t*
  InMemoryPartitionManager::getPartition(const partition_t& partition) {
    partition_t index_partition_stores =
        core::getPartitionStoreOfPartition(partition, arguments_.meta);

    size_t offset_partition_stores =
        core::getIndexOfPartitionStore(index_partition_stores, config_);

    return partition_stores_[offset_partition_stores]->getPartition(partition);
  }

  size_t InMemoryPartitionManager::getSize(const partition_t& partition) {
    partition_t index_partition_stores =
        core::getPartitionStoreOfPartition(partition, arguments_.meta);

    size_t offset_partition_stores =
        core::getIndexOfPartitionStore(index_partition_stores, config_);

    return partition_stores_[offset_partition_stores]->getSize(partition);
  }

  InMemoryPartitionManager::~InMemoryPartitionManager() {}
}
}
