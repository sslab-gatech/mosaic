#include "list-partition-store.h"

#include <assert.h>

#include <util/util.h>
#include <core/util.h>

namespace core = scalable_graphs::core;

namespace scalable_graphs {
  namespace graph_load {

    ListPartitionStore::ListPartitionStore(
        const meta_partition_meta_t& local_partition_info,
        const meta_partition_meta_t& global_partition_info,
        const config_grc_t& config)
        : IPartitionStore(local_partition_info, global_partition_info,
                          config) {
      partitions_ = new std::vector<edge_t>** [PARTITION_COLS_PER_SPARSE_FILE];

      for (int i = 0; i < PARTITION_COLS_PER_SPARSE_FILE; ++i) {
        partitions_[i] = new std::vector<edge_t>* [PARTITION_COLS_PER_SPARSE_FILE];
        for (int j = 0; j < PARTITION_COLS_PER_SPARSE_FILE; ++j) {
          partitions_[i][j] = new std::vector<edge_t>;
        }
      }
    }

    void ListPartitionStore::cleanupWrite() {
      // Nothing to do, in memory data structure.
    }

    void ListPartitionStore::initWrite() {
      // Nothing to do, in memory data structure.
    }

    void ListPartitionStore::initRead() {
      // Nothing to do, in memory data structure.
    }

    void ListPartitionStore::addEdge(const edge_t& edge) {
      partition_t internal_partition =
          core::getPartitionInsidePartitionStoreOfEdge(edge,
                                                       global_partition_info_);

      partitions_[internal_partition.i][internal_partition.j]->push_back(edge);
    }

    partition_edge_t*
    ListPartitionStore::getPartition(const partition_t& request_partition) {
      partition_t internal_partition =
          core::getPartitionInsidePartitionStoreOfPartition(
              request_partition, global_partition_info_);

      std::vector<edge_t>* edges = partitions_[internal_partition.i][internal_partition.j];

      partition_edge_t* partition = (partition_edge_t*) malloc(sizeof(partition_edge_t));
      partition->count_edges = (uint32_t) edges->size();
      partition->edges = edges->data();

      return partition;
    }

    size_t
    ListPartitionStore::getSize(const partition_t& partition) {
      partition_t internal_partition =
          core::getPartitionInsidePartitionStoreOfPartition(
              partition, global_partition_info_);

      std::vector<edge_t>* edges = partitions_[internal_partition.i][internal_partition.j];

      return edges->size();
    }

    ListPartitionStore::~ListPartitionStore() {
      for (int i = 0; i < PARTITION_COLS_PER_SPARSE_FILE; ++i) {
        for (int j = 0; j < PARTITION_COLS_PER_SPARSE_FILE; ++j) {
          delete partitions_[i][j];
        }
        delete[] partitions_[i];
      }
      delete[] partitions_;
    }
  }
}
