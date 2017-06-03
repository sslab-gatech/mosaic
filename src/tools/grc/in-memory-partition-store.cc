#include "in-memory-partition-store.h"

#include <inttypes.h>

#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#include <core/datatypes.h>
#include <util/util.h>
#include <core/util.h>

namespace core = scalable_graphs::core;

namespace scalable_graphs {
namespace graph_load {

  InMemoryPartitionStore::InMemoryPartitionStore(
      const meta_partition_meta_t& local_partition_info,
      const meta_partition_meta_t& global_partition_info,
      const config_rmat_tiler_t& config)
      : local_partition_info_(local_partition_info),
        global_partition_info_(global_partition_info), config_(config),
        count_edges_(0) {}

  void InMemoryPartitionStore::cleanUpCounting() {
    // write partition-information to disk for usage in tiling-phase
    std::string file_name_partition_info = core::getMetaPartitionInfoFileName(
        config_, global_partition_info_.meta_partition_index);

    size_t size_partition_info =
        PARTITIONS_PER_SPARSE_FILE * sizeof(partition_edge_compact_t);

    util::writeDataToFile(file_name_partition_info, partitions_,
                          size_partition_info);

    free(partitions_);
  }

  void InMemoryPartitionStore::cleanUpTiling() {
    free(partition_offsets_);
    // size_t mmap_size = sizeof(local_edge_t) * count_edges_;
    // munmap(edges_, mmap_size);
    free(edges_);
    free(partitions_);
  }

  void InMemoryPartitionStore::initCounting() {
    size_t size_partition_info =
        PARTITIONS_PER_SPARSE_FILE * sizeof(partition_edge_compact_t);
    partitions_ = (partition_edge_compact_t*)malloc(size_partition_info);

    for (int i = 0; i < PARTITIONS_PER_SPARSE_FILE; ++i) {
      partitions_[i].count_edges = 0;
      // store the lower 32-bits of the vertex-offset for this partition to
      // recreate it later when reading edges from disk
      partition_t partitionInsidePartitionStore =
          core::getPartitionOfIndexInsidePartitionStore(i);
      partition_t global_vertex_partition_start =
          core::getGlobalVertexIdsForPartitionInsidePartitionStore(
              global_partition_info_, partitionInsidePartitionStore);

      partitions_[i].partition_offset_src =
          global_vertex_partition_start.i & (UINT32_MAX);
      partitions_[i].partition_offset_tgt =
          global_vertex_partition_start.j & (UINT32_MAX);
    }

    // calculate and write meta_partition_file-information
    meta_partition_info_.global_offset.i =
        global_partition_info_.vertex_partition_index_start.i & (~UINT32_MAX);
    meta_partition_info_.global_offset.j =
        global_partition_info_.vertex_partition_index_start.j & (~UINT32_MAX);
  }

  void InMemoryPartitionStore::initTiling() {
    std::string file_name_partition_info = core::getMetaPartitionInfoFileName(
        config_, global_partition_info_.meta_partition_index);
    size_t size_partition_info =
        PARTITIONS_PER_SPARSE_FILE * sizeof(partition_edge_compact_t);

    partitions_ = (partition_edge_compact_t*)malloc(size_partition_info);
    partition_offsets_ =
        (size_t*)malloc(sizeof(size_t) * PARTITIONS_PER_SPARSE_FILE);

    util::readDataFromFile(file_name_partition_info, size_partition_info,
                           partitions_);

    // init 0 outside due to offset-calculation being based in i = 1
    partition_offsets_[0] = 0;
    count_edges_ = partitions_[0].count_edges;
    // calculate offsets, global count, then malloc the space needed:
    for (int i = 1; i < PARTITIONS_PER_SPARSE_FILE; ++i) {
      count_edges_ += partitions_[i].count_edges;
      partition_offsets_[i] =
          partition_offsets_[i - 1] + partitions_[i - 1].count_edges;
    }

    // calculate and write meta_partition_file-information
    meta_partition_info_.global_offset.i =
        global_partition_info_.vertex_partition_index_start.i & (~UINT32_MAX);
    meta_partition_info_.global_offset.j =
        global_partition_info_.vertex_partition_index_start.j & (~UINT32_MAX);

    // reset count-edges field as this is needed for adding edges into the
    // edges_-array
    for (int i = 0; i < PARTITIONS_PER_SPARSE_FILE; ++i) {
      partitions_[i].count_edges = 0;
    }

    size_t size_edges = sizeof(local_edge_t) * count_edges_;
    edges_ = (local_edge_t*)malloc(size_edges);

    // std::string filename =
    //     config_.path_to_swap + "swap-" +
    //     std::to_string(global_partition_info_.meta_partition_index.i) + "-" +
    //     std::to_string(global_partition_info_.meta_partition_index.j);

    // int fd = open(filename.c_str(), O_RDWR | O_CREAT);
    // if (fd == -1) {
    //   sg_err("Couldn't open file %s with %s\n", filename.c_str(),
    //          strerror(errno));
    //   util::die(1);
    // }
    // ftruncate(fd, size_edges);
    // edges_ = (local_edge_t*)mmap(NULL, size_edges, PROT_READ | PROT_WRITE,
    //                              MAP_PRIVATE, fd, 0);

    // if (edges_ == MAP_FAILED) {
    //   sg_err("Couldn't mmap %lu bytes for edges with %s\n", size_edges,
    //          strerror(errno));
    //   util::die(1);
    // }
    // close(fd);
    // unlink(filename.c_str());
  }

  void InMemoryPartitionStore::addEdge(const edge_t& edge) {
    partition_t internal_partition_of_edge =
        core::getPartitionInsidePartitionStoreOfEdge(edge,
                                                     global_partition_info_);

    size_t partition_index =
        core::getIndexOfPartitionInFile(internal_partition_of_edge);

    // row-first storage + offset inside partition
    // FIXME: use new offset-format
    size_t edge_offset = partition_offsets_[partition_index] +
                         partitions_[partition_index].count_edges;

    local_edge_t edge_local;
    edge_local.src = edge.src - meta_partition_info_.global_offset.i -
                     (size_t)partitions_[partition_index].partition_offset_src;
    edge_local.tgt = edge.tgt - meta_partition_info_.global_offset.j -
                     (size_t)partitions_[partition_index].partition_offset_tgt;

    edges_[edge_offset] = edge_local;

    // increase count for the partition:
    ++partitions_[partition_index].count_edges;
  }

  void InMemoryPartitionStore::addEdgeCount(const edge_t& edge) {
    partition_t internal_partition_of_edge =
        core::getPartitionInsidePartitionStoreOfEdge(edge,
                                                     global_partition_info_);
    size_t partition_index =
        core::getIndexOfPartitionInFile(internal_partition_of_edge);

    // increase count for the partition:
    ++partitions_[partition_index].count_edges;
  }

  partition_edge_t*
  InMemoryPartitionStore::getPartition(const partition_t& request_partition) {
    partition_t internal_partition =
        core::getPartitionInsidePartitionStoreOfPartition(
            request_partition, global_partition_info_);

    // first read header, then calculate how much more to allocate and read the
    // edges
    size_t partition_index =
        core::getIndexOfPartitionInFile(internal_partition);
    size_t edge_partition_offset = partition_offsets_[partition_index];

    assert(partition_index < PARTITIONS_PER_SPARSE_FILE);

    uint32_t count_edges = partitions_[partition_index].count_edges;
    partition_edge_t* partition = NULL;

    // if partition empty, create dummy-partition, otherwise do actual read
    if (count_edges == 0) {
      partition = (partition_edge_t*)malloc(sizeof(partition_edge_t));
      partition->count_edges = 0;
    } else {
      size_t size_edge_block = sizeof(local_edge_t) * count_edges;
      size_t size_partition_compact =
          sizeof(partition_edge_compact_t) + size_edge_block;

      partition_edge_compact_t* partition_compact =
          (partition_edge_compact_t*)malloc(size_partition_compact);

      memcpy(partition_compact->edges, &edges_[edge_partition_offset],
             size_edge_block);

      // now, convert compact-partition to real edge_t-partition
      size_t size_partition =
          sizeof(partition_edge_t) + sizeof(edge_t) * count_edges;
      partition = (partition_edge_t*)malloc(size_partition);
      partition->count_edges = count_edges;

      for (uint32_t i = 0; i < partition->count_edges; ++i) {
        // Convert local_edge_t to edge_t here, add global id-offset from
        // meta-information of this partition
        partition->edges[i].src =
            (size_t)partition_compact->edges[i].src +
            meta_partition_info_.global_offset.i +
            (size_t)partitions_[partition_index].partition_offset_src;
        partition->edges[i].tgt =
            (size_t)partition_compact->edges[i].tgt +
            meta_partition_info_.global_offset.j +
            (size_t)partitions_[partition_index].partition_offset_tgt;
      }

      free(partition_compact);
    }

    return partition;
  }

  size_t InMemoryPartitionStore::getSize(const partition_t& request_partition) {
    partition_t internal_partition =
        core::getPartitionInsidePartitionStoreOfPartition(
            request_partition, global_partition_info_);

    size_t partition_index =
        core::getIndexOfPartitionInFile(internal_partition);

    assert(partition_index < PARTITIONS_PER_SPARSE_FILE);

    uint32_t count_edges = partitions_[partition_index].count_edges;

    return count_edges;
  }

  InMemoryPartitionStore::~InMemoryPartitionStore() {}
}
}
