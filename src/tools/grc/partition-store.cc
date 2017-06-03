#include "partition-store.h"

#include <inttypes.h>

#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#include <core/datatypes.h>
#include <util/util.h>
#include <core/util.h>

namespace core = scalable_graphs::core;

namespace scalable_graphs {
namespace graph_load {

  PartitionStore::PartitionStore(
      const meta_partition_meta_t& local_partition_info,
      const meta_partition_meta_t& global_partition_info,
      ring_buffer_t* write_request_rb, const config_grc_t& config)
      : IPartitionStore(local_partition_info, global_partition_info,
                        config),
        write_request_rb_(write_request_rb) {
    partitions_ = (partition_edge_compact_t*)calloc(
        PARTITIONS_PER_SPARSE_FILE, sizeof(partition_edge_compact_t));
  }

  void PartitionStore::cleanupWrite() {
    // write to file as well
    size_t bytes_to_write =
        sizeof(partition_edge_compact_t) * PARTITIONS_PER_SPARSE_FILE;
    size_t offset = sizeof(meta_partition_file_info_t);
    util::writeFileOffset(file_, partitions_, bytes_to_write, offset);
  }

  void PartitionStore::initWrite() {
    std::string partition_filename = core::getMetaPartitionFileName(
        config_, global_partition_info_.meta_partition_index);

    file_ = open(partition_filename.c_str(), O_RDWR | O_CREAT, 755);
    if (file_ < 0) {
      sg_err("File couldn't be opened: %s with %s\n",
             partition_filename.c_str(), strerror(errno));
      util::die(1);
    }

    size_t max_file_size = core::getMetaPartitionFileSize();
    int rc = ftruncate(file_, max_file_size);
    if (rc == -1) {
      sg_err("ftruncate failed on %s (%d) with %s\n",
             partition_filename.c_str(), file_, strerror(errno));
      util::die(1);
    }

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
    meta_partition_file_.global_offset.i =
        global_partition_info_.vertex_partition_index_start.i & (~UINT32_MAX);
    meta_partition_file_.global_offset.j =
        global_partition_info_.vertex_partition_index_start.j & (~UINT32_MAX);

    util::writeFileOffset(file_, (void*)&meta_partition_file_,
                          sizeof(meta_partition_file_info_t), 0);
  }

  void PartitionStore::initRead() {
    std::string partition_filename = core::getMetaPartitionFileName(
        config_, global_partition_info_.meta_partition_index);
    file_ = open(partition_filename.c_str(), O_RDONLY);
    if (file_ < 0) {
      sg_err("File couldn't be opened: %s with %s\n",
             partition_filename.c_str(), strerror(errno));
      util::die(1);
    }

    // read meta-info
    util::readFileOffset(file_, &meta_partition_file_,
                         sizeof(meta_partition_file_info_t), 0);

    // read partition-meta-information
    size_t bytes_to_read =
        sizeof(partition_edge_compact_t) * PARTITIONS_PER_SPARSE_FILE;
    size_t offset = sizeof(meta_partition_file_info_t);
    util::readFileOffset(file_, partitions_, bytes_to_read, offset);
  }

  void PartitionStore::addEdge(const edge_t& edge) {
    partition_t internal_partition_of_edge =
        core::getPartitionInsidePartitionStoreOfEdge(edge,
                                                     global_partition_info_);

    size_t partition_index =
        core::getIndexOfPartitionInFile(internal_partition_of_edge);

    assert(partition_index < PARTITIONS_PER_SPARSE_FILE);

    // row-first storage + offset inside partition
    size_t offset = core::getOffsetOfPartition(internal_partition_of_edge);
    size_t offset_edge =
        offset +
        sizeof(local_edge_t) * partitions_[partition_index].count_edges;

    local_edge_t edge_local;
    edge_local.src = edge.src - meta_partition_file_.global_offset.i -
                     (size_t)partitions_[partition_index].partition_offset_src;
    edge_local.tgt = edge.tgt - meta_partition_file_.global_offset.j -
                     (size_t)partitions_[partition_index].partition_offset_tgt;

    // pass edge to the io-threads
    ring_buffer_req_t write_req;
    ring_buffer_put_req_init(&write_req, BLOCKING,
                             sizeof(edge_write_request_t) + sizeof(uint64_t));

    ring_buffer_put(write_request_rb_, &write_req);
    sg_rb_check(&write_req);

    edge_write_request_t* edge_write_request =
        (edge_write_request_t*)write_req.data;

    edge_write_request->fd = file_;
    edge_write_request->edge = edge_local;
    edge_write_request->offset = offset_edge;

    ring_buffer_elm_set_ready(write_request_rb_, write_req.data);

    // increase count for the partition:
    ++partitions_[partition_index].count_edges;
    assert(partitions_[partition_index].count_edges < MAX_EDGES_PER_TILE);

    sg_assert(partitions_[partition_index].count_edges < MAX_EDGES_PER_TILE,
              "max-edges-per-tile");
  }

  partition_edge_t*
  PartitionStore::getPartition(const partition_t& request_partition) {
    partition_t internal_partition =
        core::getPartitionInsidePartitionStoreOfPartition(
            request_partition, global_partition_info_);

    // first read header, then calculate how much more to allocate and read the
    // edges
    size_t partition_offset = core::getOffsetOfPartition(internal_partition);
    size_t partition_index =
        core::getIndexOfPartitionInFile(internal_partition);

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

      util::readFileOffset(file_, partition_compact->edges, size_edge_block,
                           partition_offset);

      // now, convert compact-partition to real edge_t-partition
      size_t size_partition =
          sizeof(partition_edge_t) + sizeof(edge_t) * count_edges;
      partition = (partition_edge_t*)malloc(size_partition);
      partition->count_edges = count_edges;
      partition->edges = get_array(edge_t*,
                                   partition,
                                   sizeof(partition_edge_t));

      for (uint32_t i = 0; i < partition->count_edges; ++i) {
        // Convert local_edge_t to edge_t here, add global id-offset from
        // meta-information of this partition
        partition->edges[i].src =
            (size_t)partition_compact->edges[i].src +
            meta_partition_file_.global_offset.i +
            (size_t)partitions_[partition_index].partition_offset_src;
        partition->edges[i].tgt =
            (size_t)partition_compact->edges[i].tgt +
            meta_partition_file_.global_offset.j +
            (size_t)partitions_[partition_index].partition_offset_tgt;
      }

      free(partition_compact);
    }

    return partition;
  }

  size_t PartitionStore::getSize(const partition_t& partition) {
    partition_t internal_partition =
        core::getPartitionInsidePartitionStoreOfPartition(
            partition, global_partition_info_);

    size_t partition_index =
        core::getIndexOfPartitionInFile(internal_partition);

    uint32_t count_edges = partitions_[partition_index].count_edges;

    return count_edges;
  }

  PartitionStore::~PartitionStore() {
    // now close file, cleanup
    close(file_);
    delete[] partitions_;
  }
}
}
