#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)

#include "iedges-reader-context.h"

#endif

#include <fstream>
#include <string.h>

#include <util/util.h>
#include <core/util.h>

#define EDGES_BATCHING 4096

namespace scalable_graphs {
  namespace graph_load {

    template<typename TVertexIdType>
    IEdgesReaderContext<TVertexIdType>::IEdgesReaderContext(const uint64_t count_vertices,
                                                            const config_partitioner_t& config,
                                                            PartitionManager** partition_managers)
        : partition_managers_(partition_managers), config_(config) {
      vertex_degrees_ = (vertex_degree_t*) calloc(1, sizeof(vertex_degree_t) * count_vertices);
      edges_for_partition_managers_ = new partition_edge_list_t* [config_.count_partition_managers];

      for (int i = 0; i < config_.count_partition_managers; ++i) {
        size_t size_edges_block = sizeof(partition_edge_list_t) + sizeof(edge_t) * EDGES_BATCHING;
        edges_for_partition_managers_[i] = (partition_edge_list_t*) malloc(size_edges_block);
        edges_for_partition_managers_[i]->count_edges = 0;
        edges_for_partition_managers_[i]->shutdown_indicator = false;
      }
    }

    template<typename TVertexIdType>
    void IEdgesReaderContext<TVertexIdType>::addEdge(const edge_t& edge) {
      partition_t meta_partition =
          core::getPartitionManagerOfEdge(edge, this->config_);
      int partition_manager_index = core::getIndexOfPartitionManager(meta_partition, this->config_);

      partition_edge_list_t* edge_list = edges_for_partition_managers_[partition_manager_index];

      uint32_t edges_index = edge_list->count_edges;
      edge_list->edges[edges_index] = edge;
      edge_list->count_edges++;

      if (edge_list->count_edges >= EDGES_BATCHING) {
        this->sendEdgesToPartitionManager(partition_manager_index);
        edge_list->count_edges = 0;
      }
    }

    template<typename TVertexIdType>
    void IEdgesReaderContext<TVertexIdType>::sendEdgesToPartitionManager(const int manager) {
      partition_edge_list_t* edge_list = edges_for_partition_managers_[manager];

      // Copy edge list into ringbuffer.
      size_t size_edges = sizeof(edge_t) * edge_list->count_edges;
      size_t size_edge_list = sizeof(partition_edge_list_t) + size_edges;

      // Acquire position in ringbuffer.
      ring_buffer_req_t request_edge;
      ring_buffer_put_req_init(&request_edge, BLOCKING, size_edge_list);
      ring_buffer_put(this->partition_managers_[manager]->edges_rb_, &request_edge);
      sg_rb_check(&request_edge);

      partition_edge_list_t* edge_list_in_rb = (partition_edge_list_t*) request_edge.data;
      edge_list_in_rb->shutdown_indicator = false;
      edge_list_in_rb->count_edges = edge_list->count_edges;
      memcpy(edge_list_in_rb->edges, edge_list->edges, size_edges);

      // set element ready, done!
      ring_buffer_elm_set_ready(this->partition_managers_[manager]->edges_rb_, request_edge.data);
    }

    template<typename TVertexIdType>
    void IEdgesReaderContext<TVertexIdType>::sendEdgesToAllPartitionManagers() {
      for (int i = 0; i < config_.count_partition_managers; ++i) {
        this->sendEdgesToPartitionManager(i);
      }
    }

    template<typename TVertexIdType>
    IEdgesReaderContext<TVertexIdType>::~IEdgesReaderContext() {
      free(vertex_degrees_);
      delete[] edges_for_partition_managers_;
    }
  }
}
