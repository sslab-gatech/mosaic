#include "rmat-edge-receiver.h"

#include <assert.h>
#include <string.h>
#include <pthread.h>

#include <util/util.h>
#include <util/hilbert.h>
#include <core/util.h>
#include <core/datatypes.h>

/*add this for time measure*/
namespace scalable_graphs {
namespace graph_load {

  size_t RMATEdgeReceiver::global_received_edges = 0;

  RMATEdgeReceiver::RMATEdgeReceiver(
      const config_rmat_tiler_t& config,
      InMemoryPartitionManager** partition_managers, int node_id)
      : config_(config), partition_managers_(partition_managers),
        node_id_(node_id) {
    // - then, create and connect to remote edges-ring buffer
    int port = config_.base_port + node_id * 100;
    int adjusted_node_id = config.run_on_mic ? node_id + 1 : 0;
    sg_log("Connection to host %d at %d\n", adjusted_node_id, port);

    int rc = ring_buffer_scif_create_shadow(port + 1, adjusted_node_id, port,
                                            NULL, NULL, &receive_rb_);
    if (rc != 0) {
      scalable_graphs::util::die(1);
    }

    sg_log("Connected to %d on port %d!\n", adjusted_node_id, port);
    port += 10;

    sg_log("Control-Connection to host %d at %d\n", adjusted_node_id, port);

    rc = ring_buffer_scif_create_shadow(port + 1, adjusted_node_id, port, NULL,
                                        NULL, &control_rb_);
    if (rc != 0) {
      scalable_graphs::util::die(1);
    }
    sg_log("Control-Connected to %d on port %d!\n", adjusted_node_id, port);

    // only in case of the vertex-degree-phase, we actually need to calloc this:
    if (config_.generator_phase ==
        RmatGeneratorPhase::RGP_GenerateVertexDegrees) {
      vertex_degrees_ = (vertex_degree_t*)calloc(sizeof(vertex_degree_t),
                                                 config_.count_vertices);
    }

    pthread_barrier_init(&receiver_barrier_, NULL, 2);
  }

  void RMATEdgeReceiver::run() {
    ring_buffer_req_t req;
    remote_partition_edge_t* edges = (remote_partition_edge_t*)malloc(
        sizeof(remote_partition_edge_t) +
        sizeof(edge_t) * RMAT_GENERATOR_MAX_EDGES_PER_BLOCK);
    uint64_t edges_per_generator =
        config_.count_edges / config_.count_edge_generators;

    bool tiler_phase =
        (config_.generator_phase == RmatGeneratorPhase::RGP_GenerateTiles);

    partition_edge_t** edges_per_partition_manager =
        new partition_edge_t*[config_.count_partition_managers];
    for (int i = 0; i < config_.count_partition_managers; ++i) {
      edges_per_partition_manager[i] = (partition_edge_t*)malloc(
          sizeof(partition_edge_t) +
          sizeof(edge_t) * RMAT_GENERATOR_MAX_EDGES_PER_BLOCK);
      edges_per_partition_manager[i]->count_edges = 0;
    }

    uint64_t edges_received = 0;
    size_t count_edges = 0;

    if (tiler_phase) {
      bool* active_partition_managers =
          new bool[config_.count_partition_managers];
      // no end-condition, is determined by no partition-manager being active
      for (int round = 0;; ++round) {
        // wait for the tile-manager to give the etart-sign:
        pthread_barrier_wait(&receiver_barrier_);

        // collect statistics on active partition-managers:
        int count_active_partition_managers = 0;

        for (int i = 0; i < config_.count_partition_managers; ++i) {
          active_partition_managers[i] =
              partition_managers_[i]->active_this_round_;
          if (active_partition_managers[i]) {
            ++count_active_partition_managers;
          }
        }

        sg_log("%d partition managers active out of %lu\n",
               count_active_partition_managers,
               config_.count_partition_managers);

        // send this array to the remote-host:
        size_t size_active_block =
            sizeof(bool) * config_.count_partition_managers;
        ring_buffer_req_t send_req;
        ring_buffer_put_req_init(&send_req, BLOCKING, size_active_block);

        ring_buffer_scif_put(&control_rb_, &send_req);
        sg_rb_check(&send_req);

        copy_to_ring_buffer_scif(&control_rb_, send_req.data,
                                 active_partition_managers, size_active_block);

        ring_buffer_scif_elm_set_ready(&control_rb_, send_req.data);

        if (count_active_partition_managers == 0) {
          sg_log("No more partition-managers active in round %d, quit!\n",
                 round);
        }

        sg_log("Start receiving edges for round %d\n", round);

        bool round_end = false;

        for (int i = 0; i < config_.count_partition_managers; ++i) {
          edges_per_partition_manager[i]->count_edges = 0;
        }

        while (!round_end) {
          ring_buffer_get_req_init(&req, BLOCKING);
          ring_buffer_scif_get(&receive_rb_, &req);

          sg_rb_check(&req);

          // copy from ring-buffer, set done immediately, don't need to hold the
          // space anymore
          copy_from_ring_buffer_scif(&receive_rb_, edges, req.data, req.size);
          ring_buffer_scif_elm_set_done(&receive_rb_, req.data);
          edges_received += edges->count_edges;
          count_edges += edges->count_edges;

          round_end = edges->end_of_round;

          if (unlikely(count_edges > 10000000)) {
            smp_faa(&RMATEdgeReceiver::global_received_edges, count_edges);
            sg_log("Received edges: Global: %lu, local %lu\n",
                   RMATEdgeReceiver::global_received_edges, edges_received);
            count_edges = 0;
          }

          // push edges to appropriate partition-manager:
          for (int i = 0; i < edges->count_edges; ++i) {
            partition_t meta_partition =
                core::getPartitionManagerOfEdge(edges->edges[i], this->config_);
            int partition_manager_index =
                core::getIndexOfPartitionManager(meta_partition, this->config_);

            // only process edge if it falls to an active partition-manager
            if (active_partition_managers[partition_manager_index]) {
              edges_per_partition_manager[partition_manager_index]
                  ->edges[edges_per_partition_manager[partition_manager_index]
                              ->count_edges] = edges->edges[i];
              ++edges_per_partition_manager[partition_manager_index]
                    ->count_edges;

              assert(edges_per_partition_manager[partition_manager_index]
                         ->count_edges <= RMAT_GENERATOR_MAX_EDGES_PER_BLOCK);
            }
          }

          for (int i = 0; i < config_.count_partition_managers; ++i) {
            if (edges_per_partition_manager[i]->count_edges == 0) {
              continue;
            }

            // make sure partition-manager that gets edges is active
            assert(active_partition_managers[i]);

            // acquire position in ringbuffer
            size_t size_edges_block =
                sizeof(partition_edge_t) +
                sizeof(edge_t) * edges_per_partition_manager[i]->count_edges;

            ring_buffer_req_t request_edge;
            ring_buffer_put_req_init(&request_edge, BLOCKING,
                                     sizeof(uint64_t) + size_edges_block);
            ring_buffer_put(this->partition_managers_[i]->edges_rb_,
                            &request_edge);
            sg_rb_check(&request_edge);

            // write negative shutdown-indictor to ringbuffer
            uint64_t* shutdown_indicator = (uint64_t*)(request_edge.data);
            *shutdown_indicator = 0;

            // copy edge-block:
            copy_to_ring_buffer(
                this->partition_managers_[i]->edges_rb_,
                (void*)(((uint8_t*)request_edge.data) + sizeof(uint64_t)),
                edges_per_partition_manager[i], size_edges_block);

            // set element ready, done!
            ring_buffer_elm_set_ready(this->partition_managers_[i]->edges_rb_,
                                      request_edge.data);

            // reset count:
            edges_per_partition_manager[i]->count_edges = 0;
          }
        }

        // notify partition-managers of soon-to-be end of current round:
        for (int i = 0; i < config_.count_partition_managers; ++i) {
          if (active_partition_managers[i]) {
            ring_buffer_req_t request_edge;
            ring_buffer_put_req_init(&request_edge, BLOCKING, sizeof(uint64_t));
            ring_buffer_put(this->partition_managers_[i]->edges_rb_,
                            &request_edge);
            sg_rb_check(&request_edge);

            // write shutdown indicator to ringbuffer
            uint64_t* shutdown_indicator = (uint64_t*)(request_edge.data);
            *shutdown_indicator = 1;

            // set element ready, done!
            ring_buffer_elm_set_ready(this->partition_managers_[i]->edges_rb_,
                                      request_edge.data);
          }
        }
      }
    } else {
      sg_log("Start receiving edges for round %d\n", 1);

      bool round_end = false;

      for (int i = 0; i < config_.count_partition_managers; ++i) {
        edges_per_partition_manager[i]->count_edges = 0;
      }

      while (!round_end) {
        ring_buffer_get_req_init(&req, BLOCKING);
        ring_buffer_scif_get(&receive_rb_, &req);

        sg_rb_check(&req);

        // copy from ring-buffer, set done immediately, don't need to hold the
        // space anymore
        copy_from_ring_buffer_scif(&receive_rb_, edges, req.data, req.size);
        ring_buffer_scif_elm_set_done(&receive_rb_, req.data);
        edges_received += edges->count_edges;
        count_edges += edges->count_edges;

        round_end = edges->end_of_round;

        if (unlikely(count_edges > 10000000)) {
          smp_faa(&RMATEdgeReceiver::global_received_edges, count_edges);
          sg_log("Received edges: Global: %lu, local %lu\n",
                 RMATEdgeReceiver::global_received_edges, edges_received);
          count_edges = 0;
        }

        // push edges to appropriate partition-manager:
        for (int i = 0; i < edges->count_edges; ++i) {
          partition_t meta_partition =
              core::getPartitionManagerOfEdge(edges->edges[i], this->config_);
          int partition_manager_index =
              core::getIndexOfPartitionManager(meta_partition, this->config_);

          // only process edge if it falls to an active partition-manager
          edges_per_partition_manager[partition_manager_index]
              ->edges[edges_per_partition_manager[partition_manager_index]
                          ->count_edges] = edges->edges[i];
          ++edges_per_partition_manager[partition_manager_index]->count_edges;

          assert(edges_per_partition_manager[partition_manager_index]
                     ->count_edges <= RMAT_GENERATOR_MAX_EDGES_PER_BLOCK);

          // if in generate-degrees-phase, collect degrees locally:
          ++vertex_degrees_[edges->edges[i].src].out_degree;
          ++vertex_degrees_[edges->edges[i].tgt].in_degree;
        }

        for (int i = 0; i < config_.count_partition_managers; ++i) {
          if (edges_per_partition_manager[i]->count_edges == 0) {
            continue;
          }

          // acquire position in ringbuffer
          size_t size_edges_block =
              sizeof(partition_edge_t) +
              sizeof(edge_t) * edges_per_partition_manager[i]->count_edges;

          ring_buffer_req_t request_edge;
          ring_buffer_put_req_init(&request_edge, BLOCKING,
                                   sizeof(uint64_t) + size_edges_block);
          ring_buffer_put(this->partition_managers_[i]->edges_rb_,
                          &request_edge);
          sg_rb_check(&request_edge);

          // write negative shutdown-indictor to ringbuffer
          uint64_t* shutdown_indicator = (uint64_t*)(request_edge.data);
          *shutdown_indicator = 0;

          // copy edge-block:
          copy_to_ring_buffer(
              this->partition_managers_[i]->edges_rb_,
              (void*)(((uint8_t*)request_edge.data) + sizeof(uint64_t)),
              edges_per_partition_manager[i], size_edges_block);

          // set element ready, done!
          ring_buffer_elm_set_ready(this->partition_managers_[i]->edges_rb_,
                                    request_edge.data);

          // reset count:
          edges_per_partition_manager[i]->count_edges = 0;
        }
      }

      sg_log("Shutdown edge-receiver %d\n", node_id_);
      // notify partition-managers of soon-to-be end of current round:
      for (int i = 0; i < config_.count_partition_managers; ++i) {
        ring_buffer_req_t request_edge;
        ring_buffer_put_req_init(&request_edge, BLOCKING, sizeof(uint64_t));
        ring_buffer_put(this->partition_managers_[i]->edges_rb_, &request_edge);
        sg_rb_check(&request_edge);

        // write shutdown indicator to ringbuffer
        uint64_t* shutdown_indicator = (uint64_t*)(request_edge.data);
        *shutdown_indicator = 1;

        // set element ready, done!
        ring_buffer_elm_set_ready(this->partition_managers_[i]->edges_rb_,
                                  request_edge.data);
      }
    }
  }

  void RMATEdgeReceiver::reduceVertexDegrees(
      vertex_degree_t* global_vertex_degrees) {
    for (size_t i = 0; i < config_.count_vertices; ++i) {
      global_vertex_degrees[i].in_degree += vertex_degrees_[i].in_degree;
      global_vertex_degrees[i].out_degree += vertex_degrees_[i].out_degree;
    }
  }

  RMATEdgeReceiver::~RMATEdgeReceiver() {
    if (config_.generator_phase ==
        RmatGeneratorPhase::RGP_GenerateVertexDegrees) {
      free(vertex_degrees_);
    }
    ring_buffer_scif_destroy_shadow(&receive_rb_);
    ring_buffer_scif_destroy_shadow(&control_rb_);
  }
}
}
