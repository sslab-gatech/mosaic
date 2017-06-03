#include "remote-rmat-generator.h"

#include <string.h>

#include <util/util.h>
#include <core/util.h>

#include <ring_buffer_scif.h>
#include <ring_buffer.h>

#include <util/hilbert.h>

#include "rmat-context.h"

namespace scalable_graphs {
namespace graph_load {

  struct RemoteRMATGeneratorInfo {
    pthread_t thr;
    int thread_id;
    RemoteRMATGenerator* parent;
    int64_t start;
    int64_t end;
    int rc;
  };

  RemoteRMATGenerator::RemoteRMATGenerator(
      const config_remote_rmat_generator_t& config)
      : config_(config), a_(RMAT_A), b_(RMAT_B), c_(RMAT_C), seed_(RMAT_SEED),
        count_vertices_(config.count_vertices) {
    pthread_barrier_init(&barrier_, NULL, config.count_threads);
    active_partition_managers_ = new bool[config_.count_partition_managers];

    command_line_args_grc_t args;
    args.count_partition_managers = config_.count_partition_managers;

    core::initGrcConfig(&config_grc_, config.count_vertices, args);
  }

  void RemoteRMATGenerator::init() {
    int port = config_.port;
    sg_log("Init scif-ringbuffer on port %d\n", port);
    int rc = ring_buffer_scif_create_master(
        size_rb_, L1D_CACHELINE_SIZE, RING_BUFFER_BLOCKING,
        RING_BUFFER_SCIF_PRODUCER, NULL, NULL, &rb_);
    if (rc)
      scalable_graphs::util::die(1);
    sg_log("Waiting for connection on port %d\n", port);
    // then, wait for shadow connection asynchronously
    ring_buffer_scif_wait_for_shadow(&rb_, port, 0);

    port += 10;

    sg_log("Init control-scif-ringbuffer on port %d\n", port);
    rc = ring_buffer_scif_create_master(
        size_control_rb_, L1D_CACHELINE_SIZE, RING_BUFFER_BLOCKING,
        RING_BUFFER_SCIF_CONSUMER, NULL, NULL, &control_rb_);
    if (rc)
      scalable_graphs::util::die(1);
    sg_log("Waiting for control-connection on port %d\n", port);
    // then, wait for shadow connection asynchronously
    ring_buffer_scif_wait_for_shadow(&control_rb_, port, 0);
  }

  int RemoteRMATGenerator::generateEdges() {
    int nthread = config_.count_threads;
    size_t count_edges = config_.edges_id_end - config_.edges_id_start;
    int64_t chunk_size = count_edges / nthread; // how many edges to generate
    std::vector<RemoteRMATGeneratorInfo*> threads;
    int rc = 0;

    sg_log(
        "Generating %lu edges with %d threads concurrently, chunksize : %lu \n",
        count_edges, nthread, chunk_size);

    // lanunch parsing threads
    for (int i = 0; i < (nthread - 1); ++i) {
      RemoteRMATGeneratorInfo* ti = new RemoteRMATGeneratorInfo;
      ti->parent = this;
      ti->thread_id = i;
      ti->start = config_.edges_id_start + chunk_size * i;
      ti->end = ti->start + chunk_size;

      int rc =
          pthread_create(&ti->thr, NULL, RemoteRMATGenerator::threadMain, ti);
      threads.push_back(ti);
    }

    // do my job
    size_t start = config_.edges_id_start + chunk_size * (nthread - 1);
    size_t end = config_.edges_id_start + count_edges;
    rc = generateEdgesInRange(nthread, start, end);

    // wait for parsing threads
    for (const auto& it : threads) {
      pthread_join(it->thr, NULL);
      if (it->rc) {
        rc = it->rc;
      }
      delete it;
    }

  err_out:
    return rc;
  }

  void* RemoteRMATGenerator::threadMain(void* args) {
    RemoteRMATGeneratorInfo* ti = static_cast<RemoteRMATGeneratorInfo*>(args);
    ti->rc =
        ti->parent->generateEdgesInRange(ti->thread_id, ti->start, ti->end);
    return NULL;
  }

  void RemoteRMATGenerator::sendEdgeBlock(
      const remote_partition_edge_t* edge_block) {
    size_t size_edge_block = sizeof(remote_partition_edge_t) +
                             sizeof(edge_t) * edge_block->count_edges;
    ring_buffer_req_t edge_block_req;
    ring_buffer_put_req_init(&edge_block_req, BLOCKING, size_edge_block);

    ring_buffer_scif_put(&rb_, &edge_block_req);
    sg_rb_check(&edge_block_req);

    copy_to_ring_buffer_scif(&rb_, edge_block_req.data, edge_block,
                             size_edge_block);

    ring_buffer_scif_elm_set_ready(&rb_, edge_block_req.data);
  }

  int RemoteRMATGenerator::generateEdgesInRange(int tid, int64_t start,
                                                int64_t end) {
    RMATContext rmatContex(a_, b_, c_, count_vertices_, seed_);

    sg_log("Generator started: thread[%d], range[%ld, %ld)\n", tid, start, end);

    remote_partition_edge_t* edge_buffer = (remote_partition_edge_t*)malloc(
        sizeof(remote_partition_edge_t) +
        sizeof(edge_t) * RMAT_GENERATOR_MAX_EDGES_PER_BLOCK);

    bool tile_phase =
        (config_.generator_phase == RmatGeneratorPhase::RGP_GenerateTiles);

    if (tile_phase) {
      // there is no clear end-condition on the remote-side
      for (int round = 0;; ++round) {
        // first, receive the bool-array with the information of which
        // partition-managers are active, do this in single-threaded mode:
        int barrier_rc = pthread_barrier_wait(&barrier_);

        if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
          sg_log("Receive active-array from %d\n", tid);

          ring_buffer_req_t req;
          ring_buffer_get_req_init(&req, BLOCKING);
          ring_buffer_scif_get(&control_rb_, &req);

          sg_rb_check(&req);

          // copy from ring-buffer, set done immediately, don't need to hold the
          // space anymore
          copy_from_ring_buffer_scif(&control_rb_, active_partition_managers_,
                                     req.data, req.size);
          ring_buffer_scif_elm_set_done(&control_rb_, req.data);
        }

        // make sure no thread escapes
        pthread_barrier_wait(&barrier_);

        int count_active_partition_managers = 0;
        for (int i = 0; i < config_.count_partition_managers; ++i) {
          if (active_partition_managers_[i]) {
            ++count_active_partition_managers;
          }
        }

        if (count_active_partition_managers == 0) {
          sg_log("No more partition-managers active in round %d, quit!\n",
                 round);
          break;
        }

        sg_log("Generating edges for round %d at %d\n", round, tid);
        // calculate the target-area along the hilbert-order:

        size_t count_edges_current_block = 0;
        for (uint64_t i = start; i < end; ++i) {
          edge_t edge = rmatContex.getEdge(i);

          partition_t meta_partition =
              core::getPartitionManagerOfEdge(edge, config_grc_);
          int partition_manager_index =
              core::getIndexOfPartitionManager(meta_partition, config_grc_);
          // in the tile-phase, check if the edge is in the target area, discard
          // otherwise
          if (active_partition_managers_[partition_manager_index]) {
            edge_buffer->edges[count_edges_current_block] = edge;
            ++count_edges_current_block;

            if (count_edges_current_block ==
                RMAT_GENERATOR_MAX_EDGES_PER_BLOCK) {
              // transmit the block now:
              edge_buffer->count_edges = count_edges_current_block;
              edge_buffer->end_of_round = false;
              sendEdgeBlock(edge_buffer);
              count_edges_current_block = 0;
            }
          }
        }

        // one final send, if anything to send:
        if (count_edges_current_block > 0) {
          edge_buffer->count_edges = count_edges_current_block;
          edge_buffer->end_of_round = false;
          sendEdgeBlock(edge_buffer);
        }

        // wait for all threads to be done enqueuing, then set this round done
        // with exactly one thread
        barrier_rc = pthread_barrier_wait(&barrier_);

        if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
          sg_log("Reset count from %d for round %d\n", tid, round);

          edge_buffer->count_edges = 0;
          edge_buffer->end_of_round = true;
          sendEdgeBlock(edge_buffer);
        }

        // make sure no thread escapes
        pthread_barrier_wait(&barrier_);
      }
    } else {
      // only one iteration is needed for the degree-phase
      size_t count_edges_current_block = 0;
      for (uint64_t i = start; i < end; ++i) {
        edge_t edge = rmatContex.getEdge(i);

        // in the tile-phase, check if the edge is in the target area, discard
        // if not:
        edge_buffer->edges[count_edges_current_block] = edge;
        ++count_edges_current_block;

        if (count_edges_current_block == RMAT_GENERATOR_MAX_EDGES_PER_BLOCK) {
          // transmit the block now:
          edge_buffer->count_edges = count_edges_current_block;
          edge_buffer->end_of_round = false;
          sendEdgeBlock(edge_buffer);
          count_edges_current_block = 0;
        }
      }

      // one final send, if anything to send:
      edge_buffer->count_edges = count_edges_current_block;
      edge_buffer->end_of_round = false;
      sendEdgeBlock(edge_buffer);

      // wait for all threads to be done enqueuing, then set this round done
      // with exactly one thread
      int barrier_rc = pthread_barrier_wait(&barrier_);

      if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
        sg_log("Reset count from %d for round 0\n", tid);
        edge_buffer->count_edges = 0;
        edge_buffer->end_of_round = true;
        sendEdgeBlock(edge_buffer);
      }

      // make sure no thread escapes
      pthread_barrier_wait(&barrier_);
    }

    free(edge_buffer);

    sg_log2("Done generating edges!\n");
    // FIXME: Proper shutdown-procedure
    while (true) {
      pthread_yield();
    }

    return 0;
  }

  RemoteRMATGenerator::~RemoteRMATGenerator() {}
}
}
