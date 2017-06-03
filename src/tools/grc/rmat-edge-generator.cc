#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "rmat-edge-generator.h"
#endif

#include <string.h>

#include <util/util.h>
#include <core/util.h>

#include "rmat-context.h"

#define DEBUG_TIME_MEASURE 0

/*add this for time measure*/
namespace scalable_graphs {
namespace graph_load {

  template <typename TEdgeType, typename TVertexIdType>
  struct RMATEdgeGeneratorInfo {
    pthread_t thr;
    int thread_id;
    RMATEdgeGenerator<TEdgeType, TVertexIdType>* eg;
    int64_t start;
    int64_t end;
    int rc;
  };

  template <typename TEdgeType, typename TVertexIdType>
  RMATEdgeGenerator<TEdgeType, TVertexIdType>::RMATEdgeGenerator(
      const config_partitioner_t& config, PartitionManager** partition_managers)
      : IEdgesReader<TEdgeType, TVertexIdType>(config, partition_managers),
        a_(RMAT_A), b_(RMAT_B), c_(RMAT_C), seed_(RMAT_SEED),
        count_vertices_(config.settings.count_vertices),
        count_edges_(config.settings.rmat_count_edges) {}
#if 0
  template <typename TEdgeType, typename TVertexIdType>
  int RMATEdgeGenerator<TEdgeType, TVertexIdType>::readEdges(int max_thread) {
    IEdgesReaderContext<TVertexIdType> ctx(this->config_.count_vertices);

    RMATContext rmatContex(a_, b_, c_, count_vertices_, seed_);
		
		uint64_t i;
		edge_t edge;
		for (i = 0; i < count_edges_; ++i) {
			edge = rmatContex.getEdge(i);
			this->addEdge(ctx, edge.src, edge.tgt);
		}


    // reduce vertex-degrees to the global array
    this->reduceVertexDegrees(ctx);

    this->writeGlobalFiles();

    return 0;
  }
#else
  template <typename TEdgeType, typename TVertexIdType>
  void* RMATEdgeGenerator<TEdgeType, TVertexIdType>::threadMain(void* arg) {
    RMATEdgeGeneratorInfo<TEdgeType, TVertexIdType>* ti =
        static_cast<RMATEdgeGeneratorInfo<TEdgeType, TVertexIdType>*>(arg);
    ti->rc = ti->eg->readEdgesInRange(ti->thread_id, ti->start, ti->end);
    return NULL;
  }

  template <typename TEdgeType, typename TVertexIdType>
  int RMATEdgeGenerator<TEdgeType, TVertexIdType>::readEdges(int max_thread) {
    int nthread = max_thread;
    int64_t chunk_size = count_edges_ / nthread; /*how many edges to generate*/
    std::vector<RMATEdgeGeneratorInfo<TEdgeType, TVertexIdType>*> threads;
    int rc = 0;

    sg_log("Generating edges with %d threads concurrently chunksize : %lu \n",
           nthread, chunk_size);

    // lanunch parsing threads
    for (int i = 0; i < (nthread - 1); ++i) {
      RMATEdgeGeneratorInfo<TEdgeType, TVertexIdType>* ti =
          new RMATEdgeGeneratorInfo<TEdgeType, TVertexIdType>;
      ti->eg = this;
      ti->thread_id = i;
      ti->start = chunk_size * i;
      ti->end = ti->start + chunk_size;

      int rc =
          pthread_create(&ti->thr, NULL, RMATEdgeGenerator::threadMain, ti);
      threads.push_back(ti);
    }

    // do my job
    rc = readEdgesInRange((int)nthread, chunk_size * (nthread - 1),
                          count_edges_);

    // wait for parsing threads
    for (const auto& it : threads) {
      pthread_join(it->thr, NULL);
      if (it->rc) {
        rc = it->rc;
      }
      delete it;
    }

    this->writeGlobalFiles();

  err_out:
    return rc;
  }

  template <typename TEdgeType, typename TVertexIdType>
  int RMATEdgeGenerator<TEdgeType, TVertexIdType>::readEdgesInRange(
      int tid, int64_t start, int64_t end)
  /*start </-in>: start index of generator
          end  </-in> : end of index of generator
          --------------------------------------
          return 0 on success, otherwise non-determine */
  {
    uint64_t i;
    edge_t edge;

#if DEBUG_TIME_MEASURE
    /*time measure*/
    uint64_t t_get_total = 0, t_get_start = 0, t_get_end = 0;
    uint64_t t_add_total = 0, t_add_start = 0, t_add_end = 0;
#endif

    // create IEdgesReaderContext for this execution for caching global values locally
    IEdgesReaderContext<TVertexIdType> ctx(this->config_.count_vertices, this->config_, this->partition_managers_);

    /*FIXME : this might need to execute only once*/
    RMATContext rmatContex(a_, b_, c_, count_vertices_, seed_);

    sg_log("Generator Started thread[%d], range[%ld, %ld)\n", tid, start, end);

#if DEBUG_TIME_MEASURE
    uint64_t j = 0;

    for (i = start; i < end; ++i) {
      t_get_start = rdtsc();
      edge = rmatContex.getEdge(i);
      t_get_end = rdtsc();

      t_add_start = rdtsc();
      this->addEdge(ctx, edge.src, edge.tgt);
      t_add_end = rdtsc();

      t_get_total += t_get_end - t_get_start;
      t_add_total += t_add_end - t_add_start;

      /*status printing */
      if (__builtin_expect((++j) % 1000000 == 0, 0)) {
        sg_log("INFO : thread[%d]- %ld generated, t_get:%ld/s, t_add:%ld/s\n",
               tid, j, t_get_total / CLOCKS_PER_SEC,
               t_add_total / CLOCKS_PER_SEC);
        t_get_total = 0;
        t_add_total = 0;
      }
    }
#else
    uint64_t j = 0;

    for (i = start; i < end; ++i) {
      edge = rmatContex.getEdge(i);

      this->addEdge(ctx, edge.src, edge.tgt);

      /*status printing */
      if (__builtin_expect((++j) % 1000000 == 0, 0)) {
        sg_log("INFO : thread[%d]- %ld generated\n", tid, j);
      }
    }

    // Finished, send remaining edges.
    ctx.sendEdgesToAllPartitionManagers();
#endif

    // reduce vertex-degrees to the global array
    this->reduceVertexDegrees(ctx);

    return 0;
  }
#endif

  template <typename TEdgeType, typename TVertexIdType>
  RMATEdgeGenerator<TEdgeType, TVertexIdType>::~RMATEdgeGenerator() {}
}
}
