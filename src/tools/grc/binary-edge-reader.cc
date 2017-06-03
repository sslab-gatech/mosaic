#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)

#include "binary-edge-reader.h"

#endif

#include <fstream>
#include <sstream>

#include <util/util.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

namespace scalable_graphs {
  namespace graph_load {

    template<typename TEdgeType, typename TVertexIdType>
    struct BinaryThreadInfo {
      pthread_t thr;
      BinaryEdgeReader<TEdgeType, TVertexIdType>* er;
      TEdgeType* edges;
      uint64_t start;
      uint64_t end;
      int rc;
    };

    template<typename TEdgeType, typename TVertexIdType>
    BinaryEdgeReader<TEdgeType, TVertexIdType>::BinaryEdgeReader(
        const config_partitioner_t& config,
        PartitionManager** partition_managers)
        : IEdgesReader<TEdgeType, TVertexIdType>(config, partition_managers) {}

    template<typename TEdgeType, typename TVertexIdType>
    int BinaryEdgeReader<TEdgeType, TVertexIdType>::readEdgesInRange(
        uint64_t start,
        uint64_t end,
        TEdgeType* edges) {
      // create IEdgesReaderContext for this execution for caching global values locally
      IEdgesReaderContext<TVertexIdType> ctx(this->config_.count_vertices, this->config_, this->partition_managers_);

      for (uint64_t edge_index = start; edge_index < end; ++edge_index) {
        TEdgeType edge = edges[edge_index];

        this->addEdge(ctx, edge.src, edge.tgt);
      }

      // Finished, send remaining edges.
      ctx.sendEdgesToAllPartitionManagers();

      // reduce vertex-degrees to the global array
      this->reduceVertexDegrees(ctx);

      return 0;
    }

    template<typename TEdgeType, typename TVertexIdType>
    int BinaryEdgeReader<TEdgeType, TVertexIdType>::calcProperNumThreads(
        uint64_t file_size,
        uint32_t max_thread) {
      uint32_t num_chunk = (uint32_t) ((file_size + min_chunk_ - 1) /
                                       min_chunk_);
      uint32_t nthreads = std::min(num_chunk, max_thread);

      return 8;
    }

    template<typename TEdgeType, typename TVertexIdType>
    void* BinaryEdgeReader<TEdgeType, TVertexIdType>::threadMain(void* arg) {
      BinaryThreadInfo<TEdgeType, TVertexIdType>* ti =
          static_cast<BinaryThreadInfo<TEdgeType, TVertexIdType>*>(arg);

      ti->rc = ti->er->readEdgesInRange(ti->start, ti->end, ti->edges);

      return NULL;
    }

    template<typename TEdgeType, typename TVertexIdType>
    int
    BinaryEdgeReader<TEdgeType, TVertexIdType>::readEdges(int max_thread) {
      int fd = open(this->config_.source.c_str(), O_RDONLY);
      struct stat file_stats;
      fstat(fd, &file_stats);
      size_t file_size = (size_t) file_stats.st_size;

      TEdgeType* input_file = (TEdgeType*)
          mmap(NULL,
               file_size,
               PROT_READ,
               MAP_PRIVATE,
               fd,
               0);

      if (input_file == MAP_FAILED) {
        sg_err("Could not open file %s: %d\n",
               this->config_.source.c_str(),
               errno);
      }

      uint64_t count_edges = file_size / sizeof(TEdgeType);

      int n_thread = calcProperNumThreads(file_size, (uint32_t) max_thread);
      uint64_t chunk_size = count_edges / n_thread;
      std::vector<BinaryThreadInfo<TEdgeType, TVertexIdType>*> threads;

      sg_log("Reading %lu edges with %d threads concurrently using %lu "
                 "partition-managers (filesize: %lu)\n", count_edges,
             n_thread, this->config_.count_partition_managers, file_size);

      // lanunch parsing threads
      for (int i = 0; i < (n_thread - 1); ++i) {
        BinaryThreadInfo<TEdgeType, TVertexIdType>* ti =
            new BinaryThreadInfo<TEdgeType, TVertexIdType>;
        ti->er = this;
        ti->start = chunk_size * i;
        ti->end = ti->start + chunk_size;
        ti->edges = input_file;

        pthread_create(&ti->thr,
                       NULL,
                       BinaryEdgeReader::threadMain,
                       ti);
        threads.push_back(ti);
      }

      // do my job
      int rc = readEdgesInRange(chunk_size * (n_thread - 1),
                                count_edges,
                                input_file);

      // wait for parsing threads
      for (const auto& it : threads) {
        pthread_join(it->thr, NULL);
        if (it->rc) {
          rc = it->rc;
        }
        delete it;
      }
      this->writeGlobalFiles();

      return rc;
    }

    template<typename TEdgeType, typename TVertexIdType>
    BinaryEdgeReader<TEdgeType, TVertexIdType>::~BinaryEdgeReader() {}
  }
}
