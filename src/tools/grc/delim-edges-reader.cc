#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "delim-edges-reader.h"
#endif

#include <assert.h>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string.h>

#include <util/util.h>
#include <core/util.h>
#include <ring_buffer.h>

namespace scalable_graphs {
namespace graph_load {

  template <typename TEdgeType, typename TVertexIdType>
  struct DelimThreadInfo {
    pthread_t thr;
    DelimEdgesReader<TEdgeType, TVertexIdType>* er;
    std::string* out_dir;
    int64_t start;
    int64_t end;
    int rc;
  };

  template <typename TEdgeType, typename TVertexIdType>
  DelimEdgesReader<TEdgeType, TVertexIdType>::DelimEdgesReader(
      const config_partitioner_t& config, PartitionManager** partition_managers)
      : IEdgesReader<TEdgeType, TVertexIdType>(config, partition_managers),
        delim_(config.settings.delimiter) {}

  template <typename TEdgeType, typename TVertexIdType>
  int DelimEdgesReader<TEdgeType, TVertexIdType>::readEdgesInRange(
      const std::string& out_dir, int64_t start, int64_t end) {
    // create IEdgesReaderContext for this execution for caching global values locally
    IEdgesReaderContext<TVertexIdType> ctx(this->config_.count_vertices, this->config_, this->partition_managers_);

    std::string line;
    // open a file
    std::ifstream input(this->config_.source);

    if (!input) {
      sg_err("Failed to open file: %s : %s\n", this->config_.source.c_str(),
             strerror(errno));
      return -errno;
    }

    // set start
    int64_t start_ahead = std::min(start - read_ahead_, 0L);
    if (!start_ahead) {
      // find the first line which just start from the start offset
      input.seekg(start_ahead, input.beg);
      do {
        if (input.tellg() >= start)
          break;
      } while (std::getline(input, line));
    }

    std::string part;
    while ((input.tellg() <= end) && std::getline(input, line)) {
      // parsing a line and put an edge to the corresponding partition
      std::istringstream iss(line);
      std::getline(iss, part, delim_);
      int64_t src = std::stoll(part);
      std::getline(iss, part, delim_);
      int64_t tgt = std::stoll(part);

      this->addEdge(ctx, src, tgt);
    }

    // Finished, send remaining edges.
    ctx.sendEdgesToAllPartitionManagers();

    // reduce vertex-degrees to the global array
    this->reduceVertexDegrees(ctx);

    return 0;
  }

  template <typename TEdgeType, typename TVertexIdType>
  int DelimEdgesReader<TEdgeType, TVertexIdType>::calcProperNumThreads(
      uint64_t file_size, int max_thread) {
    int num_chunk = (file_size + min_chunk_ - 1) / min_chunk_;
    int nthreads = std::min(num_chunk, max_thread);
    nthreads = std::max(
        this->config_.paths_to_partition.size(),
        nthreads - (nthreads % this->config_.paths_to_partition.size()));
    // FIXME: potential bug in multithreaded reading, don't do for now, no
    // speedup anyways
    return 1;
  }

  template <typename TEdgeType, typename TVertexIdType>
  void* DelimEdgesReader<TEdgeType, TVertexIdType>::threadMain(void* arg) {
    DelimThreadInfo<TEdgeType, TVertexIdType>* ti =
        static_cast<DelimThreadInfo<TEdgeType, TVertexIdType>*>(arg);
    ti->rc = ti->er->readEdgesInRange(*ti->out_dir, ti->start, ti->end);
    return NULL;
  }

  template <typename TEdgeType, typename TVertexIdType>
  int DelimEdgesReader<TEdgeType, TVertexIdType>::readEdges(int max_thread) {
    uint64_t file_size = util::getFileSize(this->config_.source);
    int nthread = calcProperNumThreads(file_size, max_thread);
    int64_t chunk_size = file_size / nthread;
    int threads_per_dir = nthread / this->config_.paths_to_partition.size();
    std::vector<DelimThreadInfo<TEdgeType, TVertexIdType>*> threads;
    int rc = 0;

    sg_log("Reading edges with %d threads concurrently using %lu "
           "partition-managers (fz: %lu).\n",
           nthread, this->config_.count_partition_managers, file_size);

    // lanunch parsing threads
    for (int i = 0; i < (nthread - 1); ++i) {
      DelimThreadInfo<TEdgeType, TVertexIdType>* ti =
          new DelimThreadInfo<TEdgeType, TVertexIdType>;
      ti->out_dir = &this->config_.paths_to_partition[i / threads_per_dir];
      ti->er = this;
      ti->start = chunk_size * i;
      ti->end = ti->start + chunk_size;

      int rc = pthread_create(&ti->thr, NULL, DelimEdgesReader::threadMain, ti);
      threads.push_back(ti);
    }

    // do my job
    rc = readEdgesInRange(this->config_.paths_to_partition.back(),
                          chunk_size * (nthread - 1), file_size);

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
  DelimEdgesReader<TEdgeType, TVertexIdType>::~DelimEdgesReader() {}
}
}
