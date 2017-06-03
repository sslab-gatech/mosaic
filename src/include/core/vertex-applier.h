#pragma once

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <cmath>
#include <pthread.h>
#include <sys/stat.h>
#include <util/runnable.h>
#include <util/arch.h>
#include <core/datatypes.h>
#include <core/util.h>

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexDomain;

  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexApplier : public scalable_graphs::util::Runnable {
  public:
    VertexApplier(VertexDomain<APP, TVertexType, TVertexIdType>& ctx,
                  vertex_array_t<TVertexType>* vertices,
                  const thread_index_t& thread_index);

    ~VertexApplier();

  private:
    virtual void run();

    void initLocalActiveTiles();

    void allocate();

    void apply(const size_t offset, const size_t end);

    // Apply the local_active_tiles_ onto the global counterpart.
    void reduceActiveTiles();

  private:
    config_vertex_domain_t config_;
    VertexDomain<APP, TVertexType, TVertexIdType>& ctx_;
    vertex_array_t<TVertexType>* vertices_;
    thread_index_t thread_index_;

    char* local_active_tiles_;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "vertex-applier.cc"
#endif
