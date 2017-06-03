#pragma once

#include <ring_buffer.h>
#include <util/runnable.h>
#include <core/datatypes.h>

#define SIZE_RESPONSE_RB 65536

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexDomain;

  template <class APP, typename TVertexType, typename TVertexIdType>
  class GlobalFetcher : public scalable_graphs::util::Runnable {
  public:
    GlobalFetcher(VertexDomain<APP, TVertexType, TVertexIdType>& ctx,
                  vertex_array_t<TVertexType>* vertices,
                  const thread_index_t& thread_index);
    ~GlobalFetcher();

  public:
    ring_buffer_t* request_rb_;

  private:
    virtual void run();

  private:
    VertexDomain<APP, TVertexType, TVertexIdType>& ctx_;
    vertex_array_t<TVertexType>* vertices_;
    thread_index_t thread_index_;

    const static size_t request_rb_size_ = 1ul * GB;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "global-fetcher.cc"
#endif
