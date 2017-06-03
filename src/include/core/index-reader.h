#pragma once

#include <unordered_map>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <util/runnable.h>
#include <core/datatypes.h>
#include <core/util.h>
#include <core/reader-base.h>

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, typename TVertexIdType>
  class VertexProcessor;

  template <class APP, typename TVertexType, typename TVertexIdType>
  class IndexReader
      : public ReaderBase<edge_block_index_t, tile_data_vertex_engine_t> {
  public:
    IndexReader(VertexProcessor<APP, TVertexType, TVertexIdType>& ctx,
                const thread_index_t& thread_index);

    ~IndexReader();

    virtual void run();

  protected:
    virtual void on_before_publish_data(edge_block_index_t* data,
                                        const size_t tile_id);

    void initDataFromParent();

    virtual size_t get_tile_size(size_t tile_id);

  private:
    config_vertex_domain_t config_;

    VertexProcessor<APP, TVertexType, TVertexIdType>& ctx_;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "index-reader.cc"
#endif
