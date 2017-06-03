#pragma once

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <util/runnable.h>
#include <core/datatypes.h>
#include <core/util.h>
#include <core/reader-base.h>

namespace scalable_graphs {
namespace core {
  template <class APP, typename TVertexType, bool is_weighted>
  class EdgeProcessor;

  template <class APP, typename TVertexType, bool is_weighted>
  class TileReader : public ReaderBase<edge_block_t, tile_data_edge_engine_t> {
  public:
    TileReader(EdgeProcessor<APP, TVertexType, is_weighted>& ctx,
               const thread_index_t& thread_index, pthread_barrier_t* barrier);
    ~TileReader();

    virtual void run();

  protected:
    virtual size_t get_tile_size(size_t tile_id);

    virtual void on_before_publish_data(edge_block_t* data,
                                        const size_t tile_id);

    void publish_perfmon(size_t start_tile_id, size_t end_tile_id,
                         size_t bytes_read);

  private:
    const config_edge_processor_t config_;

    pthread_barrier_t* barrier_;

    EdgeProcessor<APP, TVertexType, is_weighted>& ctx_;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "tile-reader.cc"
#endif
