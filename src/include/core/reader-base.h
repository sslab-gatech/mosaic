#pragma once

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <util/runnable.h>
#include <util/atomic_counter.h>
#include <core/datatypes.h>
#include <core/util.h>
#include <util/read-context.h>

namespace scalable_graphs {
namespace core {

  template <typename TData, typename TMetaData>
  class ReaderBase : public util::Runnable {
  public:
    ReaderBase(const thread_index_t& thread_index);

    ~ReaderBase();

  protected:
    virtual size_t get_tile_size(size_t tile_id) = 0;

    // Is executed for every data block before publishing the data in the offset
    // table.
    virtual void on_before_publish_data(TData* data, const size_t tile_id) = 0;

    // Returns the bytes read.
    size_t read_a_batch_of_tiles(size_t start_tile_id, size_t end_tile_id);

    size_t grab_a_tile(size_t& iteration);

  protected:
    thread_index_t thread_index_;
    util::ReadContext rdctx_;

    // The following members are set by the extending classes and accessed
    // inside the local functions of the ReaderBase.
    size_t* tile_offsets_;

    pointer_offset_table_t<TData, TMetaData> tiles_offset_table_;

    ring_buffer_t* rb_;

    int fd_;

    size_t count_tiles_for_current_mic_;

    size_t num_batch_per_iter_;

    util::AtomicCounter* reader_progress_;

    size_t tile_batch_size_;
  };
}
}

#if !defined(CLANG_COMPLETE_ONLY) && !defined(__JETBRAINS_IDE__)
#include "reader-base.cc"
#endif
