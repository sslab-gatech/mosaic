#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "reader-base.h"
#endif

#include <sstream>
#include <iostream>
#include <iomanip>
#include <fstream>
#include <algorithm>
#include <stdio.h>
#include <string.h>
#include <util/arch.h>

#define DO_TILE_PROCESSING 1

namespace scalable_graphs {
namespace core {

  template <typename TData, typename TMetaData>
  ReaderBase<TData, TMetaData>::ReaderBase(const thread_index_t& thread_index)
      : thread_index_(thread_index) {}

  template <typename TData, typename TMetaData>
  ReaderBase<TData, TMetaData>::~ReaderBase() {}

  template <typename TData, typename TMetaData>
  size_t
  ReaderBase<TData, TMetaData>::read_a_batch_of_tiles(size_t start_tile_id,
                                                      size_t end_tile_id) {
    // collect the information of tiles
    rdctx_.init();
    for (size_t tile_id = start_tile_id; tile_id < end_tile_id; ++tile_id) {
      rdctx_.add_tile(get_tile_size(tile_id), tile_offsets_[tile_id]);
    }
    rdctx_.close();

    // a batch = [tile]* refcnt
    size_t size_block = rdctx_.total_len_ + sizeof(size_t);

    // allocate space in the local_tiles_rb_
    ring_buffer_req_t tiles_req;
    ring_buffer_put_req_init(&tiles_req, BLOCKING, size_block);
    ring_buffer_put(rb_, &tiles_req);
    sg_rb_check(&tiles_req);

    // read a buldle of tiles from file
    void* bundle_raw = tiles_req.data;
    util::readFileOffset(fd_, bundle_raw, rdctx_.total_len_,
                         rdctx_.start_offset_);

    ring_buffer_elm_set_ready(rb_, tiles_req.data);
#if !DO_TILE_PROCESSING
    ring_buffer_elm_set_done(rb_, tiles_req.data);
#endif
#if DO_TILE_PROCESSING
    smp_wmb();

    // publish the available tile for processing, wait for the table
    // to be empty before updating
    size_t* bundle_refcnt = (size_t*)((uint8_t*)bundle_raw + rdctx_.total_len_);
    *bundle_refcnt = end_tile_id - start_tile_id;

    for (size_t tile_id = start_tile_id, i = 0; tile_id < end_tile_id;
         ++tile_id, ++i) {
      TData* data_block =
          (TData*)((uint8_t*)bundle_raw + rdctx_.tile_offsets_[i]);

      on_before_publish_data(data_block, tile_id);

      pointer_offset_t<TData, TMetaData>* tile_info =
          &tiles_offset_table_.data_info[tile_id];

      // wait until previous tile_data is completely consumed
      while (!smp_cas(&tile_info->meta.data_active, false, true)) {
        pthread_yield();
      }
      tile_info->data = data_block;

      // wait until previsous data is completely consumed
      // while (tile_info->data != NULL) {
      //   pthread_yield();
      //   smp_rmb();
      // }
      // tile_info->data = data_block;

      tile_info->meta.bundle_refcnt = bundle_refcnt;
      tile_info->meta.bundle_raw = bundle_raw;

      tile_info->meta.data_ready = true;

      sg_dbg("TR: Tile %lu ready for consumption (tid: %lu)\n", tile_id,
             thread_index_.id);
    }

    // flush out changes
    smp_wmb();

    // Return bytes read.
    return rdctx_.total_len_;
#endif
  }

  template <typename TData, typename TMetaData>
  size_t ReaderBase<TData, TMetaData>::grab_a_tile(size_t& iteration) {
    uint64_t batch_count = reader_progress_->inc();
    iteration = batch_count / num_batch_per_iter_;

    return (batch_count % num_batch_per_iter_) * tile_batch_size_;
  }
}
}
