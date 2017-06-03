#include <util/read-context.h>

namespace scalable_graphs {
namespace util {
  void ReadContext::init() {
    num_ = 0;
    start_offset_ = 0;
    total_len_ = 0;
  }

  void ReadContext::add_tile(size_t tile_size, size_t tile_offset) {
    sg_assert(num_ < max_batch_size, "overflow!");
    if (num_ == 0) {
      size_t lead = tile_offset & ~TILE_READ_ALIGN_MASK;
      tile_size += lead;
      tile_offsets_[num_] = lead;
      start_offset_ = tile_offset & TILE_READ_ALIGN_MASK;
    } else {
      tile_offsets_[num_] = total_len_;
    }
    total_len_ += tile_size;
    ++num_;
  }

  void ReadContext::close() {
    total_len_ = int_ceil(total_len_, TILE_READ_ALIGN);
  }
}
}
