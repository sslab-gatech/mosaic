#pragma once

#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <core/datatypes.h>
#include <core/util.h>

namespace scalable_graphs {
namespace util {
  class ReadContext {
  private:
    const static size_t max_batch_size = 16;

  public:
    void init();

    void add_tile(size_t tile_size, size_t tile_offset);

    void close();

  public:
    size_t num_;                          // number of tiles
    size_t start_offset_;                 // start offset of the first tile
    size_t total_len_;                    // length of all tiles
    size_t tile_offsets_[max_batch_size]; // array of tile lead bytes
  };
}
}
