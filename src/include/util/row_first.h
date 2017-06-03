#pragma once

#include <stdint.h>

namespace traversal {
namespace row_first {
  // convert (x,y) to d
  int64_t xy2d(int64_t n, int64_t x, int64_t y);

  // convert d to (x,y)
  void d2xy(int64_t n, int64_t d, int64_t* x, int64_t* y);
}
}
