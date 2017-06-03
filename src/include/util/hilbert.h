#pragma once

#include <stdint.h>

namespace traversal {
namespace hilbert {
  void rot(int64_t n, int64_t* x, int64_t* y, int64_t rx, int64_t ry);

  // convert (x,y) to d
  int64_t xy2d(int64_t n, int64_t x, int64_t y);

  // convert d to (x,y)
  void d2xy(int64_t n, int64_t d, int64_t* x, int64_t* y);
}
}
