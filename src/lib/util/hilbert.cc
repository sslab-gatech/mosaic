#include <util/hilbert.h>

namespace traversal {
namespace hilbert {
  void rot(int64_t n, int64_t* x, int64_t* y, int64_t rx, int64_t ry) {
    if (ry == 0) {
      if (rx == 1) {
        *x = n - 1 - *x;
        *y = n - 1 - *y;
      }

      // swap x, y
      *x = *x + *y;
      *y = *x - *y;
      *x = *x - *y;
    }
  }

  // convert (x,y) to d
  int64_t xy2d(int64_t n, int64_t x, int64_t y) {
    int64_t rx, ry, s, d = 0;

    for (s = n / 2; s > 0; s /= 2) {
      rx = (x & s) > 0;
      ry = (y & s) > 0;
      d += s * s * ((3 * rx) ^ ry);
      rot(s, &x, &y, rx, ry);
    }

    return d;
  }

  // convert d to (x,y)
  void d2xy(int64_t n, int64_t d, int64_t* x, int64_t* y) {
    int64_t rx, ry, s, t = d;
    *x = *y = 0;

    for (s = 1; s < n; s *= 2) {
      rx = 1 & (t / 2);
      ry = 1 & (t ^ rx);
      rot(s, x, y, rx, ry);
      *x += s * rx;
      *y += s * ry;
      t /= 4;
    }
  }
}
}
