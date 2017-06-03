#include "gtest/gtest.h"
#include <util/hilbert.h>
#include <util/column_first.h>
#include <util/row_first.h>

TEST(TraversalTest, Hilbert_xy2d) {
  int64_t n = 4;
  int64_t x, y;

  x = 0;
  y = 0;
  int64_t d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(0, d);

  x = 1;
  y = 0;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(1, d);

  x = 1;
  y = 1;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(2, d);

  x = 0;
  y = 1;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(3, d);

  x = 0;
  y = 2;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(4, d);

  x = 0;
  y = 3;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(5, d);

  x = 1;
  y = 3;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(6, d);

  x = 1;
  y = 2;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(7, d);

  x = 2;
  y = 2;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(8, d);

  x = 2;
  y = 3;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(9, d);

  x = 3;
  y = 3;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(10, d);

  x = 3;
  y = 2;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(11, d);

  x = 3;
  y = 1;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(12, d);

  x = 2;
  y = 1;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(13, d);

  x = 2;
  y = 0;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(14, d);

  x = 3;
  y = 0;
  d = traversal::hilbert::xy2d(n, x, y);
  ASSERT_EQ(15, d);
}

TEST(TraversalTest, Hilbert_d2xy) {
  int64_t n = 4;
  int64_t d;
  int64_t x, y;

  d = 0;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(0, y);

  d = 1;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(0, y);

  d = 2;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(1, y);

  d = 3;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(1, y);

  d = 4;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(2, y);

  d = 5;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(3, y);

  d = 6;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(3, y);

  d = 7;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(2, y);

  d = 8;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(2, y);

  d = 9;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(3, y);

  d = 10;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(3, y);

  d = 11;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(2, y);

  d = 12;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(1, y);

  d = 13;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(1, y);

  d = 14;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(0, y);

  d = 15;
  traversal::hilbert::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(0, y);
}

TEST(TraversalTest, ColumnFirst_xy2d) {
  int64_t n = 4;
  int64_t x, y;

  x = 0;
  y = 0;
  int64_t d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(0, d);

  x = 0;
  y = 1;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(1, d);

  x = 0;
  y = 2;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(2, d);

  x = 0;
  y = 3;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(3, d);

  x = 1;
  y = 0;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(4, d);

  x = 1;
  y = 1;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(5, d);

  x = 1;
  y = 2;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(6, d);

  x = 1;
  y = 3;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(7, d);

  x = 2;
  y = 0;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(8, d);

  x = 2;
  y = 1;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(9, d);

  x = 2;
  y = 2;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(10, d);

  x = 2;
  y = 3;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(11, d);

  x = 3;
  y = 0;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(12, d);

  x = 3;
  y = 1;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(13, d);

  x = 3;
  y = 2;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(14, d);

  x = 3;
  y = 3;
  d = traversal::column_first::xy2d(n, x, y);
  ASSERT_EQ(15, d);
}

TEST(TraversalTest, ColumnFirst_d2xy) {
  int64_t n = 4;
  int64_t d;
  int64_t x, y;

  d = 0;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(0, y);

  d = 1;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(1, y);

  d = 2;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(2, y);

  d = 3;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(3, y);

  d = 4;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(0, y);

  d = 5;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(1, y);

  d = 6;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(2, y);

  d = 7;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(3, y);

  d = 8;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(0, y);

  d = 9;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(1, y);

  d = 10;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(2, y);

  d = 11;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(3, y);

  d = 12;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(0, y);

  d = 13;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(1, y);

  d = 14;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(2, y);

  d = 15;
  traversal::column_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(3, y);
}

TEST(TraversalTest, RowFirst_xy2d) {
  int64_t n = 4;
  int64_t x, y;

  x = 0;
  y = 0;
  int64_t d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(0, d);

  x = 1;
  y = 0;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(1, d);

  x = 2;
  y = 0;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(2, d);

  x = 3;
  y = 0;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(3, d);

  x = 0;
  y = 1;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(4, d);

  x = 1;
  y = 1;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(5, d);

  x = 2;
  y = 1;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(6, d);

  x = 3;
  y = 1;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(7, d);

  x = 0;
  y = 2;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(8, d);

  x = 1;
  y = 2;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(9, d);

  x = 2;
  y = 2;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(10, d);

  x = 3;
  y = 2;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(11, d);

  x = 0;
  y = 3;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(12, d);

  x = 1;
  y = 3;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(13, d);

  x = 2;
  y = 3;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(14, d);

  x = 3;
  y = 3;
  d = traversal::row_first::xy2d(n, x, y);
  ASSERT_EQ(15, d);
}

TEST(TraversalTest, RowFirst_d2xy) {
  int64_t n = 4;
  int64_t d;
  int64_t x, y;

  d = 0;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(0, y);

  d = 1;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(0, y);

  d = 2;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(0, y);

  d = 3;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(0, y);

  d = 4;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(1, y);

  d = 5;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(1, y);

  d = 6;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(1, y);

  d = 7;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(1, y);

  d = 8;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(2, y);

  d = 9;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(2, y);

  d = 10;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(2, y);

  d = 11;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(2, y);

  d = 12;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(0, x);
  ASSERT_EQ(3, y);

  d = 13;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(1, x);
  ASSERT_EQ(3, y);

  d = 14;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(2, x);
  ASSERT_EQ(3, y);

  d = 15;
  traversal::row_first::d2xy(n, d, &x, &y);
  ASSERT_EQ(3, x);
  ASSERT_EQ(3, y);
}
