#include <core/util.h>
#include "test.h"

namespace ut = scalable_graphs::util;

int main(int argc, char** argv) {
  struct test_struct {
    int id;
    int ar1;
    int ar2;
  };

  int size_ar1 = 100;
  int size_ar2 = 200;
  test_struct* ts = (test_struct*)malloc(
      sizeof(test_struct) + sizeof(int) * size_ar1 + sizeof(int) * size_ar2);

  ts->id = 1;
  ts->ar1 = sizeof(test_struct);
  ts->ar2 = ts->ar1 + size_ar1 * sizeof(int);

  int* ar1 = get_array(int*, ts, ts->ar1);
  int* ar2 = get_array(int*, ts, ts->ar2);

  for (int i = 0; i < size_ar1; ++i) {
    ar1[i] = i;
  }

  for (int i = 0; i < size_ar2; ++i) {
    ar2[i] = i + 1;
  }

  // check
  int* ar1_test = get_array(int*, ts, ts->ar1);
  int* ar2_test = get_array(int*, ts, ts->ar2);

  sg_dbg("%d %d\n", ts->ar1, ts->ar2);
  sg_dbg("%p %p %p %p %p\n", ts, ar1, ar2, ar1_test, ar2_test);

  for (int i = 0; i < size_ar1; ++i) {
    sg_test(ar1_test[i] == i, "ar_1");
  }
  for (int i = 0; i < size_ar2; ++i) {
    sg_test(ar2_test[i] == i + 1, "ar_2");
  }

  return 0;
}
