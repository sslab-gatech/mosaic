#include <core/util.h>
#include <string.h>
#include "test.h"

int main(int argc, char** argv) {
  int count_booleans = 100;
  int size_char_array = size_bool_array(count_booleans);
  sg_test(size_char_array == 13, "size_bool_array");

  char* bool_array = new char[size_char_array];

  for (int i = 0; i < size_char_array; ++i) {
    bool_array[i] = 0;
  }

  set_bool_array(bool_array, 0, true);
  sg_test(bool_array[0] == 1, "test_bit_one");

  set_bool_array(bool_array, 0, false);
  set_bool_array(bool_array, 1, true);
  sg_test(bool_array[0] == 2, "test_bit_two");

  set_bool_array(bool_array, 8, true);
  sg_test(bool_array[1] == 1, "test_bit_one_char_two");

  sg_test(eval_bool_array(bool_array, 0) == 0, "test_eval_bit_one");
  sg_test(eval_bool_array(bool_array, 1) == 1, "test_eval_bit_two");
  sg_test(eval_bool_array(bool_array, 8) == 1, "test_eval_bit_one_char_two");

  memset(bool_array, (unsigned char)255, 1);
  for (int i = 0; i < 8; ++i) {
    sg_test(eval_bool_array(bool_array, i) == 1, "test_eval_loop_true");
  }

  memset(bool_array, (unsigned char)0, 1);
  for (int i = 0; i < 8; ++i) {
    sg_test(eval_bool_array(bool_array, i) == 0, "test_eval_loop_false");
  }

  return 0;
}
