#include "gtest/gtest.h"
#include <core/util.h>
#include <string.h>

TEST(BoolArrayTest, BasicOperations) {
  int count_booleans = 100;
  int size_char_array = size_bool_array(count_booleans);
  ASSERT_EQ(13, size_char_array);

  char* bool_array = new char[size_char_array];
  for (int i = 0; i < size_char_array; ++i) {
    bool_array[i] = 0;
  }

  set_bool_array(bool_array, 0, true);
  ASSERT_EQ(1, bool_array[0]);

  set_bool_array(bool_array, 0, false);
  set_bool_array(bool_array, 1, true);
  ASSERT_EQ(2, bool_array[0]);

  set_bool_array(bool_array, 8, true);
  ASSERT_EQ(1, bool_array[1]);

  ASSERT_EQ(0, eval_bool_array(bool_array, 0));
  ASSERT_EQ(1, eval_bool_array(bool_array, 1));
  ASSERT_EQ(1, eval_bool_array(bool_array, 8));

  memset(bool_array, (unsigned char)255, 1);
  for (int i = 0; i < 8; ++i) {
    ASSERT_EQ(1, eval_bool_array(bool_array, i));
  }

  memset(bool_array, (unsigned char)0, 1);
  for (int i = 0; i < 8; ++i) {
    ASSERT_EQ(0, eval_bool_array(bool_array, i));
  }
}
