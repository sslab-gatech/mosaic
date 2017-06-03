#include <core/util.h>
#include "test.h"

namespace core = scalable_graphs::core;

int main(int argc, char** argv) {
  std::unordered_map<int, int> test_map;
  test_map[1] = 2;
  test_map[0] = 1;
  test_map[10] = 100;

  std::string file_name = "test_map.dat";
  core::writeMapToFile<int, int>(file_name, test_map);

  std::unordered_map<int, int> file_map;
  core::readMapFromFile<int, int>(file_name, file_map);

  sg_test(file_map[0] == 1, "test_elem_one");
  sg_test(file_map[1] == 2, "test_elem_two");
  sg_test(file_map[10] == 100, "test_elem_three");
  sg_test(file_map.size() == 3, "test_map_size");

  return 0;
}
