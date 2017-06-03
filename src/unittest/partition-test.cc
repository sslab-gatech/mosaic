#include "test.h"
#include <core/util.h>
#include <core/datatypes.h>

namespace core = scalable_graphs::core;
namespace util = scalable_graphs::util;

struct meta_partition_store_t {
  meta_partition_meta_t local_partition_info;
  meta_partition_meta_t global_partition_info;
};

int main(int argc, char** argv) {

  uint64_t count_vertices = 41652230;

  const size_t expected_num_meta_partitions = 8;
  const size_t expected_num_meta_partitions_per_partition_manager = 4;
  const size_t count_partition_managers = 4;

  command_line_args_grc_t cmd_args;
  cmd_args.count_partition_managers = count_partition_managers;
  cmd_args.nthreads = 2;

  config_grc_t config;
  core::initGrcConfig(&config, count_vertices, cmd_args);

  partition_manager_arguments_t* arguments =
      new partition_manager_arguments_t[count_partition_managers];

  for (int i = 0; i < config.count_partition_managers; ++i) {
    partition_manager_arguments_t argument =
        core::getPartitionManagerArguments(i, config);
    arguments[i] = argument;
  }

  meta_partition_store_t** partition_store_meta =
      new meta_partition_store_t*[count_partition_managers];

  for (int i = 0; i < config.count_partition_managers; ++i) {
    size_t count_partition_stores =
        expected_num_meta_partitions_per_partition_manager *
        expected_num_meta_partitions_per_partition_manager;
    partition_store_meta[i] =
        new meta_partition_store_t[count_partition_stores];

    for (int j = 0; j < count_partition_stores; ++j) {
      partition_store_meta[i][j].local_partition_info =
          core::getLocalPartitionInfoForPartitionStore(config, j);

      partition_store_meta[i][j].global_partition_info =
          core::getGlobalPartitionInfoForPartitionStore(
              config, arguments[i].meta,
              partition_store_meta[i][j].local_partition_info, j);
    }
  }

  sg_test(config.count_vertices == 41652230, "count_vertices");
  sg_test(config.count_partition_managers == 4, "count_partition_managers");
  sg_test(config.count_rows_partitions == 1024, "rows_partitions");
  sg_test(config.count_rows_meta_partitions == expected_num_meta_partitions,
          "rows_meta_partitions");
  sg_test(config.count_rows_partition_stores_per_partition_manager ==
              expected_num_meta_partitions_per_partition_manager,
          "count_rows_partition_stores_per_partition_manager");
  sg_test(config.count_rows_vertices_per_partition_manager ==
              expected_num_meta_partitions_per_partition_manager *
                  MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
          "count_rows_vertices_per_partition_manager");
  sg_test(config.nthreads == 2, "nthreads");

  // test partition-manager-arguments
  // partition-manager 0
  sg_test(arguments[0].thread_index.id == 0, "pm_0_thread_index");
  sg_test(arguments[0].thread_index.count == 4, "pm_0_thread_count");

  sg_test(arguments[0].meta.meta_partition_index.i == 0,
          "pm_0_meta_partition_i");
  sg_test(arguments[0].meta.meta_partition_index.j == 0,
          "pm_0_meta_partition_j");

  sg_test(arguments[0].meta.vertex_partition_index_start.i == 0,
          "pm_0_meta_vertices_i");
  sg_test(arguments[0].meta.vertex_partition_index_start.j == 0,
          "pm_0_meta_vertices_j");

  // partition-manager 1
  sg_test(arguments[1].thread_index.id == 1, "pm_1_thread_index");
  sg_test(arguments[1].thread_index.count == 4, "pm_1_thread_count");

  sg_test(arguments[1].meta.meta_partition_index.i == 0,
          "pm_1_meta_partition_i");
  sg_test(arguments[1].meta.meta_partition_index.j == 4,
          "pm_1_meta_partition_j");

  sg_test(arguments[1].meta.vertex_partition_index_start.i == 0,
          "pm_1_meta_vertices_i");
  sg_test(arguments[1].meta.vertex_partition_index_start.j ==
              expected_num_meta_partitions_per_partition_manager *
                  MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
          "pm_1_meta_vertices_j");

  // partition-manager 2
  sg_test(arguments[2].thread_index.id == 2, "pm_2_thread_index");
  sg_test(arguments[2].thread_index.count == 4, "pm_2_thread_count");

  sg_test(arguments[2].meta.meta_partition_index.i == 4,
          "pm_2_meta_partition_i");
  sg_test(arguments[2].meta.meta_partition_index.j == 0,
          "pm_2_meta_partition_j");

  sg_test(arguments[2].meta.vertex_partition_index_start.i ==
              expected_num_meta_partitions_per_partition_manager *
                  MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
          "pm_2_meta_vertices_i");
  sg_test(arguments[2].meta.vertex_partition_index_start.j == 0,
          "pm_2_meta_vertices_j");

  // partition-manager 2
  sg_test(arguments[3].thread_index.id == 3, "pm_3_thread_index");
  sg_test(arguments[3].thread_index.count == 4, "pm_3_thread_count");

  sg_test(arguments[3].meta.meta_partition_index.i == 4,
          "pm_3_meta_partition_i");
  sg_test(arguments[3].meta.meta_partition_index.j == 4,
          "pm_3_meta_partition_j");

  sg_test(arguments[3].meta.vertex_partition_index_start.i ==
              expected_num_meta_partitions_per_partition_manager *
                  MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
          "pm_3_meta_vertices_i");
  sg_test(arguments[3].meta.vertex_partition_index_start.j ==
              expected_num_meta_partitions_per_partition_manager *
                  MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
          "pm_3_meta_vertices_j");

  // test partition-store arguments
  // test partition-store (1, 1) -> 5 of partition-manager 3
  // first test local properties
  sg_test(
      partition_store_meta[3][5].local_partition_info.meta_partition_index.i ==
          1,
      "partition_store_local_meta_i");
  sg_test(
      partition_store_meta[3][5].local_partition_info.meta_partition_index.j ==
          1,
      "partition_store_local_meta_j");
  sg_test(partition_store_meta[3][5]
                  .local_partition_info.vertex_partition_index_start.i ==
              VERTICES_PER_SPARSE_FILE,
          "partition_store_local_vertex_i");
  sg_test(partition_store_meta[3][5]
                  .local_partition_info.vertex_partition_index_start.j ==
              VERTICES_PER_SPARSE_FILE,
          "partition_store_local_vertex_j");

  // new test global properties
  sg_test(
      partition_store_meta[3][5].global_partition_info.meta_partition_index.i ==
          5,
      "partition_store_global_meta_i");
  sg_test(
      partition_store_meta[3][5].global_partition_info.meta_partition_index.j ==
          5,
      "partition_store_global_meta_j");
  sg_test(partition_store_meta[3][5]
                  .global_partition_info.vertex_partition_index_start.i ==
              expected_num_meta_partitions_per_partition_manager *
                      VERTICES_PER_SPARSE_FILE +
                  VERTICES_PER_SPARSE_FILE,
          "partition_store_global_vertex_i");
  sg_test(partition_store_meta[3][5]
                  .global_partition_info.vertex_partition_index_start.j ==
              expected_num_meta_partitions_per_partition_manager *
                      VERTICES_PER_SPARSE_FILE +
                  VERTICES_PER_SPARSE_FILE,
          "partition_store_global_vertex_j");

  // meta-partition-testing
  // edge 1
  edge_t test_edge = {1, 1};

  partition_t meta_partition = core::getMetaPartitionOfEdge(test_edge);
  sg_test(meta_partition.i == 0, "meta_partition_i");
  sg_test(meta_partition.j == 0, "meta_partition_j");

  // edge 2
  test_edge = {MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1,
               MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1};

  meta_partition = core::getMetaPartitionOfEdge(test_edge);
  sg_test(meta_partition.i == 0, "meta_partition_i");
  sg_test(meta_partition.j == 0, "meta_partition_j");

  // edge 3
  test_edge = {MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
               MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE};

  meta_partition = core::getMetaPartitionOfEdge(test_edge);
  sg_test(meta_partition.i == 1, "meta_partition_i");
  sg_test(meta_partition.j == 1, "meta_partition_j");

  // test partition-manager matching
  // edge 1
  test_edge = {1, 1};
  partition_t partition_manager_partition =
      core::getPartitionManagerOfEdge(test_edge, config);
  sg_test(partition_manager_partition.i == 0, "partition_manager_partition_i");
  sg_test(partition_manager_partition.j == 0, "partition_manager_partition_j");

  // edge 2
  test_edge = {expected_num_meta_partitions_per_partition_manager *
                       MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE -
                   1,
               expected_num_meta_partitions_per_partition_manager *
                       MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE -
                   1};
  partition_manager_partition =
      core::getPartitionManagerOfEdge(test_edge, config);
  sg_test(partition_manager_partition.i == 0, "partition_manager_partition_i");
  sg_test(partition_manager_partition.j == 0, "partition_manager_partition_j");

  // edge 3
  test_edge = {expected_num_meta_partitions_per_partition_manager *
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
               expected_num_meta_partitions_per_partition_manager *
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE};
  partition_manager_partition =
      core::getPartitionManagerOfEdge(test_edge, config);
  sg_test(partition_manager_partition.i == 1, "partition_manager_partition_i");
  sg_test(partition_manager_partition.j == 1, "partition_manager_partition_j");

  // test partition-store matching
  // first, with partition-manager 0
  // edge 1
  test_edge = {1, 1};
  partition_t partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[0].meta);
  sg_test(partition_store_partition.i == 0, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 0, "partition_store_partition_j");

  // edge 2
  test_edge = {MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1,
               MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1};
  partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[0].meta);
  sg_test(partition_store_partition.i == 0, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 0, "partition_store_partition_j");

  // edge 3
  test_edge = {MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
               MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE};
  partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[0].meta);
  sg_test(partition_store_partition.i == 1, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 1, "partition_store_partition_j");

  // now with partition-manager 3
  // edge 1
  test_edge = {arguments[3].meta.vertex_partition_index_start.i + 1,
               arguments[3].meta.vertex_partition_index_start.j + 1};
  partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[3].meta);
  sg_test(partition_store_partition.i == 0, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 0, "partition_store_partition_j");

  // edge 2
  test_edge = {arguments[3].meta.vertex_partition_index_start.i +
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1,
               arguments[3].meta.vertex_partition_index_start.j +
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1};
  partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[3].meta);
  sg_test(partition_store_partition.i == 0, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 0, "partition_store_partition_j");

  // edge 3
  test_edge = {arguments[3].meta.vertex_partition_index_start.i +
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
               arguments[3].meta.vertex_partition_index_start.j +
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE};
  partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[3].meta);
  sg_test(partition_store_partition.i == 1, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 1, "partition_store_partition_j");

  // test partition-inside-partition-store with partition-manager 3,
  // partition-store 15
  test_edge = {arguments[3].meta.vertex_partition_index_start.i +
                   (expected_num_meta_partitions_per_partition_manager - 1) *
                       MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE +
                   MAX_VERTICES_PER_TILE,
               arguments[3].meta.vertex_partition_index_start.j +
                   (expected_num_meta_partitions_per_partition_manager - 1) *
                       MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE +
                   MAX_VERTICES_PER_TILE};
  partition_t partition_inside_partition_store =
      core::getPartitionInsidePartitionStoreOfEdge(
          test_edge, partition_store_meta[3][15].global_partition_info);
  sg_test(partition_inside_partition_store.i == 1,
          "partition_inside_partition_store_i");
  sg_test(partition_inside_partition_store.j == 1,
          "partition_inside_partition_store_j");

  // test offsets
  size_t offset_partition_manager =
      core::getIndexOfPartitionManager({1, 1}, config);
  sg_test(offset_partition_manager == 3, "offset_partition_manager");

  size_t offset_partition_store =
      core::getIndexOfPartitionStore({1, 1}, config);
  sg_test(offset_partition_store == 5, "offset_partition_store");

  size_t offset_partition_inside_file = core::getIndexOfPartitionInFile({1, 1});
  sg_test(offset_partition_inside_file == PARTITION_COLS_PER_SPARSE_FILE + 1,
          "offset_partition_inside_file");

  // now, test the reverse translation, i.e. using partitions to obtain
  // PartitionManagers etc.
  // first, test meta-partition resolution
  partition_t test_partition = {1, 1};
  meta_partition = core::getMetaPartitionOfPartition(test_partition);
  sg_test(meta_partition.i == 0, "partition_meta_partition_i");
  sg_test(meta_partition.j == 0, "partition_meta_partition_j");

  test_partition = {PARTITION_COLS_PER_SPARSE_FILE - 1,
                    PARTITION_COLS_PER_SPARSE_FILE - 1};
  meta_partition = core::getMetaPartitionOfPartition(test_partition);
  sg_test(meta_partition.i == 0, "partition_meta_partition_i");
  sg_test(meta_partition.j == 0, "partition_meta_partition_j");

  test_partition = {PARTITION_COLS_PER_SPARSE_FILE,
                    PARTITION_COLS_PER_SPARSE_FILE};
  meta_partition = core::getMetaPartitionOfPartition(test_partition);
  sg_test(meta_partition.i == 1, "partition_meta_partition_i");
  sg_test(meta_partition.j == 1, "partition_meta_partition_j");

  // test partition-manager-resolution
  test_partition = {1, 1};
  partition_manager_partition =
      core::getPartitionManagerOfPartition(test_partition, config);
  sg_test(partition_manager_partition.i == 0, "partition_manager_partition_i");
  sg_test(partition_manager_partition.j == 0, "partition_manager_partition_j");

  test_partition = {config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE -
                        1ul,
                    config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE -
                        1ul};
  partition_manager_partition =
      core::getPartitionManagerOfPartition(test_partition, config);
  sg_test(partition_manager_partition.i == 0, "partition_manager_partition_i");
  sg_test(partition_manager_partition.j == 0, "partition_manager_partition_j");

  test_partition = {config.count_rows_partition_stores_per_partition_manager *
                        PARTITION_COLS_PER_SPARSE_FILE,
                    config.count_rows_partition_stores_per_partition_manager *
                        PARTITION_COLS_PER_SPARSE_FILE};
  partition_manager_partition =
      core::getPartitionManagerOfPartition(test_partition, config);
  sg_test(partition_manager_partition.i == 1, "partition_manager_partition_i");
  sg_test(partition_manager_partition.j == 1, "partition_manager_partition_j");

  // now test partition-store-resolution
  test_partition = {1, 1};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[0].meta);
  sg_test(partition_store_partition.i == 0, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 0, "partition_store_partition_j");

  test_partition = {PARTITION_COLS_PER_SPARSE_FILE - 1,
                    PARTITION_COLS_PER_SPARSE_FILE - 1};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[0].meta);
  sg_test(partition_store_partition.i == 0, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 0, "partition_store_partition_j");

  test_partition = {PARTITION_COLS_PER_SPARSE_FILE,
                    PARTITION_COLS_PER_SPARSE_FILE};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[0].meta);
  sg_test(partition_store_partition.i == 1, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 1, "partition_store_partition_j");

  // test with partition-manager 3 : (1, 1) as well:
  test_partition = {config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        1,
                    config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        1};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[3].meta);
  sg_test(partition_store_partition.i == 0, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 0, "partition_store_partition_j");

  test_partition = {config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        PARTITION_COLS_PER_SPARSE_FILE - 1,
                    config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        PARTITION_COLS_PER_SPARSE_FILE - 1};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[3].meta);
  sg_test(partition_store_partition.i == 0, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 0, "partition_store_partition_j");

  test_partition = {config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        PARTITION_COLS_PER_SPARSE_FILE,
                    config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        PARTITION_COLS_PER_SPARSE_FILE};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[3].meta);
  sg_test(partition_store_partition.i == 1, "partition_store_partition_i");
  sg_test(partition_store_partition.j == 1, "partition_store_partition_j");

  // test partition-inside-partition-store with partition-manager 3,
  // partition-store 15
  test_partition = {
      arguments[3].meta.meta_partition_index.i *
              PARTITION_COLS_PER_SPARSE_FILE +
          (expected_num_meta_partitions_per_partition_manager - 1) *
              PARTITION_COLS_PER_SPARSE_FILE +
          1,
      arguments[3].meta.meta_partition_index.j *
              PARTITION_COLS_PER_SPARSE_FILE +
          (expected_num_meta_partitions_per_partition_manager - 1) *
              PARTITION_COLS_PER_SPARSE_FILE +
          1};
  partition_inside_partition_store =
      core::getPartitionInsidePartitionStoreOfPartition(
          test_partition, partition_store_meta[3][15].global_partition_info);
  sg_test(partition_inside_partition_store.i == 1,
          "partition_inside_partition_store_i");
  sg_test(partition_inside_partition_store.j == 1,
          "partition_inside_partition_store_j");

  uint64_t large_test_int = 2ul * (UINT32_MAX + 1ul) + 1ul;
  uint64_t upper_32_bits_large_test_int =
      large_test_int & (~((size_t)UINT32_MAX));
  uint32_t large_test_int_compressed =
      large_test_int - upper_32_bits_large_test_int;

  sg_test(upper_32_bits_large_test_int == (2ul * (UINT32_MAX + 1ul)),
          "upper_32_bits_large_test_int");
  sg_test(large_test_int_compressed == 1, "large_test_int_compressed");

  uint64_t test_int = (1ul << 32) + 1;
  uint32_t lower_bits = test_int & ((size_t)UINT32_MAX);
  char* upper_bits = new char[1];
  upper_bits[0] = 0;
  set_bool_array(upper_bits, 0, (test_int & (1ul << 32)));
  uint64_t output_test_int =
      (uint64_t)lower_bits | ((uint64_t)upper_bits[0] << 32);

  sg_test(lower_bits == 1, "lower_bits");
  sg_test(upper_bits[0] == 1, "upper_bits");
  sg_test(test_int == output_test_int, "test_int");

  config.paths_to_partition.push_back("/mnt/sda1/");
  config.paths_to_partition.push_back("/mnt/sdb1/");
  config.paths_to_partition.push_back("/mnt/sdc1/");
  config.paths_to_partition.push_back("/mnt/sdd1/");

  test_partition = {0, 0};
  std::string meta_partition_file_name =
      core::getMetaPartitionFileName(config, test_partition);
  sg_test(meta_partition_file_name ==
              "/mnt/sda1/000/meta-pt-00000000-00000000.dat",
          "meta_partition_file_name");

  test_partition = {0, 1};
  meta_partition_file_name =
      core::getMetaPartitionFileName(config, test_partition);
  sg_test(meta_partition_file_name ==
              "/mnt/sdb1/001/meta-pt-00000000-00000001.dat",
          "meta_partition_file_name");

  test_partition = {1, 0};
  meta_partition_file_name =
      core::getMetaPartitionFileName(config, test_partition);
  sg_test(meta_partition_file_name ==
              "/mnt/sdb1/001/meta-pt-00000001-00000000.dat",
          "meta_partition_file_name");

  test_partition = {1, 1};
  meta_partition_file_name =
      core::getMetaPartitionFileName(config, test_partition);
  sg_test(meta_partition_file_name ==
              "/mnt/sdc1/002/meta-pt-00000001-00000001.dat",
          "meta_partition_file_name");

  return 0;
}
