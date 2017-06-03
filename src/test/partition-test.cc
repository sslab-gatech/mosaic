#include "gtest/gtest.h"
#include <core/util.h>
#include <core/datatypes.h>

namespace core = scalable_graphs::core;
namespace util = scalable_graphs::util;

struct meta_partition_store_t {
  meta_partition_meta_t local_partition_info;
  meta_partition_meta_t global_partition_info;
};

TEST(Util, GrcConfig) {
  uint64_t count_vertices = 41652230;

  const size_t expected_num_meta_partitions = 16;
  const size_t expected_num_meta_partitions_per_partition_manager = 8;
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

  // count_vertices
  ASSERT_EQ(41652230, config.count_vertices);
  // count_partition_managers
  ASSERT_EQ(4, config.count_partition_managers);
  // rows_partitions
  ASSERT_EQ(1024, config.count_rows_partitions);
  // rows_meta_partitions
  ASSERT_EQ(16, config.count_rows_meta_partitions);
  // count_rows_partition_stores_per_partition_manager
  ASSERT_EQ(expected_num_meta_partitions_per_partition_manager,
            config.count_rows_partition_stores_per_partition_manager);
  // count_rows_vertices_per_partition_manager
  ASSERT_EQ(expected_num_meta_partitions_per_partition_manager *
                MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
            config.count_rows_vertices_per_partition_manager);
  // nthreads
  ASSERT_EQ(2, config.nthreads);

  // test partition-manager-arguments
  // partition-manager 0
  // pm_0_thread_index
  ASSERT_EQ(0, arguments[0].thread_index.id);
  // pm_0_thread_count
  ASSERT_EQ(4, arguments[0].thread_index.count);

  // pm_0_meta_partition_i
  ASSERT_EQ(0, arguments[0].meta.meta_partition_index.i);
  // pm_0_meta_partition_j
  ASSERT_EQ(0, arguments[0].meta.meta_partition_index.j);

  // pm_0_meta_vertices_i
  ASSERT_EQ(0, arguments[0].meta.vertex_partition_index_start.i);
  // pm_0_meta_vertices_j
  ASSERT_EQ(0, arguments[0].meta.vertex_partition_index_start.j);

  // partition-manager 1
  ASSERT_EQ(1, arguments[1].thread_index.id);
  ASSERT_EQ(4, arguments[1].thread_index.count);

  ASSERT_EQ(0, arguments[1].meta.meta_partition_index.i);
  ASSERT_EQ(8, arguments[1].meta.meta_partition_index.j);

  ASSERT_EQ(0, arguments[1].meta.vertex_partition_index_start.i);
  ASSERT_EQ(expected_num_meta_partitions_per_partition_manager *
                MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
            arguments[1].meta.vertex_partition_index_start.j);

  // partition-manager 2
  ASSERT_EQ(2, arguments[2].thread_index.id);
  ASSERT_EQ(4, arguments[2].thread_index.count);

  ASSERT_EQ(8, arguments[2].meta.meta_partition_index.i);
  ASSERT_EQ(0, arguments[2].meta.meta_partition_index.j);

  // pm_2_meta_vertices_i
  ASSERT_EQ(expected_num_meta_partitions_per_partition_manager *
                MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
            arguments[2].meta.vertex_partition_index_start.i);
  // pm_2_meta_vertices_j
  ASSERT_EQ(0, arguments[2].meta.vertex_partition_index_start.j);

  // partition-manager 2
  // pm_3_thread_index
  ASSERT_EQ(3, arguments[3].thread_index.id);
  // pm_3_thread_count
  ASSERT_EQ(4, arguments[3].thread_index.count);

  // pm_3_meta_partition_i
  ASSERT_EQ(8, arguments[3].meta.meta_partition_index.i);
  // pm_3_meta_partition_j
  ASSERT_EQ(8, arguments[3].meta.meta_partition_index.j);

  // pm_3_meta_vertices_i
  ASSERT_EQ(expected_num_meta_partitions_per_partition_manager *
                MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
            arguments[3].meta.vertex_partition_index_start.i);
  // pm_3_meta_vertices_j
  ASSERT_EQ(expected_num_meta_partitions_per_partition_manager *
                MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
            arguments[3].meta.vertex_partition_index_start.j);

  // test partition-store arguments
  // test partition-store (1, 1) -> 9 of partition-manager 3
  // first test local properties
  ASSERT_EQ(
      1,
      partition_store_meta[3][9].local_partition_info.meta_partition_index.i);
  ASSERT_EQ(
      1,
      partition_store_meta[3][9].local_partition_info.meta_partition_index.j);
  ASSERT_EQ(VERTICES_PER_SPARSE_FILE,
            partition_store_meta[3][9]
                .local_partition_info.vertex_partition_index_start.i);
  ASSERT_EQ(VERTICES_PER_SPARSE_FILE,
            partition_store_meta[3][9]
                .local_partition_info.vertex_partition_index_start.j);

  // new test global properties
  ASSERT_EQ(
      9,
      partition_store_meta[3][9].global_partition_info.meta_partition_index.i);
  ASSERT_EQ(
      9,
      partition_store_meta[3][9].global_partition_info.meta_partition_index.j);
  ASSERT_EQ(expected_num_meta_partitions_per_partition_manager *
                    VERTICES_PER_SPARSE_FILE +
                VERTICES_PER_SPARSE_FILE,
            partition_store_meta[3][9]
                .global_partition_info.vertex_partition_index_start.i);
  ASSERT_EQ(expected_num_meta_partitions_per_partition_manager *
                    VERTICES_PER_SPARSE_FILE +
                VERTICES_PER_SPARSE_FILE,
            partition_store_meta[3][9]
                .global_partition_info.vertex_partition_index_start.j);

  // test partition-manager matching
  // edge 1
  edge_t test_edge = {1, 1};
  partition_t partition_manager_partition =
      core::getPartitionManagerOfEdge(test_edge, config);
  ASSERT_EQ(0, partition_manager_partition.i);
  ASSERT_EQ(0, partition_manager_partition.j);

  // edge 2
  test_edge = {expected_num_meta_partitions_per_partition_manager *
                       MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE -
                   1,
               expected_num_meta_partitions_per_partition_manager *
                       MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE -
                   1};
  partition_manager_partition =
      core::getPartitionManagerOfEdge(test_edge, config);
  ASSERT_EQ(0, partition_manager_partition.i);
  ASSERT_EQ(0, partition_manager_partition.j);

  // edge 3
  test_edge = {expected_num_meta_partitions_per_partition_manager *
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
               expected_num_meta_partitions_per_partition_manager *
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE};
  partition_manager_partition =
      core::getPartitionManagerOfEdge(test_edge, config);
  ASSERT_EQ(1, partition_manager_partition.i);
  ASSERT_EQ(1, partition_manager_partition.j);

  // test partition-store matching
  // first, with partition-manager 0
  // edge 1
  test_edge = {1, 1};
  partition_t partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[0].meta);
  ASSERT_EQ(0, partition_store_partition.i);
  ASSERT_EQ(0, partition_store_partition.j);

  // edge 2
  test_edge = {MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1,
               MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1};
  partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[0].meta);
  ASSERT_EQ(0, partition_store_partition.i);
  ASSERT_EQ(0, partition_store_partition.j);

  // edge 3
  test_edge = {MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
               MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE};
  partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[0].meta);
  ASSERT_EQ(1, partition_store_partition.i);
  ASSERT_EQ(1, partition_store_partition.j);

  // now with partition-manager 3
  // edge 1
  test_edge = {arguments[3].meta.vertex_partition_index_start.i + 1,
               arguments[3].meta.vertex_partition_index_start.j + 1};
  partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[3].meta);
  ASSERT_EQ(0, partition_store_partition.i);
  ASSERT_EQ(0, partition_store_partition.j);

  // edge 2
  test_edge = {arguments[3].meta.vertex_partition_index_start.i +
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1,
               arguments[3].meta.vertex_partition_index_start.j +
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1};
  partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[3].meta);
  ASSERT_EQ(0, partition_store_partition.i);
  ASSERT_EQ(0, partition_store_partition.j);

  // edge 3
  test_edge = {arguments[3].meta.vertex_partition_index_start.i +
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
               arguments[3].meta.vertex_partition_index_start.j +
                   MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE};
  partition_store_partition =
      core::getPartitionStoreOfEdge(test_edge, arguments[3].meta);
  ASSERT_EQ(1, partition_store_partition.i);
  ASSERT_EQ(1, partition_store_partition.j);

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
          test_edge, partition_store_meta[3][63].global_partition_info);
  ASSERT_EQ(1, partition_inside_partition_store.i);
  ASSERT_EQ(1, partition_inside_partition_store.j);

  // test offsets
  size_t offset_partition_manager =
      core::getIndexOfPartitionManager({1, 1}, config);
  ASSERT_EQ(3, offset_partition_manager);

  size_t offset_partition_store =
      core::getIndexOfPartitionStore({1, 1}, config);
  ASSERT_EQ(9, offset_partition_store);

  size_t offset_partition_inside_file = core::getIndexOfPartitionInFile({1, 1});
  ASSERT_EQ(PARTITION_COLS_PER_SPARSE_FILE + 1, offset_partition_inside_file);

  // now, test the reverse translation, i.e. using partitions to obtain
  // PartitionManagers etc.
  // first, test meta-partition resolution
  partition_t test_partition = {1, 1};
  partition_t meta_partition =
      core::getMetaPartitionOfPartition(test_partition);
  ASSERT_EQ(0, meta_partition.i);
  ASSERT_EQ(0, meta_partition.j);

  test_partition = {PARTITION_COLS_PER_SPARSE_FILE - 1,
                    PARTITION_COLS_PER_SPARSE_FILE - 1};
  meta_partition = core::getMetaPartitionOfPartition(test_partition);
  ASSERT_EQ(0, meta_partition.i);
  ASSERT_EQ(0, meta_partition.j);

  test_partition = {PARTITION_COLS_PER_SPARSE_FILE,
                    PARTITION_COLS_PER_SPARSE_FILE};
  meta_partition = core::getMetaPartitionOfPartition(test_partition);
  ASSERT_EQ(1, meta_partition.i);
  ASSERT_EQ(1, meta_partition.j);

  // test partition-manager-resolution
  test_partition = {1, 1};
  partition_manager_partition =
      core::getPartitionManagerOfPartition(test_partition, config);
  ASSERT_EQ(0, partition_manager_partition.i);
  ASSERT_EQ(0, partition_manager_partition.j);

  test_partition = {config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE -
                        1ul,
                    config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE -
                        1ul};
  partition_manager_partition =
      core::getPartitionManagerOfPartition(test_partition, config);
  ASSERT_EQ(0, partition_manager_partition.i);
  ASSERT_EQ(0, partition_manager_partition.j);

  test_partition = {config.count_rows_partition_stores_per_partition_manager *
                        PARTITION_COLS_PER_SPARSE_FILE,
                    config.count_rows_partition_stores_per_partition_manager *
                        PARTITION_COLS_PER_SPARSE_FILE};
  partition_manager_partition =
      core::getPartitionManagerOfPartition(test_partition, config);
  ASSERT_EQ(1, partition_manager_partition.i);
  ASSERT_EQ(1, partition_manager_partition.j);

  // now test partition-store-resolution
  test_partition = {1, 1};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[0].meta);
  ASSERT_EQ(0, partition_store_partition.i);
  ASSERT_EQ(0, partition_store_partition.j);

  test_partition = {PARTITION_COLS_PER_SPARSE_FILE - 1,
                    PARTITION_COLS_PER_SPARSE_FILE - 1};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[0].meta);
  ASSERT_EQ(0, partition_store_partition.i);
  ASSERT_EQ(0, partition_store_partition.j);

  test_partition = {PARTITION_COLS_PER_SPARSE_FILE,
                    PARTITION_COLS_PER_SPARSE_FILE};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[0].meta);
  ASSERT_EQ(1, partition_store_partition.i);
  ASSERT_EQ(1, partition_store_partition.j);

  // test with partition-manager 3 : (1, 1) as well:
  test_partition = {config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        1,
                    config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        1};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[3].meta);
  ASSERT_EQ(0, partition_store_partition.i);
  ASSERT_EQ(0, partition_store_partition.j);

  test_partition = {config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        PARTITION_COLS_PER_SPARSE_FILE - 1,
                    config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        PARTITION_COLS_PER_SPARSE_FILE - 1};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[3].meta);
  ASSERT_EQ(0, partition_store_partition.i);
  ASSERT_EQ(0, partition_store_partition.j);

  test_partition = {config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        PARTITION_COLS_PER_SPARSE_FILE,
                    config.count_rows_partition_stores_per_partition_manager *
                            PARTITION_COLS_PER_SPARSE_FILE +
                        PARTITION_COLS_PER_SPARSE_FILE};
  partition_store_partition =
      core::getPartitionStoreOfPartition(test_partition, arguments[3].meta);
  ASSERT_EQ(1, partition_store_partition.i);
  ASSERT_EQ(1, partition_store_partition.j);

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
          test_partition, partition_store_meta[3][63].global_partition_info);
  ASSERT_EQ(1, partition_inside_partition_store.i);
  ASSERT_EQ(1, partition_inside_partition_store.j);

  config.paths_to_partition.push_back("/mnt/sda1/");
  config.paths_to_partition.push_back("/mnt/sdb1/");
  config.paths_to_partition.push_back("/mnt/sdc1/");
  config.paths_to_partition.push_back("/mnt/sdd1/");

  test_partition = {0, 0};
  std::string meta_partition_file_name =
      core::getMetaPartitionFileName(config, test_partition);
  ASSERT_EQ("/mnt/sda1/000/meta-pt-00000000-00000000.dat",
            meta_partition_file_name);

  test_partition = {0, 1};
  meta_partition_file_name =
      core::getMetaPartitionFileName(config, test_partition);
  ASSERT_EQ("/mnt/sdb1/001/meta-pt-00000000-00000001.dat",
            meta_partition_file_name);

  test_partition = {1, 0};
  meta_partition_file_name =
      core::getMetaPartitionFileName(config, test_partition);
  ASSERT_EQ("/mnt/sdb1/001/meta-pt-00000001-00000000.dat",
            meta_partition_file_name);

  test_partition = {1, 1};
  meta_partition_file_name =
      core::getMetaPartitionFileName(config, test_partition);
  ASSERT_EQ("/mnt/sdc1/002/meta-pt-00000001-00000001.dat",
            meta_partition_file_name);
}

TEST(Util, TestMetaPartitions) {

  // meta-partition-testing
  // edge 1
  edge_t test_edge = {1, 1};

  partition_t meta_partition = core::getMetaPartitionOfEdge(test_edge);
  ASSERT_EQ(0, meta_partition.i);
  ASSERT_EQ(0, meta_partition.j);

  // edge 2
  test_edge = {MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1,
               MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE - 1};

  meta_partition = core::getMetaPartitionOfEdge(test_edge);
  ASSERT_EQ(0, meta_partition.i);
  ASSERT_EQ(0, meta_partition.j);

  // edge 3
  test_edge = {MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE,
               MAX_VERTICES_PER_TILE * PARTITION_COLS_PER_SPARSE_FILE};

  meta_partition = core::getMetaPartitionOfEdge(test_edge);
  ASSERT_EQ(1, meta_partition.i);
  ASSERT_EQ(1, meta_partition.j);
}

TEST(Util, BitShifting) {
  uint64_t large_test_int = 2ul * (UINT32_MAX + 1ul) + 1ul;
  uint64_t upper_32_bits_large_test_int =
      large_test_int & (~((size_t)UINT32_MAX));
  uint32_t large_test_int_compressed =
      large_test_int - upper_32_bits_large_test_int;

  // upper_32_bits_large_test_int
  ASSERT_EQ(2ul * (UINT32_MAX + 1ul), upper_32_bits_large_test_int);
  // large_test_int_compressed
  ASSERT_EQ(1, large_test_int_compressed);

  uint64_t test_int = (1ul << 32) + 1;
  uint32_t lower_bits = test_int & ((size_t)UINT32_MAX);
  char* upper_bits = new char[1];
  upper_bits[0] = 0;
  set_bool_array(upper_bits, 0, (test_int & (1ul << 32)));
  uint64_t output_test_int =
      (uint64_t)lower_bits | ((uint64_t)upper_bits[0] << 32);

  ASSERT_EQ(1, lower_bits);
  ASSERT_EQ(1, upper_bits[0]);
  ASSERT_EQ(output_test_int, test_int);
}
