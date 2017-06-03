#include "gtest/gtest.h"
#include <core/tile-processor.h>
#include <core/edge-processor.h>
#include "../lib/core/algorithms/pagerank.h"
#include <core/datatypes.h>

namespace scalable_graphs {
namespace core {
  class TileProcessorTest : public ::testing::Test {

  protected:
    TileProcessorTest()
        : edge_processor_(config_edge_processor_t()),
          tile_processor_(edge_processor_, thread_index_t()) {}
    virtual ~TileProcessorTest() {}

    EdgeProcessor<PageRank, float, false> edge_processor_;
    TileProcessor<PageRank, float, false> tile_processor_;
  };

  TEST_F(TileProcessorTest, GetRleOffset) {
    edge_block_t* edge_block = (edge_block_t*)malloc(
        sizeof(edge_block_t) + sizeof(vertex_count_t) * 4);
    // set up target block directly behind the header
    edge_block->offset_tgt = sizeof(edge_block_t);
    vertex_count_t* tgt_block_rle =
        get_array(vertex_count_t*, edge_block, edge_block->offset_tgt);
    tgt_block_rle[0].count = 2;
    tgt_block_rle[1].count = 1;
    // use up all potential targets, trigger wrap around
    tgt_block_rle[2].count = static_cast<uint16_t>(65536);
    tgt_block_rle[3].count = 2;

    uint32_t tgt_count;
    int start = 0;
    tile_processor_.edge_block_ = edge_block;
    uint32_t rle_offset = tile_processor_.get_rle_offset(start, tgt_count);
    ASSERT_EQ(rle_offset, 0);
    ASSERT_EQ(tgt_count, 0);

    start = 1;
    rle_offset = tile_processor_.get_rle_offset(start, tgt_count);
    ASSERT_EQ(rle_offset, 0);
    ASSERT_EQ(tgt_count, 1);

    start = 2;
    rle_offset = tile_processor_.get_rle_offset(start, tgt_count);
    EXPECT_EQ(rle_offset, 1);
    ASSERT_EQ(tgt_count, 0);

    start = 3;
    rle_offset = tile_processor_.get_rle_offset(start, tgt_count);
    ASSERT_EQ(rle_offset, 2);
    ASSERT_EQ(tgt_count, 0);

    start = 65537;
    rle_offset = tile_processor_.get_rle_offset(start, tgt_count);
    ASSERT_EQ(rle_offset, 2);
    ASSERT_EQ(tgt_count, 65534);

    start = 65538;
    rle_offset = tile_processor_.get_rle_offset(start, tgt_count);
    ASSERT_EQ(rle_offset, 2);
    ASSERT_EQ(tgt_count, 65535);

    start = 65539;
    rle_offset = tile_processor_.get_rle_offset(start, tgt_count);
    ASSERT_EQ(rle_offset, 3);
    ASSERT_EQ(tgt_count, 0);

    start = 65540;
    rle_offset = tile_processor_.get_rle_offset(start, tgt_count);
    ASSERT_EQ(rle_offset, 3);
    ASSERT_EQ(tgt_count, 1);

    free(edge_block);
  }

  TEST_F(TileProcessorTest, ProcessEdgesRangeList) {
    // Build 6 edges, use Pagerank to calculate the result.
    // This represents the same graph as the test-graph.
    edge_block_t* edge_block = (edge_block_t*)malloc(
        sizeof(edge_block_t) + sizeof(local_vertex_id_t) * 6 +
        sizeof(local_vertex_id_t) * 6);
    // Set up source block directly behind the header, target block behind the
    // source block.
    edge_block->offset_src = sizeof(edge_block_t);
    edge_block->offset_tgt =
        edge_block->offset_src + sizeof(local_vertex_id_t) * 6;

    local_vertex_id_t* src_block =
        get_array(local_vertex_id_t*, edge_block, edge_block->offset_src);
    local_vertex_id_t* tgt_block =
        get_array(local_vertex_id_t*, edge_block, edge_block->offset_tgt);

    // Set up the edges.
    src_block[0] = 0;
    tgt_block[0] = 0;

    src_block[1] = 2;
    tgt_block[1] = 0;

    src_block[2] = 0;
    tgt_block[2] = 1;

    src_block[3] = 1;
    tgt_block[3] = 1;

    src_block[4] = 1;
    tgt_block[4] = 2;

    src_block[5] = 2;
    tgt_block[5] = 3;

    // Set up the src-degree array.
    vertex_degree_t* src_degrees = new vertex_degree_t[3];

    src_degrees[0].out_degree = 2;
    src_degrees[1].out_degree = 2;
    src_degrees[2].out_degree = 2;

    // Also set up the src_vertices and tgt_vertices.
    float* src_vertices = new float[3];
    float* tgt_vertices = new float[4];

    // Set up the standard value as inputs.
    src_vertices[0] = 0.15;
    src_vertices[1] = 0.15;
    src_vertices[2] = 0.15;

    // Set neutral value for output.
    tgt_vertices[0] = 0.0;
    tgt_vertices[1] = 0.0;
    tgt_vertices[2] = 0.0;
    tgt_vertices[3] = 0.0;

    // Set up all local fields for the TileProcessor.
    tile_processor_.edge_block_ = edge_block;
    tile_processor_.src_degrees_ = src_degrees;
    tile_processor_.src_vertices_ = src_vertices;
    tile_processor_.tgt_vertices_ = tgt_vertices;

    // Now process the edges set up.
    tile_processor_.process_edges_range_list(0, 6);

    // And check for the correct calculation. We use a deviation of at most
    // 10**-4 to check for double "equality".

    // For tgt 0, global 1, orig 2: 0.15/2 + 0.15/2 = 0.15
    ASSERT_NEAR(0.15, tile_processor_.tgt_vertices_[0], 0.0001);
    // For tgt 1, global 2, orig 4: 0.15/2 + 0.15/2 = 0.15
    ASSERT_NEAR(0.15, tile_processor_.tgt_vertices_[1], 0.0001);
    // For tgt 2, global 3, orig 3: 0.15/2 = 0.075
    ASSERT_NEAR(0.075, tile_processor_.tgt_vertices_[2], 0.0001);
    // For tgt 3, global 0, orig 1: 0.15/2 = 0.075
    ASSERT_NEAR(0.075, tile_processor_.tgt_vertices_[3], 0.0001);

    free(edge_block);
    delete[] src_degrees;
    delete[] src_vertices;
    delete[] tgt_vertices;
  }

  TEST_F(TileProcessorTest, ProcessEdgesRangeRle) {
    // Build 6 edges, use Pagerank to calculate the result.
    // This represents the same graph as the test-graph.
    // Now, we are using the RLE-encoded version of the target-block.
    edge_block_t* edge_block = (edge_block_t*)malloc(
        sizeof(edge_block_t) + sizeof(local_vertex_id_t) * 6 +
        sizeof(vertex_count_t) * 4);
    // Set up source block directly behind the header, target block behind the
    // source block.
    edge_block->offset_src = sizeof(edge_block_t);
    edge_block->offset_tgt =
        edge_block->offset_src + sizeof(local_vertex_id_t) * 6;

    local_vertex_id_t* src_block =
        get_array(local_vertex_id_t*, edge_block, edge_block->offset_src);
    vertex_count_t* tgt_block =
        get_array(vertex_count_t*, edge_block, edge_block->offset_tgt);

    // Set up the edges.
    src_block[0] = 0;
    src_block[1] = 2;
    src_block[2] = 0;
    src_block[3] = 1;
    src_block[4] = 1;
    src_block[5] = 2;

    tgt_block[0].count = 2;
    tgt_block[0].id = 0;

    tgt_block[1].count = 2;
    tgt_block[1].id = 1;

    tgt_block[2].count = 1;
    tgt_block[2].id = 2;

    tgt_block[3].count = 1;
    tgt_block[3].id = 3;

    // Set up the src-degree array.
    vertex_degree_t* src_degrees = new vertex_degree_t[3];

    src_degrees[0].out_degree = 2;
    src_degrees[1].out_degree = 2;
    src_degrees[2].out_degree = 2;

    // Also set up the src_vertices and tgt_vertices.
    float* src_vertices = new float[3];
    float* tgt_vertices = new float[4];

    // Set up the standard value as inputs.
    src_vertices[0] = 0.15;
    src_vertices[1] = 0.15;
    src_vertices[2] = 0.15;

    // Set neutral value for output.
    tgt_vertices[0] = 0.0;
    tgt_vertices[1] = 0.0;
    tgt_vertices[2] = 0.0;
    tgt_vertices[3] = 0.0;

    // Set up all local fields for the TileProcessor.
    tile_processor_.edge_block_ = edge_block;
    tile_processor_.src_degrees_ = src_degrees;
    tile_processor_.src_vertices_ = src_vertices;
    tile_processor_.tgt_vertices_ = tgt_vertices;

    // Now process the edges set up.
    tile_processor_.process_edges_range_rle(0, 6);

    // And check for the correct calculation. We use a deviation of at most
    // 10**-4 to check for double "equality".

    // For tgt 0, global 1, orig 2: 0.15/2 + 0.15/2 = 0.15
    ASSERT_NEAR(0.15, tile_processor_.tgt_vertices_[0], 0.0001);
    // For tgt 1, global 2, orig 4: 0.15/2 + 0.15/2 = 0.15
    ASSERT_NEAR(0.15, tile_processor_.tgt_vertices_[1], 0.0001);
    // For tgt 2, global 3, orig 3: 0.15/2 = 0.075
    ASSERT_NEAR(0.075, tile_processor_.tgt_vertices_[2], 0.0001);
    // For tgt 3, global 0, orig 1: 0.15/2 = 0.075
    ASSERT_NEAR(0.075, tile_processor_.tgt_vertices_[3], 0.0001);

    free(edge_block);
    delete[] src_degrees;
    delete[] src_vertices;
    delete[] tgt_vertices;
  }
}
}
