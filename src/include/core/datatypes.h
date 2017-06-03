#pragma once

#include <string>
#include <vector>
#include <ring_buffer.h>
#if !defined(MOSAIC_HOST_ONLY)
#include <ring_buffer_scif.h>
#endif

#include <core/datatypes_config.h>

// For intermediate usage when reading a graph into partitions
#define INDEX_BITS 16
#define MB 1024ul * 1024ul
#define GB 1024 * MB
#define VERTICES_PER_PARTITION_STRIPE 1024ul

// The count of vertices to step over for the tileprocessors/followers.
#define EDGES_STRIPE_SIZE 16

// #define MAX_VERTICES_PER_TILE 65536ul      // 2**16
// #define MAX_EDGES_PER_TILE 67108864ul      // 2**13 * 2**13 (max is
// 2**16*2**16)
// #define VERTICES_PER_SPARSE_FILE 8388608ul // 2**7 * 2**16 = 2*23
// #define PARTITIONS_PER_SPARSE_FILE 16384ul // 2**7 * 2**7 = 2**14
// #define PARTITION_COLS_PER_SPARSE_FILE 128ul // 2**7

// #define MAX_VERTICES_PER_TILE 65536ul // 2**16
// #define MAX_EDGES_PER_TILE 16777216ul // 2**12 * 2**12 (max is 2**16*2**16)
// #define VERTICES_PER_SPARSE_FILE 16777216ul  // 2**8 * 2**16 = 2*24
// #define PARTITIONS_PER_SPARSE_FILE 65536ul   // 2**8 * 2**8 = 2**16
// #define PARTITION_COLS_PER_SPARSE_FILE 256ul // 2**8

#define MAX_VERTICES_PER_TILE 65536ul      // 2**16
#define MAX_EDGES_PER_TILE 268435456ul     // 2**14 * 2**14 (max is 2**16*2**16)
#define VERTICES_PER_SPARSE_FILE 4194304ul // 2**6 * 2**16 = 2*22
#define PARTITIONS_PER_SPARSE_FILE 4096ul  // 2**6 * 2**6 = 2**12
#define PARTITION_COLS_PER_SPARSE_FILE 64ul // 2**6

#define MAX_EDGES_PER_TILE_IN_MEMORY 268435456ul // 2**14 * 2**14

// for 4MB blocks, batch 2**18 edges:
#define RMAT_GENERATOR_MAX_EDGES_PER_BLOCK 262144
#define RMAT_TILER_MAX_EDGES_PER_ROUND 137438953472 // 2**37
// #define RMAT_TILER_MAX_EDGES_PER_ROUND 268435456 // 2**28

#define RMAT_A 0.57f
#define RMAT_B 0.19f
#define RMAT_C 0.19f
#define RMAT_SEED 2

// alignment for tile reading
// NOTE: TILE_READ_ALIGN must be power of 2
#define TILE_READ_ALIGN (128 * 1024)
#define TILE_READ_ALIGN_MASK (~(TILE_READ_ALIGN - 1))
static_assert((TILE_READ_ALIGN & (TILE_READ_ALIGN - 1)) == 0,
              "TILE_READ_ALIGN must be power of two");

#define MAGIC_IDENTIFIER 11118719669451714817ul

// Sample 1% of all tiles.
#define SAMPLE_THRESHOLD 0.01

// The initial tile break point, will be adaptively modified.
#define INIT_TILE_BREAK_POINT 2500000
#define MAX_TILE_BREAK_POINT 2500000ul
#define MIN_TILE_BREAK_POINT 500000ul

// Do not pin anything on core 1.
#define CORE_OFFSET 1

typedef uint64_t vertex_id_t;
typedef uint16_t local_vertex_id_t;
typedef uint32_t edge_local_id_t;

struct file_edge_t {
  int src;
  int tgt;
};

struct edge_t {
  vertex_id_t src;
  vertex_id_t tgt;
};

struct edge_compact_t {
  uint32_t src;
  uint32_t tgt;
};

struct edge_weighted_t {
  vertex_id_t src;
  vertex_id_t tgt;

  float weight;
};

struct local_edge_t {
  local_vertex_id_t tgt;
  local_vertex_id_t src;

  // sort by target, then by source
  bool operator<(const local_edge_t& other) const {
    if (tgt == other.tgt) {
      return src < other.src;
    }
    return tgt < other.tgt;
  }
};

struct local_edge_weighted_t {
  local_vertex_id_t tgt;
  local_vertex_id_t src;

  float weight;

  // sort by target, then by source
  bool operator<(const local_edge_weighted_t& other) const {
    if (tgt == other.tgt) {
      return src < other.src;
    }
    return tgt < other.tgt;
  }
};

// For interaction with processing-entities
template <typename T>
struct vertex_block_t {
  uint32_t count_vertex;
  T vertex_data[0];
};

template <typename T>
struct global_vertex_block_t {
  uint64_t count_vertex;
  T vertex_data[0];
};

struct vertex_count_t {
  uint16_t count;
  local_vertex_id_t id;
};

struct edge_block_t {
  uint64_t block_id;

  // these three blocks symbolize the edges (weight is only set for weighted
  // graphs):
  // e_0 = (src[0], tgt[0], (weight[0]))
  uint32_t offset_src;    // local_vertex_id_t*
  uint32_t offset_tgt;    // local_vertex_id_t* or vertex_count_t* with RLE
  uint32_t offset_weight; // float*
};

struct edge_block_index_t {
  uint64_t block_id;

  uint32_t count_src_vertices;
  uint32_t count_tgt_vertices;

  // offsets
  uint32_t offset_src_index; // T_VertexIdType*
  uint32_t offset_tgt_index; // T_VertexIdType*

  uint32_t offset_src_index_bit_extension; // bool*
  uint32_t offset_tgt_index_bit_extension; // bool*
};

struct vertex_degree_t {
  uint32_t in_degree;
  uint32_t out_degree;
};

struct vertex_edge_tiles_block_sizes_t {
  size_t count_active_vertex_src_block;
  size_t count_active_vertex_tgt_block;
  size_t size_active_vertex_src_block;
  size_t size_active_vertex_tgt_block;
  size_t size_src_degree_block;
  size_t size_tgt_degree_block;
  size_t size_source_vertex_block;
  size_t size_extension_fields_vertex_block;
};

struct vertex_edge_tiles_block_counts_t {
  uint32_t count_active_vertex_src_block;
  uint32_t count_active_vertex_tgt_block;
  uint32_t count_src_vertex_block;
  uint32_t count_tgt_vertex_block;
};

struct vertex_edge_tiles_block_t {
  // Indicates whether to shutdown, if set any other data is not meant to be
  // read.
  bool shutdown;

  // Indicates whether the execution time of the current tile shall be sampled
  // to allow for adaptive balancing of the tile split point.
  bool sample_execution_time;

  // Magic ID to detect corruption of the block.
  uint64_t magic_identifier;

  uint64_t block_id;
  uint32_t count_active_vertex_src_block;
  uint32_t count_active_vertex_tgt_block;
  uint32_t count_src_vertex_block;
  uint32_t count_tgt_vertex_block;

  // tile processing information
  uint32_t num_tile_partition;
  uint32_t tile_partition_id;

  // offsets
  uint32_t offset_active_vertices_src; // char*
  uint32_t offset_active_vertices_tgt; // char*
  uint32_t offset_src_degrees;         // vertex_degree_t*
  uint32_t offset_tgt_degrees;         // vertex_degree_t*
  uint32_t offset_source_vertex_block; // T*
  uint32_t offset_extensions;          // void*, for algorithm-specific use
} __attribute__((aligned(64)));

struct scenario_stats_t {
  uint64_t count_vertices;
  uint64_t count_tiles;
  bool is_index_32_bits;
  bool is_weighted_graph;
  bool index_33_bit_extension;
};

struct tile_stats_t {
  uint64_t block_id;
  uint32_t count_vertex_src;
  uint32_t count_vertex_tgt;
  uint32_t count_edges;
  // indicates whether the target-block is encoded using run-length-encoding
  // if using RLE, the tgt-block is encoded as an array of vertex_count_t's
  bool use_rle;
};

struct command_line_args_grc_t {
  uint64_t nthreads;
  uint64_t count_partition_managers;
  uint64_t count_vertices;
  bool input_weighted;
  std::string graphname;
  std::string path_to_globals;
  std::vector<std::string> paths_to_partition;
};

// intuition: The current-array is of step T, the next-array of step T+1, they
// will get swapped when step T is done.
template <typename T>
struct vertex_array_t {
  size_t count;
  size_t size_active;

  vertex_degree_t* degrees;

  T* current;
  T* next;

  char* active_current;
  char* active_next;

  // Keep track of the vertices which have changed in the current iteration.
  char* changed;
};

struct processed_vertex_block_t {
  // Indicates whether to shutdown, if set any other data is not meant to be
  // read.
  bool shutdown;

  // Indicates whether the current tile was sampled for determining the edge
  // processing speed.
  bool sample_execution_time;
  // The processing time, in nano seconds.
  size_t processing_time_nano;
  // The number of edges in the sampled tile/tilepartition.
  uint32_t count_edges;

  // Magic ID to detect corruption of the block.
  uint64_t magic_identifier;

  uint64_t block_id;
  uint32_t count_active_vertex_src_block;
  uint32_t count_active_vertex_tgt_block;
  uint32_t count_tgt_vertex_block;

  // offsets
  uint32_t offset_active_vertices_src; // char*
  uint32_t offset_active_vertices_tgt; // char*
  uint32_t offset_vertices;            // T*
};

struct processed_vertex_index_block_t {
  // Indicates whether to shutdown, if set any other data is not meant to be
  // read.
  bool shutdown;

  // Indicates whether the current tile was sampled for determining the edge
  // processing speed.
  bool sample_execution_time;
  // The processing time, in nano seconds.
  size_t processing_time_nano;
  // The number of edges in the sampled tile/tilepartition.
  uint32_t count_edges;

  // When sent by the VertexFetcher to indicate a missing tile due to selective
  // scheduling, allows the GlobalReducer to simply skip over the tile.
  bool dummy;

  uint64_t block_id;
  uint32_t count_src_vertex_block;
  uint32_t count_tgt_vertex_block;
  uint32_t completed;

  // offsets
  uint32_t offset_active_vertices_src; // char*
  uint32_t offset_active_vertices_tgt; // char*
  uint32_t offset_vertices;            // T*
  uint32_t offset_src_indices;         // IndexType*
  uint32_t offset_tgt_indices;         // IndexType*
};

struct fetch_vertices_request_t {
  // Indicates whether to shutdown, if set any other data is not meant to be
  // read.
  bool shutdown;

  uint64_t block_id;
  ring_buffer_t* response_ring_buffer;
  uint32_t count_vertices;

  // offsets
  uint32_t offset_request_vertices;
};

struct fetch_vertices_response_t {
  uint64_t block_id;
  uint32_t global_fetcher_id;
  uint32_t count_vertices;

  // offsets
  uint32_t offset_vertex_responses;
};

struct tile_data_t {
  // Indicates whether the data is currently in use, if false this data can be
  // reclaimed.
  volatile bool data_active;
  // Indicates that the data is ready to be read, i.e. all necessary fields have
  // been written.
  volatile bool data_ready;

  volatile void* bundle_raw;      // raw start of a bundle of tiles
  volatile size_t* bundle_refcnt; // pointer to a batch reference counter
};

struct tile_data_edge_engine_t : public tile_data_t {
  uint32_t fetch_refcnt;
  uint32_t process_refcnt;
  volatile vertex_edge_tiles_block_t* tile_block;
};

struct tile_data_vertex_engine_t : public tile_data_t {
  volatile size_t total_cnt; // total count of partitions
  volatile size_t vr_refcnt; // reference counter for global-reducer
};

template <typename TPointer, typename TMetaData>
struct pointer_offset_t {
  // a bundle of tile
  volatile TPointer* data; // real tile data
  volatile TMetaData meta; // meta data, according to the needs of the user
};

template <typename TPointer, typename TMetaData>
struct pointer_offset_table_t {
  pointer_offset_t<TPointer, TMetaData>* data_info;
};

struct thread_index_t {
  size_t count;
  size_t id;
};

struct partition_meta_t {
  uint32_t count_edges;
};

struct partition_t {
  uint64_t i;
  uint64_t j;
};

struct meta_partition_meta_t {
  partition_t meta_partition_index;
  partition_t vertex_partition_index_start;
};

struct partition_edge_compact_t {
  uint32_t count_edges;
  uint32_t partition_offset_src;
  uint32_t partition_offset_tgt;
  local_edge_t edges[0];
};

struct partition_edge_list_t {
  uint32_t count_edges;
  bool shutdown_indicator;
  edge_t edges[0];
};

struct partition_edge_t {
  uint32_t count_edges;
  edge_t* edges;
};

struct remote_partition_edge_t {
  uint32_t count_edges;
  bool end_of_round;
  edge_t edges[0];
};

struct meta_partition_file_info_t {
  partition_t global_offset;
};

struct partition_manager_arguments_t {
  meta_partition_meta_t meta;
  std::vector<std::string> partitions;
  thread_index_t thread_index;
  ring_buffer_t* write_request_rb;
};

struct edge_write_request_t {
  int fd;
  local_edge_t edge;
  uint64_t offset;
};

struct vertex_area_t {
  uint64_t src_min;
  uint64_t src_max;
  uint64_t tgt_min;
  uint64_t tgt_max;
};

struct active_tiles_t {
  size_t count_active_tiles;
  char active_tiles[0];
};

enum class ProfilingType {
  PT_Duration,
  PT_RingbufferSizes,
};

enum class ComponentType {
  CT_GlobalReducer,
  CT_IndexReader,
  CT_None,
  CT_RingBufferSizes,
  CT_TileProcessor,
  CT_TileReader,
  CT_VertexApplier,
  CT_VertexFetcher,
  CT_VertexReducer,
};

struct profiling_duration_t {
  uint64_t time_start;
  uint64_t time_end;
  uint64_t metadata;
};

struct profiling_ringbuffer_sizes_t {
  uint64_t time;
  size_t size_index_rb;
  size_t size_tiles_rb;
  size_t size_response_rb;
};

struct profiling_data_t {
  // Determines the type of the union subtype.
  ProfilingType type;
  int64_t pid;
  // Support a two level id scheme, discriminating between local and global ids,
  // e.g. for MicId and FetcherId. In case of using only one level of Ids, e.g.
  // the VertexDomain, use the global_id to discriminate components.
  ComponentType component;
  int64_t local_id;
  int64_t global_id;

  union {
    profiling_duration_t duration;
    profiling_ringbuffer_sizes_t ringbuffer_sizes;
  };

  char name[0];
};

struct profiling_transport_t {
  // Indicates whether to shutdown, if set any other data is not meant to be
  // read.
  bool shutdown;

  // Has to be the last member to allow name to be packed in behind.
  profiling_data_t data;
};

enum class PerfEventMode { PEM_Host, PEM_Client };

#define VERTEX_LOCK_TABLE_SIZE 223

struct vertex_lock_table_t {
  size_t count;
  int salt;
  pthread_spinlock_t* locks;
};
