#include <string>
#include <vector>

#define MAX_EDGE_ENGINES 16

#if defined(MOSAIC_HOST_ONLY)
typedef ring_buffer_t* ring_buffer_type;
#else
typedef ring_buffer_scif_t ring_buffer_type;
#endif
enum class grc_tile_traversals_t { Hilbert, ColumnFirst, RowFirst };

enum class LocalReducerMode {
  // The default mode, exclusive and partitioned access to the global array
  // using the global reducers.
  LRM_GlobalReducer,
  // Lock parts of the array with a global lock array.
  LRM_Locking,
  // Use atomics to discriminate accesses from each other.
  LRM_Atomic,
  // Do nothing, simply consume the array and return immediately.
  LRM_LocalReducer_Noop,
};

enum class GlobalReducerMode {
  // Normal mode.
  GRM_Active,
  // Do nothing, simply consume the array and return immediately.
  GRM_Noop,
};

enum class LocalFetcherMode {
  // Use GlobalFetcher to obtain value.
  LFM_GlobalFetcher,
  // Fetch value directly, without GlobalFetcher.
  LFM_DirectAccess,
  // Use a constant value for filling the source vertices array as well as the
  // other necessary blocks (e.g. degrees).
  LFM_ConstantValue,
  // Use a fake local fetcher, transmits empty vertex blocks to be "consumed" by
  // the tile processor.
  LFM_Fake,
};

enum class GlobalFetcherMode {
  // Normal mode.
  GFM_Active,
  // Use a constant value for filling the source vertices array.
  GFM_ConstantValue,
};

enum class RmatGeneratorPhase { RGP_GenerateVertexDegrees, RGP_GenerateTiles };

enum class TileProcessorMode {
  // Normal mode.
  TPM_Active,
  // Do nothing, simply return.
  TPM_Noop,
};

enum class TileProcessorInputMode {
  // Normal mode: Input from VertexFetcher.
  TPIM_VertexFetcher,
  // The VertexFetcher transmits empty blocks, use fake input for the
  // TileProcessor.
  TPIM_FakeVertexFetcher,
  // Input is a fake value array, VertexFetcher is inactive.
  TPIM_ConstantValue,
};

enum class TileProcessorOutputMode {
  // Write back result to the VertexReducer.
  TPOM_VertexReducer,
  // Only allocate and send result, do not write into block but use a constant
  // value.
  TPOM_FakeVertexReducer,
  // Do nothing, no allocation, no write back, disables reducers.
  TPOM_Noop,
};

struct ringbuffer_config_t {
  ring_buffer_type response_rb;
  ring_buffer_type tiles_data_rb;
  ring_buffer_type active_tiles_rb;
};

struct config_t {
  size_t count_tiles;
  // For selective scheduling.
  int count_tile_readers;
  // For global shutdown.
  int count_tile_processors;
  int count_vertex_reducers;

  int count_vertex_fetchers;
  int count_global_reducers;
  int count_global_fetchers;
  int count_index_readers;
  // General options.
  int count_edge_processors;
  int max_iterations;
  int port;
  bool is_index_32_bits;
  bool is_graph_weighted;
  bool in_memory_mode;
  bool run_on_mic;
  bool use_smt;
  bool use_selective_scheduling;
  bool do_perfmon;
  bool enable_perf_event_collection;
  bool enable_local_reducer;
  bool enable_local_fetcher;

  // The size of the rb for receiving tile information from the host.
  size_t host_tiles_rb_size;

  LocalFetcherMode local_fetcher_mode;

  std::string path_to_globals;
  std::string algorithm;
  std::string path_to_perf_events;
  std::vector<std::string> paths_to_meta;
  std::vector<std::string> paths_to_tile;
};

struct config_vertex_domain_t : public config_t {
  size_t count_vertices;
  int count_vertex_appliers;
  bool enable_fault_tolerance;
  bool enable_tile_partitioning;
  bool enable_index_reader;
  LocalReducerMode local_reducer_mode;
  GlobalReducerMode global_reducer_mode;
  GlobalFetcherMode global_fetcher_mode;
  std::string fault_tolerance_ouput_path;
  std::string path_to_log;
  std::vector<int> edge_engine_to_mic;
  ringbuffer_config_t ringbuffer_configs[MAX_EDGE_ENGINES];
};

struct config_edge_processor_t : public config_t {
  // NOTE
  // - The stock Xeon Phi kernel on mic starts to kill
  //   user processes when mmap-ed memory size is over 3.7 GB.
  //   For now, let's keep the ring buffer size below the threshold.
  //   We need to change the Xeon Phi's OOM killer policy.

  // The size of the rb to send processed tiles back to the host.
  size_t processed_rb_size;
  // The size of the rb to read tiles from disk into.
  size_t read_tiles_rb_size;
  int mic_index;
  // The number of followers per TileProcessor.
  int count_followers;
  TileProcessorMode tile_processor_mode;
  TileProcessorInputMode tile_processor_input_mode;
  TileProcessorOutputMode tile_processor_output_mode;
};

enum class PartitionMode {
  PM_InMemoryMode, PM_FileBackedMode
};

struct config_grc_t {
  size_t count_rows_partitions;
  size_t count_rows_meta_partitions;
  size_t count_rows_partition_stores_per_partition_manager;
  size_t count_rows_vertices_per_partition_manager;
  size_t count_vertices;
  size_t nthreads;
  size_t count_partition_managers;
  size_t count_rows_partition_managers;
  bool input_weighted;
  bool enable_tile_reader;
  bool enable_tile_processing;
  std::string graphname;
  std::string path_to_globals;
  std::vector<std::string> paths_to_partition;
  PartitionMode partition_mode;

};

struct config_tiler_t : public config_grc_t {
  bool output_weighted;
  bool use_rle;
  grc_tile_traversals_t traversal;
  std::vector<std::string> paths_to_meta;
  std::vector<std::string> paths_to_tile;
};

struct scenario_settings_t {
  uint64_t count_vertices;
  uint64_t rmat_count_edges;
  char delimiter;
};

struct config_partitioner_t : public config_grc_t {
  uint32_t count_write_threads;
  uint64_t rmat_count_edges;
  scenario_settings_t settings;
  std::string source;
  bool use_original_ids;
};

struct config_rmat_tiler_t : public config_tiler_t {
  uint64_t count_edges;
  int base_port;
  int count_edge_generators;
  RmatGeneratorPhase generator_phase;
  bool run_on_mic;
};

struct config_remote_rmat_generator_t {
  uint64_t edges_id_start;
  uint64_t edges_id_end;
  uint64_t count_vertices;
  int port;
  uint64_t count_partition_managers;
  int count_threads;
  RmatGeneratorPhase generator_phase;
};
