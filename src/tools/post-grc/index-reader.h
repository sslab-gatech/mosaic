#pragma once

#include <vector>
#include <unordered_map>

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#include <util/runnable.h>
#include <core/datatypes.h>
#include <core/util.h>

namespace scalable_graphs {
namespace post_grc {
  class IndexReader : public scalable_graphs::util::Runnable {
  public:
    IndexReader(const std::string& path_to_globals,
                const std::vector<std::string>& path_to_meta,
                size_t count_vertices, int count_tiles, bool is_index_32_bits,
                const thread_index_t& thread_index);

    ~IndexReader();

    void init();

  public:
    std::vector<uint32_t>** vertex_to_tiles_index_;
    size_t count;

  private:
    virtual void run();

  private:
    std::string path_to_globals_;
    std::vector<std::string> paths_to_meta_;
    size_t count_vertices_;
    int count_tiles_;
    size_t count_tiles_for_current_mic_;
    int meta_fd_;
    tile_stats_t* tile_stats_;
    size_t* tile_offsets_;
    bool is_index_32_bits_;
    thread_index_t thread_index_;
  };
}
}
