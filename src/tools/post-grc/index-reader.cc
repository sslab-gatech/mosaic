#include "index-reader.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

namespace scalable_graphs {
namespace post_grc {
  IndexReader::IndexReader(const std::string& path_to_globals,
                           const std::vector<std::string>& paths_to_meta,
                           size_t count_vertices, int count_tiles,
                           bool is_index_32_bits,
                           const thread_index_t& thread_index)
      : path_to_globals_(path_to_globals), paths_to_meta_(paths_to_meta),
        count_vertices_(count_vertices), count_tiles_(count_tiles),
        is_index_32_bits_(is_index_32_bits), thread_index_(thread_index),
        count(0) {}

  IndexReader::~IndexReader() {
    delete tile_stats_;
    delete tile_offsets_;
    for (size_t i = 0; i < count_vertices_; ++i) {
      delete vertex_to_tiles_index_[i];
    }
    delete[] vertex_to_tiles_index_;
  }

  void IndexReader::init() {
    config_t config;
    config.paths_to_meta = paths_to_meta_;
    config.count_tiles = count_tiles_;
    config.count_edge_processors = thread_index_.count;

    count_tiles_for_current_mic_ =
        core::countTilesPerMic(config, thread_index_.id);

    std::string meta_file_name =
        core::getEdgeTileIndexFileName(config, thread_index_.id);
    meta_fd_ = util::openFileDirectly(meta_file_name);

    tile_stats_ = new tile_stats_t[count_tiles_for_current_mic_];

    std::string global_tile_stats_file_name =
        core::getGlobalTileStatsFileName(config, thread_index_.id);

    size_t size_tile_stats =
        sizeof(tile_stats_t) * count_tiles_for_current_mic_;
    util::readDataFromFile(global_tile_stats_file_name, size_tile_stats,
                           tile_stats_);

    tile_offsets_ = new size_t[count_tiles_for_current_mic_];
    tile_offsets_[0] = 0;

    for (int i = 1; i < count_tiles_for_current_mic_; ++i) {
      // offset is the offset of the previous block plus the size of the
      // previous block:
      tile_stats_t tile_stats = tile_stats_[i - 1];

      // step 2: calculate required space to fetch the index blocks
      size_t size_edge_index_tgt_vertex_block =
          sizeof(uint32_t) * tile_stats.count_vertex_tgt;
      size_t size_edge_index_src_vertex_block =
          sizeof(uint32_t) * tile_stats.count_vertex_src;
      size_t size_edge_index_src_extended_block =
          is_index_32_bits_ ? 0 : size_bool_array(tile_stats.count_vertex_src);
      size_t size_edge_index_tgt_extended_block =
          is_index_32_bits_ ? 0 : size_bool_array(tile_stats.count_vertex_tgt);
      size_t size_index_block = sizeof(edge_block_index_t) +
                                size_edge_index_src_vertex_block +
                                size_edge_index_tgt_vertex_block +
                                size_edge_index_src_extended_block +
                                size_edge_index_tgt_extended_block;
      size_t size_rb_block = int_ceil(size_index_block, PAGE_SIZE);
      tile_offsets_[i] = tile_offsets_[i - 1] + size_rb_block;
    }

    vertex_to_tiles_index_ = new std::vector<uint32_t>*[count_vertices_];
    for (size_t i = 0; i < count_vertices_; ++i) {
      vertex_to_tiles_index_[i] = new std::vector<uint32_t>();
    }
    sg_log("Tiles for reader %lu: %lu\n", thread_index_.id,
           count_tiles_for_current_mic_);
  }

  void IndexReader::run() {
    init();

    size_t size_edge_index_src_vertex_block =
        sizeof(uint32_t) * MAX_VERTICES_PER_TILE;
    size_t size_edge_index_tgt_vertex_block =
        sizeof(uint32_t) * MAX_VERTICES_PER_TILE;
    size_t size_edge_index_src_extended_block =
        is_index_32_bits_ ? 0 : size_bool_array(MAX_VERTICES_PER_TILE);
    size_t size_edge_index_tgt_extended_block =
        is_index_32_bits_ ? 0 : size_bool_array(MAX_VERTICES_PER_TILE);
    size_t size_index_block =
        sizeof(edge_block_index_t) + size_edge_index_src_vertex_block +
        size_edge_index_tgt_vertex_block + size_edge_index_src_extended_block +
        size_edge_index_tgt_extended_block;
    size_t max_size_index_block = int_ceil(size_index_block, PAGE_SIZE);

    edge_block_index_t* index;
    int rc = posix_memalign((void**)&index, 4096, max_size_index_block);
    if (rc != 0) {
      sg_err("posix_memalign failed with %s (%d)\n", strerror(rc), rc);
      util::die(1);
    }

    // one round only
    for (size_t current_tile = 0; current_tile < count_tiles_for_current_mic_;
         ++current_tile) {
      // step 1: find the current tile status
      tile_stats_t tile_stats = tile_stats_[current_tile];

      // step 2: calculate required space to fetch the index blocks
      size_t size_edge_index_src_vertex_block =
          sizeof(uint32_t) * tile_stats.count_vertex_src;
      size_t size_edge_index_tgt_vertex_block =
          sizeof(uint32_t) * tile_stats.count_vertex_tgt;
      size_t size_edge_index_src_extended_block =
          is_index_32_bits_ ? 0 : size_bool_array(tile_stats.count_vertex_src);
      size_t size_edge_index_tgt_extended_block =
          is_index_32_bits_ ? 0 : size_bool_array(tile_stats.count_vertex_tgt);
      size_t size_index_block = sizeof(edge_block_index_t) +
                                size_edge_index_src_vertex_block +
                                size_edge_index_tgt_vertex_block +
                                size_edge_index_src_extended_block +
                                size_edge_index_tgt_extended_block;
      size_t size_rb_block = int_ceil(size_index_block, PAGE_SIZE);

      // step 4: read indices
      util::readFileOffset(meta_fd_, index, size_rb_block,
                           tile_offsets_[current_tile]);

      sg_dbg("Index %lu read (reader_id: %lu)\n", current_tile,
             thread_index_.id);

      uint32_t* edge_block_index_src =
          get_array(uint32_t*, index, index->offset_src_index);
      uint32_t* edge_block_index_tgt =
          get_array(uint32_t*, index, index->offset_tgt_index);

      char* edge_block_index_src_upper_bits =
          get_array(char*, index, index->offset_src_index_bit_extension);
      char* edge_block_index_tgt_upper_bits =
          get_array(char*, index, index->offset_tgt_index_bit_extension);

      uint32_t current_tile_global_id =
          thread_index_.count * current_tile + thread_index_.id;

      for (uint32_t i = 0; i < tile_stats.count_vertex_src; ++i) {
        size_t id;
        if (is_index_32_bits_) {
          id = (size_t)edge_block_index_src[i];
        } else {
          id = (size_t)edge_block_index_src[i] |
               ((size_t)eval_bool_array(edge_block_index_src_upper_bits, i)
                << 32);
        }
        vertex_to_tiles_index_[id]->push_back(current_tile_global_id);
        // ++count;
      }

      // for (uint32_t i = 0; i < tile_stats.count_vertex_tgt; ++i) {
      //   size_t id;
      //   if (is_index_32_bits_) {
      //     id = (size_t)edge_block_index_tgt[i];
      //   } else {
      //     id = (size_t)edge_block_index_tgt[i] |
      //          ((size_t)eval_bool_array(edge_block_index_tgt_upper_bits, i)
      //           << 32);
      //   }
      //   // vertex_to_tiles_index_[id]->push_back(current_tile_global_id);
      //   ++count;
      // }
      if (current_tile % 5000 == 0) {
        sg_log("At tile %lu\n", current_tile);
      }
    }
  }
}
}
