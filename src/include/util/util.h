#pragma once

#include <cmath>
#include <string>
#include <sstream>
#include <vector>
#include <fstream>
#include <unordered_map>

#include <sys/time.h>
#include <time.h>

#include <core/datatypes.h>

// add this for type_MAX
#ifndef UINT64_MAX
#define UINT64_MAX __UINT64_MAX__
#endif
#ifndef UINT32_MAX
#define UINT32_MAX __UINT32_MAX__
#endif

namespace scalable_graphs {
namespace util {
  std::vector<std::string> splitDirPaths(const std::string& s);

  std::string prepareDirPath(const std::string& s);

  uint64_t getFileSize(std::string file);

  void writeDataToFile(const std::string& file_name, const void* data,
                       size_t size);

  void writeDataToFileSync(const std::string& file_name, const void* data,
                           size_t size);

  void writeDataToFileDirectly(const std::string& file_name, const void* data,
                               size_t size);

  void appendDataToFile(const std::string& file_name, const void* data,
                        size_t size);

  void readDataFromFile(const std::string& file_name, size_t size, void* data);

  void readDataFromFileDirectly(const std::string& file_name, size_t size,
                                void* data);

  int openFileDirectly(const std::string& file_name);

  void readFileOffset(int fd, void* buf, size_t count, size_t offset);

  void writeFileOffset(int fd, void* buf, size_t count, size_t offset);

  std::string getFilenameForProfilingData(const config_t& config,
                                          PerfEventMode mode, uint64_t id);

  FILE* initFileProfilingData(const config_t& config, PerfEventMode mode,
                              uint64_t id);

  void writeProfilingDuration(const profiling_data_t& data, FILE* file);

  void writeRingBufferSizes(const profiling_data_t& data, FILE* file);

  void __die(int rc, const char* func, int line);

  std::vector<int> splitToIntVector(std::string input);

  // Hash strategy adopted from Ligra:
  // https://github.com/jshun/ligra/blob/master/utils/rMatGraph.C
  inline uint32_t hash(uint32_t a) {
    a = (a + 0x7ed55d16) + (a << 12);
    a = (a ^ 0xc761c23c) ^ (a >> 19);
    a = (a + 0x165667b1) + (a << 5);
    a = (a + 0xd3a2646c) ^ (a << 9);
    a = (a + 0xfd7046c5) + (a << 3);
    a = (a ^ 0xb55a4f09) ^ (a >> 16);
    return a;
  }

  inline uint64_t hash(uint64_t a) {
    a = (a + 0x7ed55d166bef7a1d) + (a << 12);
    a = (a ^ 0xc761c23c510fa2dd) ^ (a >> 9);
    a = (a + 0x165667b183a9c0e1) + (a << 59);
    a = (a + 0xd3a2646cab3487e3) ^ (a << 49);
    a = (a + 0xfd7046c5ef9ab54c) + (a << 3);
    a = (a ^ 0xb55a4f090dd4a67b) ^ (a >> 32);
    return a;
  }

  inline double hashDouble(uint64_t i) {
    return ((double)(hash((uint64_t)i)) / ((double)UINT64_MAX));
  }

  inline uint64_t get_time_usec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_usec + 1000000 * tv.tv_sec);
  }

  inline uint64_t get_time_nsec() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ts.tv_nsec + 1000000000 * ts.tv_sec);
  }

  template <typename T>
  int log2Up(T i) {
    int a = 0;
    T b = i - 1;
    while (b > 0) {
      b = b >> 1;
      ++a;
    }
    return a;
  }

#define die(__rc) __die(__rc, __func__, __LINE__)
}

#define sg_rb_check(__req)                                                     \
  if ((__req)->rc != 0) {                                                      \
    sg_dbg("Ringbuffer returned %d\n", (__req)->rc);                           \
    scalable_graphs::util::die(1);                                             \
  }

#define int_ceil(__val, __ceil_to) ((__val + __ceil_to - 1) & ~(__ceil_to - 1))

#define get_array(__type, __base, __offset)                                    \
  ((__type)((uint8_t*)__base + __offset))

#define size_bool_array(__count) std::ceil(__count / 8.0)

#define eval_bool_array(__array, __index)                                      \
  ((__array[__index / 8] >> (__index % 8)) & 1)

#define set_bool_array(__array, __index, __val)                                \
  if (__val) {                                                                 \
    __array[__index / 8] |= 1 << (__index % 8);                                \
  } else {                                                                     \
    __array[__index / 8] &= ~(1 << (__index % 8));                             \
  }

#define get_time_diff(tv1, tv2)                                                \
  (((tv2.tv_sec) - (tv1.tv_sec)) * 1000000 + (tv2.tv_usec) - (tv1.tv_usec))

#ifndef SCALABLE_GRAPHS_DEBUG
#define sg_assert(__cond, __msg)
#define sg_dbg(__fmt, ...)
#define sg_print(__fmt)
#define sg_here()
#else
#define sg_assert(__cond, __msg)                                               \
  if (!(__cond)) {                                                             \
    int* __p = NULL;                                                           \
    fprintf(stderr, "[SG-ASSERT:%s:%d] %s\n", __func__, __LINE__, __msg);      \
    scalable_graphs::util::die(__cond);                                        \
    *__p = 0;                                                                  \
  }
#define sg_print(__fmt)                                                        \
  fprintf(stdout, "[SG-PRT:%s:%d] " __fmt, __func__, __LINE__)
#define sg_dbg(__fmt, ...)                                                     \
  fprintf(stdout, "[SG-DBG:%s:%d] " __fmt, __func__, __LINE__, __VA_ARGS__)
#define sg_here() fprintf(stderr, "[SG-HERE:%s:%d] <== \n", __func__, __LINE__)
#endif /* SCALABLE_GRAPHS_DEBUG */

#define sg_log(__fmt, ...) fprintf(stderr, "[SG-LOG] " __fmt, __VA_ARGS__)
#define sg_log2(__fmt) fprintf(stderr, "[SG-LOG] " __fmt)

#define sg_mon(__fmt, ...) fprintf(stderr, "[SG-MON] " __fmt, __VA_ARGS__)

#define sg_err(__fmt, ...)                                                     \
  fprintf(stderr, "[SG-ERR:%s:%d] " __fmt, __func__, __LINE__, __VA_ARGS__)
}

#define scoped_profile_tid(__component, __identifier, __tile_id)               \
  PerfEventScoped perf_event(                                                  \
      PerfEventManager::getInstance(config_)->getRingBuffer(), __identifier,   \
      __component, thread_index_.id, ctx_.edge_engine_index_, 0, (__tile_id),  \
      config_.enable_perf_event_collection);

#define scoped_profile_tid_meta(__component, __identifier, __tile_id, __meta)  \
  PerfEventScoped perf_event(                                                  \
      PerfEventManager::getInstance(config_)->getRingBuffer(), __identifier,   \
      __component, thread_index_.id, ctx_.edge_engine_index_, (__meta),        \
      (__tile_id), config_.enable_perf_event_collection);

#define scoped_profile(__component, __identifier)                              \
  PerfEventScoped perf_event(                                                  \
      PerfEventManager::getInstance(config_)->getRingBuffer(), __identifier,   \
      __component, thread_index_.id, ctx_.edge_engine_index_, 0, tile_id,      \
      config_.enable_perf_event_collection);

#define scoped_profile_meta(__component, __identifier, __meta)                 \
  PerfEventScoped perf_event(                                                  \
      PerfEventManager::getInstance(config_)->getRingBuffer(), __identifier,   \
      __component, thread_index_.id, ctx_.edge_engine_index_, (__meta),        \
      tile_id, config_.enable_perf_event_collection);
