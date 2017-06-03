#pragma once
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdint.h>
#include <util/arch.h>

namespace scalable_graphs {
namespace util {
  class AtomicCounter {
  public:
    AtomicCounter(int max_contender)
        : max_contender_(max_contender), counter_(0) {
      // do nothing
    }

    uint64_t inc() {
      if (max_contender_ == 1) {
        uint64_t old_counter = counter_;
        ++counter_;
        return old_counter;
      }
      return smp_faa(&counter_, 1);
    }

    uint64_t counter() {
      smp_rmb();
      return counter_;
    }

  private:
    int max_contender_;
    volatile uint64_t counter_ __attribute__((aligned(64)));
  };
}
}
