#pragma once
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <string>
#include <sched.h>
#include <pthread.h>
#include <limits.h>
#include <util/cpu_topology.h>

namespace scalable_graphs {
namespace util {
  struct cpu_id_t {
    unsigned int socket;
    unsigned int pcpu;
    unsigned int smt;

    enum {
      ANYWHERE = UINT_MAX,
    };

    cpu_id_t(int s, int p, int m) : socket(s), pcpu(p), smt(m) {}

    cpu_id_t& nextNearest(bool use_smt);
  };

  class Runnable {
  public:
    Runnable();
    virtual ~Runnable();
    void setAffinity(cpu_id_t& cpu_id);
    void setHighestPriority();
    int start();
    void setName(const std::string& name);
    void join();

  public:
    cpu_id_t cpu_id_;

  protected:
    virtual void run() = 0;

  private:
    static void* threadMain(void* arg);

  private:
    cpu_set_t cpuset_;
    pthread_t t_;
  };
}
}
