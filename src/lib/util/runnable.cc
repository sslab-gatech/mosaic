#include <unistd.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <util/runnable.h>
#include <util/util.h>

namespace scalable_graphs {
namespace util {

  cpu_id_t& cpu_id_t::nextNearest(bool use_smt) {
    // increase smt
    if (use_smt) {
      ++this->smt;
      if (this->smt < SMT_LEVEL) {
        return *this;
      }
      this->smt = 0;
    } else {
      this->smt = ANYWHERE;
    }

    // carry over smt
    ++this->pcpu;
    if (this->pcpu < NUM_PHYSICAL_CPU_PER_SOCKET) {
      return *this;
    }
    this->pcpu = 0;

    // carry over pcpu
    ++this->socket;
    if (this->socket < NUM_SOCKET) {
      return *this;
    }

    // carry over socket
    this->socket = 0;
    return *this;
  }

  Runnable::Runnable()
      : cpu_id_(cpu_id_t::ANYWHERE, cpu_id_t::ANYWHERE, cpu_id_t::ANYWHERE) {
    // zero out cpuset
    CPU_ZERO(&cpuset_);
  }

  Runnable::~Runnable() {
    // do nothing
  }

  void Runnable::setAffinity(cpu_id_t& cpu_id) {
    int os_cpu_id;
    cpu_id_ = cpu_id;

    // initially, a thread can go anywhere: {*, *, *}
    CPU_ZERO(&cpuset_);

    // if nothing specified, do nothing
    if (cpu_id.socket == cpu_id_t::ANYWHERE) {
      return;
    }
    sg_assert(0 <= cpu_id.socket && cpu_id.socket < NUM_SOCKET,
              "socket_id is out of bound");

    // socket_id is specified
    if (cpu_id.pcpu == cpu_id_t::ANYWHERE) {
      // enumerate all cpus in {socket_id, *, *}
      for (int p = 0; p < NUM_PHYSICAL_CPU_PER_SOCKET; ++p) {
        for (int m = 0; m < SMT_LEVEL; ++m) {
          os_cpu_id = OS_CPU_ID[cpu_id.socket][p][m];
          CPU_SET(os_cpu_id, &cpuset_);
        }
      }
      return;
    }
    sg_assert(0 <= cpu_id.pcpu && cpu_id.pcpu < NUM_PHYSICAL_CPU_PER_SOCKET,
              "pcpu_id is out of bound");

    // socket_id and pcpu_id are specified
    if (cpu_id.smt == cpu_id_t::ANYWHERE) {
      // enumerate all cpus in {socket_id, pcpu_id, *}
      for (int m = 0; m < SMT_LEVEL; ++m) {
        os_cpu_id = OS_CPU_ID[cpu_id.socket][cpu_id.pcpu][m];
        CPU_SET(os_cpu_id, &cpuset_);
      }
      return;
    }
    sg_assert(0 <= cpu_id.smt && cpu_id.smt < SMT_LEVEL,
              "pcpu_id is out of bound");

    // socket_id, pcpu_id, and smt_id are specified
    os_cpu_id = OS_CPU_ID[cpu_id.socket][cpu_id.pcpu][cpu_id.smt];
    CPU_SET(os_cpu_id, &cpuset_);
  }

  void Runnable::setHighestPriority() {
    pid_t tid = syscall(__NR_gettid);
    int ret = setpriority(PRIO_PROCESS, tid, -19);
    if (ret != 0) {
      sg_err("Fail to set thread scheduling parameters: %d!\n", ret);
      die(1);
    }
  }

  int Runnable::start() { return pthread_create(&t_, NULL, threadMain, this); }

  void Runnable::setName(const std::string& name) {
    pthread_setname_np(t_, name.c_str());
  }

  void Runnable::join() { pthread_join(t_, NULL); }

  void* Runnable::threadMain(void* arg) {
    Runnable* runnable = static_cast<Runnable*>(arg);
    cpu_set_t* cpuset = &runnable->cpuset_;
    if (CPU_COUNT(cpuset)) {
      int rc = sched_setaffinity(0, sizeof(*cpuset), cpuset);
      if (rc) {
        sg_err("Fail to pin a thread: %s\n", strerror(errno));
        die(1);
      }
    }
    runnable->run();
  }
}
}
