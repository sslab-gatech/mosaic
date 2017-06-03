#include <vector>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <getopt.h>
#include <malloc.h>
#include <pthread.h>

#include <ring_buffer.h>
#include <ring_buffer_scif.h>
#include <util/util.h>

#include "defines.h"

struct cmd_t {
  int base_port;
  int count_rbs;
  int count_host;
  int block_size;
  bool run_on_mic;
};

struct thread_args_t {
  ring_buffer_scif_t* rb;
  int block_size;
};

struct ring_buffer_elm_t {
  unsigned int __size;            /* internal aligned size */
  unsigned short padding;         /* padding size for alignment */
  volatile unsigned short status; /* status flags of an element */
} __attribute__((__packed__));

size_t count_blocks_sent = 0;
size_t count_blocks_corrupted = 0;
size_t count_dma_errors = 0;
const static int val = 0xff;

static int parse_option(int argc, char* argv[], cmd_t& cmd) {
  static struct option options[] = {
      {"base_port", required_argument, 0, 'a'},
      {"count_rbs", required_argument, 0, 'b'},
      {"count_host", required_argument, 0, 'c'},
      {"run_on_mic", required_argument, 0, 'd'},
      {"block_size", required_argument, 0, 'e'},

      {0, 0, 0, 0},
  };

  int arg_cnt;
  int c;
  int idx;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    c = getopt_long(argc, argv, "a:b:c:d:e:", options, &idx);
    if (c == -1)
      break;
    switch (c) {
    case 'a':
      cmd.base_port = atoi(optarg);
      break;
    case 'b':
      cmd.count_rbs = atoi(optarg);
      break;
    case 'c':
      cmd.count_host = atoi(optarg);
      break;
    case 'd':
      cmd.run_on_mic = (atoi(optarg) == 1) ? true : false;
      break;
    case 'e':
      cmd.block_size = atoi(optarg);
      break;
    default:
      return -EINVAL;
    }
  }
  return arg_cnt;
}

void* put_rb_content(void* arguments) {
  thread_args_t* arg = (thread_args_t*)arguments;
  ring_buffer_scif_t* rb = arg->rb;

  ring_buffer_req_t tiles_req;
  size_t size_block = arg->block_size * 1024;
  // uint8_t* block = (uint8_t*)malloc(size_block);

  uint8_t* block;
  int rc = posix_memalign((void**)&block, PAGE_SIZE, size_block);
  if (rc) {
    printf("RC memalign: %d\n", rc);
    exit(1);
  }

  memset(block, val, size_block);
  block[0] = 0;

  if (block[0] != 0) {
    printf("Corrupt 0: %d %p\n", block[0], block);
    exit(1);
  }
  if (block[1] != val) {
    printf("Corrupt 1: %d %p\n", block[1], block);
    exit(1);
  }

  while (true) {
    ring_buffer_put_req_init(&tiles_req, BLOCKING, size_block);
    ring_buffer_scif_put(rb, &tiles_req);
    sg_rb_check(&tiles_req);

    int rc = copy_to_ring_buffer_scif(rb, tiles_req.data, block, size_block);
    if (rc) {
      smp_faa(&count_dma_errors, 1);
      // while (rc) {
      //   int rc =
      //       copy_to_ring_buffer_scif(rb, tiles_req.data, block, size_block);
      // }
      // printf("Fixed\n");
    }
    // Check if byte is 0.
    // uint8_t* block_remote = (uint8_t*)tiles_req.data;
    // assert((size_t)block_remote % 64 == 0);
    // ring_buffer_elm_t* elm =
    //     (ring_buffer_elm_t*)(block_remote - sizeof(ring_buffer_elm_t));

    // assert(elm->__size == (4160));

    // while (block_remote[0] != 0) {
    // printf("Waiting\n");
    // }

    // if (block_remote[0] != 0) {
    //   smp_faa(&count_blocks_corrupted, 1);
    //   printf("Corrupt 0: %d %p\n", block_remote[0], block_remote);
    // }
    // if (block_remote[1] != val) {
    //   printf("Corrupt 1: %d %p\n", block_remote[1], block_remote);
    // }

    ring_buffer_scif_elm_set_ready(rb, tiles_req.data);

    size_t current_count = smp_faa(&count_blocks_sent, 1);
    if (current_count % 1000 == 0) {
      printf("Count: %zu Errors: %zu Corrupted: %zu\n", current_count,
             count_dma_errors, count_blocks_corrupted);
    }
  }
}

int main(int argc, char* argv[]) {
  cmd_t cmd;
  int arg_count = parse_option(argc, argv, cmd);
  if (arg_count != 5) {
    printf("Wrong argument count: %d\n", arg_count);
    return 1;
  }

  std::vector<ring_buffer_scif_t> rbs;

  int node_id = cmd.run_on_mic ? 1 : 0;
  for (int i = 0; i < cmd.count_rbs; ++i) {
    int port = cmd.base_port + 100 * i;
    // Increment ID by 1 to run on mic, use host (0) otherwise.
    ring_buffer_scif_t rb;
    int rc = ring_buffer_scif_create_shadow(port + 1, node_id, port, NULL, NULL,
                                            &rb);
    if (rc) {
      printf("RC: %d\n", rc);
      return 1;
    }

    rbs.push_back(rb);
  }
  printf("Connected to all ringbuffers\n");

  std::vector<pthread_t> threads;

  for (int i = 0; i < cmd.count_rbs; ++i) {
    for (int j = 0; j < cmd.count_host; ++j) {
      thread_args_t* args = new thread_args_t;
      args->rb = &rbs[i];
      args->block_size = cmd.block_size;

      pthread_t t;
      pthread_create(&t, NULL, put_rb_content, (void*)args);
      threads.push_back(t);
    }
  }

  for (auto t : threads) {
    pthread_join(t, NULL);
  }

  return 0;
}
