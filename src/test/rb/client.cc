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
  int port;
  int count_rbs;
  int count_clients;
};

struct thread_args_t {
  ring_buffer_scif_t* rb;
};

size_t count_blocks_received = 0;
size_t count_blocks_corrupted = 0;
const static int val = 0xff;

static int parse_option(int argc, char* argv[], cmd_t& cmd) {
  static struct option options[] = {
      {"port", required_argument, 0, 'a'},
      {"count_clients", required_argument, 0, 'b'},
      {"count_rbs", required_argument, 0, 'c'},

      {0, 0},
  };

  int arg_cnt;
  int c;
  int idx;

  for (arg_cnt = 0; 1; ++arg_cnt) {
    c = getopt_long(argc, argv, "a:b:c:", options, &idx);
    if (c == -1)
      break;
    switch (c) {
    case 'a':
      cmd.port = atoi(optarg);
      break;
    case 'b':
      cmd.count_clients = atoi(optarg);
      break;
    case 'c':
      cmd.count_rbs = atoi(optarg);
      break;
    default:
      return -EINVAL;
    }
  }
  return arg_cnt;
}

void* get_rb_content(void* arguments) {
  thread_args_t* args = (thread_args_t*)arguments;
  ring_buffer_scif_t* rb = args->rb;

  ring_buffer_req_t request_tiles;
  while (true) {
    ring_buffer_get_req_init(&request_tiles, BLOCKING);
    ring_buffer_scif_get(rb, &request_tiles);
    sg_rb_check(&request_tiles);

    uint8_t* block = (uint8_t*)request_tiles.data;
    assert((size_t)block % 64 == 0);
    // printf("%p\n", request_tiles.data);
    // return NULL;
    if (block[0] != 0 || block[1] != val) {
      printf("Block not Zero: %d %p\n", block[0], request_tiles.data);
      smp_faa(&count_blocks_corrupted, 1);
      assert(0);
    }

    block[0] = val;

    ring_buffer_scif_elm_set_done(rb, request_tiles.data);
    // printf("Received\n");
    size_t current_count = smp_faa(&count_blocks_received, 1);
    if (current_count % 1000 == 0) {
      printf("Count: %zu Corrupted: %zu\n", current_count,
             count_blocks_corrupted);
    }
  }
}

int main(int argc, char* argv[]) {
  cmd_t cmd;
  int arg_count = parse_option(argc, argv, cmd);
  if (arg_count != 3) {
    printf("Wrong argument count: %d\n", arg_count);
    return 1;
  }

  size_t rb_size = 512 * 1024 * 1024;
  std::vector<ring_buffer_scif_t> rbs;

  for (int i = 0; i < cmd.count_rbs; ++i) {
    ring_buffer_scif_t rb;
    int rc = ring_buffer_scif_create_master(
        rb_size, L1D_CACHELINE_SIZE, RING_BUFFER_BLOCKING,
        RING_BUFFER_SCIF_CONSUMER, NULL, NULL, &rb);
    if (rc) {
      printf("RC: %d\n", rc);
      return 1;
    }
    rbs.push_back(rb);
  }

  for (int i = 0; i < cmd.count_rbs; ++i) {
    int port = cmd.port + 100 * i;
    //   then, wait for shadow connection asynchronously
    ring_buffer_scif_wait_for_shadow(&rbs[i], port, true);
    printf("Connected rb %d\n", i);
  }

  std::vector<pthread_t> threads;

  for (int i = 0; i < cmd.count_rbs; ++i) {
    for (int j = 0; j < cmd.count_clients; ++j) {
      pthread_t t;
      thread_args_t* args = new thread_args_t;
      args->rb = &rbs[i];
      pthread_create(&t, NULL, get_rb_content, (void*)args);
      threads.push_back(t);
    }
  }

  for (auto t : threads) {
    pthread_join(t, NULL);
  }

  return 0;
}
