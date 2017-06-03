#include <vector>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include <getopt.h>
#include <malloc.h>
#include <pthread.h>

#include <ring_buffer.h>
#include <util/util.h>

#include "defines.h"

struct cmd_t {
  int count_put;
  int count_get;
  int block_size;
};

struct thread_args_t {
  ring_buffer_t* rb;
  int block_size;
};

int count_blocks_sent = 0;
int count_blocks_received = 0;
int count_blocks_corrupted = 0;

static int parse_option(int argc, char* argv[], cmd_t& cmd) {
  static struct option options[] = {
      {"count_put", required_argument, 0, 'a'},
      {"count_get", required_argument, 0, 'b'},
      {"block_size", required_argument, 0, 'c'},

      {0, 0, 0, 0},
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
      cmd.count_put = atoi(optarg);
      break;
    case 'b':
      cmd.count_get = atoi(optarg);
      break;
    case 'c':
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
  ring_buffer_t* rb = arg->rb;

  ring_buffer_req_t tiles_req;
  size_t size_block = arg->block_size * 1024;
  void* block;
  int rc = posix_memalign(&block, PAGE_SIZE, size_block);
  if (rc) {
    printf("RC memalign: %d\n", rc);
    exit(1);
  }

  uint8_t* block_cast = (uint8_t*)block;
  memset(block, 255, size_block);
  block_cast[0] = 0;

  while (true) {
    ring_buffer_put_req_init(&tiles_req, BLOCKING, size_block);
    ring_buffer_put(rb, &tiles_req);
    sg_rb_check(&tiles_req);

    copy_to_ring_buffer(rb, tiles_req.data, block, size_block);
    // Check if byte is 0.
    uint8_t* block_remote = (uint8_t*)tiles_req.data;

    // while (block_remote[0] != 0) {
    // printf("Waiting\n");
    // }

    if (block_remote[0] != 0) {
      printf("Corrupt: %d %p\n", block_remote[0], block_remote);
    }

    ring_buffer_elm_set_ready(rb, tiles_req.data);

    int current_count = smp_faa(&count_blocks_sent, 1);
    if (current_count % 1000 == 0) {
      printf("Count: %d\n", current_count);
    }
  }
}

void* get_rb_content(void* args) {
  ring_buffer_t* rb = (ring_buffer_t*)args;

  ring_buffer_req_t request_tiles;
  while (true) {
    ring_buffer_get_req_init(&request_tiles, BLOCKING);
    ring_buffer_get(rb, &request_tiles);
    sg_rb_check(&request_tiles);

    uint8_t* block = (uint8_t*)request_tiles.data;
    if (block[0] != 0) {
      // printf("Block not Zero: %d %p\n", block[0], request_tiles.data);
      smp_faa(&count_blocks_corrupted, 1);
    }

    ring_buffer_elm_set_done(rb, request_tiles.data);
    // printf("Received\n");
    int current_count = smp_faa(&count_blocks_received, 1);
    if (current_count % 1000 == 0) {
      printf("Count: %d Corrupted: %d\n", current_count,
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

  ring_buffer_t* rb;
  size_t size_rb = 512 * 1024 * 1024;

  int rc = ring_buffer_create(size_rb, PAGE_SIZE, RING_BUFFER_BLOCKING, NULL,
                              NULL, &rb);
  if (rc) {
    printf("RC: %d\n", rc);
    return 1;
  }

  printf("Connected to all ringbuffers\n");

  std::vector<pthread_t> threads;

  for (int i = 0; i < cmd.count_put; ++i) {
    thread_args_t* args = new thread_args_t;
    args->rb = rb;
    args->block_size = cmd.block_size;

    pthread_t t;
    pthread_create(&t, NULL, put_rb_content, (void*)args);
    threads.push_back(t);
  }

  for (int i = 0; i < cmd.count_get; ++i) {
    pthread_t t;
    pthread_create(&t, NULL, get_rb_content, (void*)rb);
    threads.push_back(t);
  }

  for (auto t : threads) {
    pthread_join(t, NULL);
  }

  return 0;
}
