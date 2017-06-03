#if defined(CLANG_COMPLETE_ONLY) || defined(__JETBRAINS_IDE__)
#include "global-fetcher.h"
#endif
#pragma once

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <cmath>
#include <pthread.h>
#include <sys/stat.h>
#include <core/datatypes.h>
#include <core/util.h>
#include <util/arch.h>
#include <util/util.h>

#define DO_PROCESSING_GF 1

namespace scalable_graphs {
namespace core {

  template <class APP, typename TVertexType, typename TVertexIdType>
  GlobalFetcher<APP, TVertexType, TVertexIdType>::GlobalFetcher(
      VertexDomain<APP, TVertexType, TVertexIdType>& ctx,
      vertex_array_t<TVertexType>* vertices, const thread_index_t& thread_index)
      : ctx_(ctx), vertices_(vertices), thread_index_(thread_index) {
    int rc = ring_buffer_create(request_rb_size_, PAGE_SIZE,
                                RING_BUFFER_BLOCKING, NULL, NULL, &request_rb_);
    if (rc) {
      scalable_graphs::util::die(1);
    }
    // do nothing
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  GlobalFetcher<APP, TVertexType, TVertexIdType>::~GlobalFetcher() {
    // do nothing
  }

  template <class APP, typename TVertexType, typename TVertexIdType>
  void GlobalFetcher<APP, TVertexType, TVertexIdType>::run() {
    // Wait for all memory inits to be done.
    int barrier_rc = pthread_barrier_wait(&ctx_.memory_init_barrier_);

    if (barrier_rc == PTHREAD_BARRIER_SERIAL_THREAD) {
      ctx_.initTimers();
    }

    // wait for requests
    ring_buffer_req_t request_fetch;
    ring_buffer_req_t request_response;

    size_t max_response_size =
        sizeof(fetch_vertices_response_t) + sizeof(TVertexType) * UINT16_MAX;
    fetch_vertices_response_t* local_response =
        (fetch_vertices_response_t*)malloc(max_response_size);
    // set global properties
    local_response->offset_vertex_responses = sizeof(fetch_vertices_response_t);
    local_response->global_fetcher_id = thread_index_.id;

    TVertexType* local_response_vertices = get_array(
        TVertexType*, local_response, local_response->offset_vertex_responses);

    while (true) {
      sg_print("Waiting for fetch-request to global-array\n");
      ring_buffer_get_req_init(&request_fetch, BLOCKING);
      ring_buffer_get(request_rb_, &request_fetch);
      sg_rb_check(&request_fetch);
      sg_print("Got request to fetch vertices\n");

      fetch_vertices_request_t* fetch_request =
          (fetch_vertices_request_t*)request_fetch.data;

      local_response->count_vertices = fetch_request->count_vertices;
      local_response->block_id = fetch_request->block_id;

      // allocate response
      size_t size_response =
          sizeof(fetch_vertices_response_t) +
          sizeof(TVertexType) * fetch_request->count_vertices;

      ring_buffer_put_req_init(&request_response, BLOCKING, size_response);
      ring_buffer_put(fetch_request->response_ring_buffer, &request_response);
      sg_rb_check(&request_response);
      sg_dbg("Allocated response to fetch vertices into for block %lu\n",
             fetch_request->block_id);

      TVertexIdType* vertices =
          get_array(TVertexIdType*, fetch_request,
                    fetch_request->offset_request_vertices);

      if (ctx_.config_.global_fetcher_mode == GlobalFetcherMode::GFM_Active) {
        for (uint32_t i = 0; i < fetch_request->count_vertices; ++i) {
          local_response_vertices[i] = vertices_->current[vertices[i]];
        }
      } else if (ctx_.config_.global_fetcher_mode ==
                 GlobalFetcherMode::GFM_ConstantValue) {
        for (uint32_t i = 0; i < fetch_request->count_vertices; ++i) {
          local_response_vertices[i] = 0.5;
        }
      }

      sg_dbg("Sending back response to fetch-request for block %lu \n",
             fetch_request->block_id);
      ring_buffer_elm_set_done(request_rb_, request_fetch.data);
      copy_to_ring_buffer(fetch_request->response_ring_buffer,
                          request_response.data, local_response, size_response);
      ring_buffer_elm_set_ready(fetch_request->response_ring_buffer,
                                request_response.data);
#if !DO_PROCESSING_GF
      ring_buffer_elm_set_done(fetch_request->response_ring_buffer,
                               request_response.data);
#endif
    }
  }
}
}
