#!/usr/bin/env python3
import os
import optparse
import time
import build
from datetime import datetime
import utils

import config_engine as conf
import topology.cpu_topology as topo

def killMosaic(print_only):
    cmd = "sudo killall mosaic"
    print(cmd)
    if not print_only:
        os.system(cmd)

def startMosaic(opts):
    killMosaic(opts.print_only)
    build.build(False, False)

    meta_dirs = []
    tile_dirs = []
    global_dir = conf.getGlobalsDir(opts.dataset)

    # set up fault-tolerance dir if required
    fault_tolerance_dir = conf.getFaultToleranceDir(opts.dataset)
    if opts.fault_tolerant_mode:
        shutil.rmtree(fault_tolerance_dir, True)
        utils.mkdirp(fault_tolerance_dir, conf.FILE_GROUP)

    perf_events_dir = conf.getPerfEventsDir(opts.dataset)
    if opts.enable_perf_event_collection:
        utils.mkdirp(perf_events_dir, conf.FILE_GROUP)

    for i in range(0, int(opts.nmic)):
        meta_dirs.append(
            conf.getMicSubdir(conf.SG_DATAPATH_VERTEX_ENGINE[i], opts.dataset,
                              "meta", i))
        tile_dirs.append(
            conf.getMicSubdir(conf.SG_DATAPATH_VERTEX_ENGINE[i], opts.dataset,
                              "tile", i))

    in_memory_mode_int = 1 if opts.in_memory_mode else 0

    meta_dirs_string = ":".join(meta_dirs)
    tile_dirs_string = ":".join(tile_dirs)

    enable_tile_partitioning_int = 1 if opts.enable_tile_partitioning else 0
    enable_fault_tolerance_int = 1 if opts.fault_tolerant_mode else 0
    enable_perf_event_collection_int = 1 if opts.enable_perf_event_collection else 0

    # for selective scheduling
    use_selective_scheduling_int = 1 if \
        conf.SG_ALGORITHM_ENABLE_SELECTIVE_SCHEDULING[opts.algorithm] else 0
    if opts.dataset in conf.SG_DATASET_DISABLE_SELECTIVE_SCHEDULING:
        use_selective_scheduling_int = 0

    # For pinning, count threads and determine if we need to use smt or not.
    count_tile_readers = conf.SG_NREADER
    count_tile_processors = conf.SG_NPROCESSOR

    edge_engine_per_socket = opts.nmic / topo.NUM_SOCKET

    count_threads_per_edge_engine = opts.count_indexreader + opts.count_vertex_fetcher + opts.count_vertex_reducer + count_tile_readers + count_tile_processors
    count_threads_per_socket = count_threads_per_edge_engine * edge_engine_per_socket + opts.count_globalreducer / topo.NUM_SOCKET

    use_smt_int = 1 if count_threads_per_socket >= topo.NUM_PHYSICAL_CPU_PER_SOCKET else 0

    # Set the size of the read tiles rb to the in memory value iff not running on
    # the mic and the in memory mode is activated.
    read_tiles_rb_size = conf.SG_RB_SIZE_READ_TILES
    if opts.in_memory_mode:
        read_tiles_rb_size = conf.SG_RB_SIZE_READ_TILES_IN_MEMORY

    args = [
        "--algorithm", opts.algorithm,
        "--max-iterations", opts.max_iterations,
        "--nmic", opts.nmic,
        "--count-applier", opts.count_applier,
        "--count-globalreducer", opts.count_globalreducer,
        "--count-globalfetcher", opts.count_globalfetcher,
        "--count-indexreader", opts.count_indexreader,
        "--count-vertex-reducer", opts.count_vertex_reducer,
        "--count-vertex-fetcher", opts.count_vertex_fetcher,
        "--in-memory-mode", in_memory_mode_int,
        "--paths-meta", meta_dirs_string,
        "--paths-tile", tile_dirs_string,
        "--path-globals", global_dir,
        "--use-selective-scheduling", use_selective_scheduling_int,
        "--path-fault-tolerance-output", fault_tolerance_dir,
        "--enable-fault-tolerance", enable_fault_tolerance_int,
        "--enable-tile-partitioning", enable_tile_partitioning_int,
        "--count-tile-reader", count_tile_readers,
        "--local-fetcher-mode", opts.local_fetcher_mode,
        "--global-fetcher-mode", opts.global_fetcher_mode,
        "--enable-perf-event-collection", enable_perf_event_collection_int,
        "--path-perf-events", perf_events_dir,
        "--count-tile-processors", count_tile_processors,
        "--use-smt", use_smt_int,
        "--host-tiles-rb-size", conf.SG_RB_SIZE_HOST_TILES,
        "--local-reducer-mode", opts.local_reducer_mode,
        "--processed-rb-size", conf.SG_RB_SIZE_PROCESSED,
        "--read-tiles-rb-size", read_tiles_rb_size,
        "--tile-processor-mode", opts.tile_processor_mode,
        "--tile-processor-input-mode", opts.tile_processor_input_mode,
        "--tile-processor-output-mode", opts.tile_processor_output_mode,
        "--count-followers", opts.count_followers,
    ]

    if opts.enable_log:
        log_dir = os.path.join(conf.LOG_ROOT, (
            conf.getWeightedName(opts.dataset,
                                 conf.SG_ALGORITHM_WEIGHTED[opts.algorithm])))
        utils.mkdirp(log_dir, conf.FILE_GROUP)
        args = args + ["--log", log_dir]

    if opts.debug:
        b = conf.DBIN_MOSAIC
    else:
        b = conf.RBIN_MOSAIC

    # We need sudo for scif
    args = [b] + args

    if opts.gdb:
        args = ["gdb", "--args"] + args

    # We need sudo for scif
    # args = ["sudo", "LD_LIBRARY_PATH=/usr/lib64/:$LD_LIBRARY_PATH"] + args
    # args = ["sudo", "valgrind"] + args
    if opts.run == "perfstat":
        args = ["perf", "stat", "-B", "-e",
                "cache-references,cache-misses,cycles,instructions,branches,faults,migrations"] + args
    if opts.run == "likwid":
        max_cpu_id = multiprocessing.cpu_count() - 1
        args = ["likwid-perfctr", "-f", "-g", "NUMA", "-g", "L2", "-g",
                "L2CACHE", "-g", "BRANCH", "-g", "CYCLE_ACTIVITY", "-g", "L3",
                "-g", "L3CACHE", "-c", "0-%d" % max_cpu_id] + args

    args = ["sudo"] + args

    if not opts.print_only:
        if opts.gdb:
            utils.run(opts.print_only, *args)
        else:
            out_file = utils.getVertexEngineLogName(opts)
            utils.run_output(opts.print_only, out_file, *args)


def run(cmd, print_only):
    print(cmd)
    if not print_only:
        os.system(cmd)


if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--algorithm", default=conf.SG_ALGORITHM)
    parser.add_option("--max-iterations", default=conf.SG_MAX_ITERATIONS)
    parser.add_option("--count-applier", default=conf.SG_NAPPLIER)
    parser.add_option("--count-globalreducer", default=conf.SG_NGLOBALREDUCER)
    parser.add_option("--count-globalfetcher", default=conf.SG_NGLOBALFETCHER)
    parser.add_option("--local-fetcher-mode",
                      default=conf.SG_LOCAL_FETCHER_MODE)
    parser.add_option("--local-reducer-mode",
                      default=conf.SG_LOCAL_REDUCER_MODE)
    parser.add_option("--global-fetcher-mode",
                      default=conf.SG_GLOBAL_FETCHER_MODE)
    parser.add_option("--enable-tile-partitioning",
                      default=conf.SG_ENABLE_TILE_PARTITIONING)
    parser.add_option("--count-indexreader", default=conf.SG_NINDEXREADER)
    parser.add_option("--count-vertex-reducer", default=conf.SG_NREDUCER)
    parser.add_option("--count-vertex-fetcher", default=conf.SG_NFETCHER)
    parser.add_option("--nmic", default=conf.SG_NMIC)
    parser.add_option("--dataset", default=conf.SG_DATASET)
    parser.add_option("--enable-perf-event-collection", action="store_true",
                      dest="enable_perf_event_collection",
                      default=conf.SG_ENABLE_PERF_EVENT_COLLECTION)
    parser.add_option("--in-memory-mode", default=conf.SG_IN_MEMORY_MODE,
                      action="store_true", dest="in_memory_mode")
    parser.add_option("--debug", action="store_true", dest="debug",
                      default=conf.SG_DEBUG)
    parser.add_option("--print-only", action="store_true", dest="print_only",
                      default=conf.SG_PRINT_ONLY)
    parser.add_option("--enable-log", action="store_true", dest="enable_log",
                      default=conf.SG_ENABLE_LOG)
    parser.add_option("--gdb", action="store_true", dest="gdb",
                      default=conf.SG_GDB_VERTEX_ENGINE)
    parser.add_option("--benchmark-mode", action="store_true",
                      dest="benchmark_mode", default=False)
    parser.add_option("--fault-tolerant-mode", action="store_true",
                      dest="fault_tolerant_mode",
                      default=conf.SG_ENABLE_FAULT_TOLERANCE)
    parser.add_option("--tile-processor-mode",
                      default=conf.SG_TILE_PROCESSOR_MODE)
    parser.add_option("--tile-processor-input-mode",
                      default=conf.SG_TILE_PROCESSOR_INPUT_MODE)
    parser.add_option("--tile-processor-output-mode",
                      default=conf.SG_TILE_PROCESSOR_OUTPUT_MODE)
    parser.add_option("--count-followers", default=conf.SG_COUNT_FOLLOWERS)
    parser.add_option("--run", default="")
    parser.add_option("--run-on-mic", default=False)
    (opts, args) = parser.parse_args()

    print("# Starting vertex-engine")
    startMosaic(opts)
