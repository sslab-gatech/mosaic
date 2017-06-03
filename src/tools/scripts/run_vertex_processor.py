#!/usr/bin/env python3

import os
import optparse
import utils
import math
import tempfile
import subprocess
import numpy
import shutil
import multiprocessing

import config_engine as conf
import topology.cpu_topology as topo

def _exec_cmd(cmd, out=None):
    p = subprocess.Popen(cmd, shell=True, stdout=out, stderr=out)
    return p

def startVertexEngine(opts):

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
        meta_dirs.append(conf.getMicSubdir(conf.SG_DATAPATH_VERTEX_ENGINE[i], opts.dataset, "meta", i))
        tile_dirs.append(conf.getMicSubdir(conf.SG_DATAPATH_VERTEX_ENGINE[i], opts.dataset, "tile", i))

    run_on_mic_arg = "1" if opts.run_on_mic else "0"

    in_memory_mode_int = 1 if opts.in_memory_mode else 0

    meta_dirs_string = ":".join(meta_dirs)
    tile_dirs_string = ":".join(tile_dirs)

    edge_engine_to_mic_string = ['%s' % str(a) for a in conf.SG_EDGE_ENGINE_TO_MIC]

    edge_engine_to_mic_arg = ":".join(edge_engine_to_mic_string)

    enable_tile_partitioning_int = 1 if opts.enable_tile_partitioning else 0
    enable_fault_tolerance_int = 1 if opts.fault_tolerant_mode else 0
    enable_perf_event_collection_int = 1 if opts.enable_perf_event_collection else 0
    perfmon = 1 if opts.perfmon == "True" else 0

#for selective scheduling
    use_selective_scheduling_int = 1 if conf.SG_ALGORITHM_ENABLE_SELECTIVE_SCHEDULING[opts.algorithm] else 0
    if opts.dataset in conf.SG_DATASET_DISABLE_SELECTIVE_SCHEDULING:
        use_selective_scheduling_int = 0

# For pinning, count threads and determine if we need to use smt or not.
    count_tile_readers = conf.SG_NREADER_MIC if opts.run_on_mic else conf.SG_NREADER
    count_tile_processors = conf.SG_NPROCESSOR_MIC if opts.run_on_mic else conf.SG_NPROCESSOR

    edge_engine_per_socket = opts.nmic / topo.NUM_SOCKET

    count_threads_per_edge_engine = opts.count_indexreader + opts.count_vertex_fetcher + opts.count_vertex_reducer + count_tile_readers + count_tile_processors
    count_threads_per_socket = count_threads_per_edge_engine * edge_engine_per_socket + opts.count_globalreducer / topo.NUM_SOCKET

    if opts.local_fetcher_mode == "GlobalFetcher":
        count_threads += opts.count_globalfetcher
    use_smt_int = 1 if count_threads_per_socket >= topo.NUM_PHYSICAL_CPU_PER_SOCKET else 0

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
            "--count-tile-reader", count_tile_readers,
            "--count-tile-processors", count_tile_processors,
            "--in-memory-mode", in_memory_mode_int,
            "--port", opts.port,
            "--run-on-mic", run_on_mic_arg,
            "--paths-meta", meta_dirs_string,
            "--paths-tile", tile_dirs_string,
            "--path-global", global_dir,
            "--edge-engine-to-mic", edge_engine_to_mic_arg,
            "--use-selective-scheduling", use_selective_scheduling_int,
            "--path-fault-tolerance-output", fault_tolerance_dir,
            "--enable-fault-tolerance", enable_fault_tolerance_int,
            "--enable-tile-partitioning", enable_tile_partitioning_int,
            "--do-perfmon", perfmon,
            "--local-fetcher-mode", opts.local_fetcher_mode,
            "--local-reducer-mode", opts.local_reducer_mode,
            "--global-fetcher-mode", opts.global_fetcher_mode,
            "--enable-perf-event-collection", enable_perf_event_collection_int,
            "--path-perf-events", perf_events_dir,
            "--use-smt", use_smt_int,
            "--host-tiles-rb-size", conf.SG_RB_SIZE_HOST_TILES,
            ]

    if opts.enable_log:
        log_dir = os.path.join(conf.LOG_ROOT, (conf.getWeightedName(opts.dataset, conf.SG_ALGORITHM_WEIGHTED[opts.algorithm])))
        utils.mkdirp(log_dir, conf.FILE_GROUP)
        args = args + ["--log", log_dir]

    if opts.debug:
        b = conf.DBIN_VERTEX_ENGINE
    else:
        b = conf.RBIN_VERTEX_ENGINE

    # We need sudo for scif
    args = [b] + args

    if opts.gdb:
        args = ["gdb", "--args"] + args

    # We need sudo for scif
    # args = ["sudo", "LD_LIBRARY_PATH=/usr/lib64/:$LD_LIBRARY_PATH"] + args
    # args = ["sudo", "valgrind"] + args
    if opts.run == "perfstat":
        args = ["perf", "stat", "-B", "-e", "cache-references,cache-misses,cycles,instructions,branches,faults,migrations"] + args
    if opts.run == "likwid":
        max_cpu_id = multiprocessing.cpu_count() - 1
        args = ["likwid-perfctr", "-f", "-g", "NUMA", "-g", "L2", "-g", "L2CACHE", "-g", "BRANCH", "-g", "CYCLE_ACTIVITY", "-g", "L3", "-g", "L3CACHE", "-c", "0-%d" % max_cpu_id] + args

    args = ["sudo"] + args

    if not opts.print_only:
      if opts.benchmark_mode:
        with open(os.path.join(conf.LOG_ROOT, 'benchmark.dat'), 'a+') as f:
          values = []
          sargs = ['"%s"' % str(a) for a in args]
          cmd = " ".join(sargs)
          p = _exec_cmd(cmd, subprocess.PIPE)
          target_str = "Time for iteration "
          finish_str = "Finished with execution!"
          flag_continue = True
          while flag_continue:
              for l in p.stderr.readlines():
                  l_str = str(l)
                  # print(l_str.strip())
                  id_end = l_str.find(finish_str)
                  if id_end is not -1:
                    flag_continue = False
                    break

                  id_target = l_str.find(target_str)
                  if id_target is not -1:
                      performance_str = l_str[id_target+len(target_str):]
                      string_parts = performance_str.strip().split(' ')
                      iteration = int(string_parts[0].strip())
                      iteration_time = float(string_parts[1].rstrip('\\n\''))
                      values.append(iteration_time)

          print(values)
          sum_values = numpy.sum(numpy.array(values))
          median = numpy.median(numpy.array(values))
          mean = numpy.mean(numpy.array(values))
          stderr = numpy.std(numpy.array(values))
          f.write("\t\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (opts.algorithm,
                  opts.dataset, len(values),
                  sum_values, mean, median, stderr))
      else:
        out_file = utils.getVertexEngineLogName(opts)

        if opts.gdb:
            utils.run(opts.print_only, *args)
        else:
            utils.run_output(opts.print_only, out_file, *args)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--port", default = conf.SG_PORT)
    parser.add_option("--algorithm", default = conf.SG_ALGORITHM)
    parser.add_option("--max-iterations", default = conf.SG_MAX_ITERATIONS)
    parser.add_option("--count-applier", default = conf.SG_NAPPLIER)
    parser.add_option("--count-globalreducer", default = conf.SG_NGLOBALREDUCER)
    parser.add_option("--count-globalfetcher", default = conf.SG_NGLOBALFETCHER)
    parser.add_option("--local-fetcher-mode", default = conf.SG_LOCAL_FETCHER_MODE)
    parser.add_option("--local-reducer-mode", default = conf.SG_LOCAL_REDUCER_MODE)
    parser.add_option("--global-fetcher-mode", default = conf.SG_GLOBAL_FETCHER_MODE)
    parser.add_option("--enable-tile-partitioning", default = conf.SG_ENABLE_TILE_PARTITIONING)
    parser.add_option("--count-indexreader", default = conf.SG_NINDEXREADER)
    parser.add_option("--count-vertex-reducer", default = conf.SG_NREDUCER)
    parser.add_option("--count-vertex-fetcher", default = conf.SG_NFETCHER)
    parser.add_option("--nmic", default = conf.SG_NMIC)
    parser.add_option("--dataset", default = conf.SG_DATASET)
    parser.add_option("--enable-perf-event-collection", action="store_true", dest="enable_perf_event_collection", default = conf.SG_ENABLE_PERF_EVENT_COLLECTION)
    parser.add_option("--in-memory-mode", default=conf.SG_IN_MEMORY_MODE, action="store_true", dest="in_memory_mode")
    parser.add_option("--run-on-mic", action="store_true", dest="run_on_mic", default = conf.SG_RUN_ON_MIC)
    parser.add_option("--debug", action="store_true", dest="debug", default = conf.SG_DEBUG)
    parser.add_option("--print-only", action="store_true", dest="print_only", default = conf.SG_PRINT_ONLY)
    parser.add_option("--enable-log", action="store_true", dest="enable_log", default = conf.SG_ENABLE_LOG)
    parser.add_option("--gdb", action="store_true", dest="gdb", default = conf.SG_GDB_VERTEX_ENGINE)
    parser.add_option("--benchmark-mode", action="store_true", dest="benchmark_mode", default = False)
    parser.add_option("--fault-tolerant-mode", action="store_true", dest="fault_tolerant_mode", default = conf.SG_ENABLE_FAULT_TOLERANCE)
    parser.add_option("--enable-perfmon", action="store_true", dest="perfmon", default = conf.SG_PERFMON)
    parser.add_option("--run", default = "")
    (opts, args) = parser.parse_args()

    print("# Starting vertex-engine")
    startVertexEngine(opts)

