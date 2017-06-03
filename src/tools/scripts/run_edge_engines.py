#!/usr/bin/env python3

import os
import optparse
import utils
import shutil

import config_engine as conf
import topology.cpu_topology as topo

def startSingleEdgeEngine(edge_engine_index, opts):
    mic_index = conf.SG_EDGE_ENGINE_TO_MIC[edge_engine_index]

    datapath_edge = conf.SG_DATAPATH_EDGE_ENGINE[edge_engine_index]

    meta_dir = conf.getMicSubdir(datapath_edge, opts.dataset, "meta", edge_engine_index)
    tile_dir = conf.getMicSubdir(datapath_edge, opts.dataset, "tile", edge_engine_index)
    # the global dir is the same as the meta-dir, we only mount one path on the MIC
    global_dir = conf.getMicSubdir(datapath_edge, opts.dataset, "meta", edge_engine_index)

    in_memory_mode_int = 1 if opts.in_memory_mode else 0

    use_selective_scheduling_int = 1 if conf.SG_ALGORITHM_ENABLE_SELECTIVE_SCHEDULING[opts.algorithm] else 0
    if opts.dataset in conf.SG_DATASET_DISABLE_SELECTIVE_SCHEDULING:
        use_selective_scheduling_int = 0

    perfmon = 1 if opts.perfmon == "True" else 0

    # Set the size of the read tiles rb to the in memory value iff not running on
    # the mic and the in memory mode is activated.
    read_tiles_rb_size = conf.SG_RB_SIZE_READ_TILES
    if opts.in_memory_mode and not opts.run_on_mic:
        read_tiles_rb_size = conf.SG_RB_SIZE_READ_TILES_IN_MEMORY

    perf_events_dir = conf.getPerfEventsDir(opts.dataset)
    # For the MIC, output perf events to the root:
    if opts.run_on_mic:
        perf_events_dir = "~/"

    if opts.enable_perf_event_collection:
        shutil.rmtree(perf_events_dir, True)
        utils.mkdirp(perf_events_dir, conf.FILE_GROUP)
    enable_perf_event_collection_int = 1 if opts.enable_perf_event_collection else 0

    # For pinning, count threads and determine if we need to use smt or not.
    count_tile_readers = conf.SG_NREADER_MIC if opts.run_on_mic else conf.SG_NREADER
    count_tile_processors = conf.SG_NPROCESSOR_MIC if opts.run_on_mic else conf.SG_NPROCESSOR

    edge_engine_per_socket = opts.nmic / topo.NUM_SOCKET

    count_threads_per_edge_engine = opts.count_indexreader + opts.count_vertex_fetcher + opts.count_vertex_reducer + count_tile_readers + count_tile_processors
    count_threads_per_socket = count_threads_per_edge_engine * edge_engine_per_socket + opts.count_globalreducer / topo.NUM_SOCKET

    if opts.local_fetcher_mode == "GlobalFetcher":
        count_threads += opts.count_globalfetcher
    use_smt_int = 1 if count_threads_per_socket >= topo.NUM_PHYSICAL_CPU_PER_SOCKET else 0

    # Disable usage of smt on MIC for now:
    if opts.run_on_mic:
        use_smt_int = 0

    args = ["--port"       , opts.port + edge_engine_index * 100,
            "--algorithm"  , opts.algorithm,
            "--count-reader"    , count_tile_readers,
            "--count-processor" , count_tile_processors,
            "--mic-index"  , edge_engine_index,
            "--in-memory-mode", in_memory_mode_int,
            "--nmic"       , opts.nmic,
            "--run-on-mic"       , opts.run_on_mic,
            "--paths-meta"       , meta_dir,
            "--paths-tile"       , tile_dir,
            "--use-selective-scheduling", use_selective_scheduling_int,
            "--max-iterations", opts.max_iterations,
            "--path-global"      , global_dir,
            "--do-perfmon"       , perfmon,
            "--processed-rb-size" , conf.SG_RB_SIZE_PROCESSED,
            "--read-tiles-rb-size", read_tiles_rb_size,
            "--host-tiles-rb-size", conf.SG_RB_SIZE_HOST_TILES,
            "--tile-processor-mode", opts.tile_processor_mode,
            "--tile-processor-input-mode", opts.tile_processor_input_mode,
            "--tile-processor-output-mode", opts.tile_processor_output_mode,
            "--count-globalreducer", opts.count_globalreducer,
            "--count-globalfetcher", opts.count_globalfetcher,
            "--count-indexreader", opts.count_indexreader,
            "--count-vertex-reducer", opts.count_vertex_reducer,
            "--count-vertex-fetcher", opts.count_vertex_fetcher,
            "--enable-perf-event-collection", enable_perf_event_collection_int,
            "--path-perf-events", perf_events_dir,
            "--local-fetcher-mode", opts.local_fetcher_mode,
            "--use-smt", use_smt_int,
            "--count-followers", opts.count_followers,
            ]

    if opts.run_on_mic:
        # args = ["perf", "stat", "-B", "-e", "cache-references,cache-misses,cycles,instructions", "./edge-engine"] + args
        # args = ["echo"] + args + [">", "run_script"]
        # utils.run_sshpass(opts.print_only, "phi", "root", "mic%s" % (mic_index), *args)
        # # args = ["nohup"] + args + [">", "edge-engine-%d.out" % (edge_engine_index),
        # args = ["nohup", "sh", "run_script", ">", "edge-engine-%d.out" % (edge_engine_index),
        args = ["nohup", "./edge-engine"] + args + [">", "edge-engine-%d.out" % (edge_engine_index),
            "2>", "edge-engine-%d.err" % (edge_engine_index),
            "<", "/dev/null", "&"]

        utils.run_sshpass(opts.print_only, "phi", "root", "mic%s" % (mic_index), *args)
    else:
        if opts.debug:
            b = conf.DBIN_EDGE_ENGINE
        else:
            b = conf.RBIN_EDGE_ENGINE
        
        log_dir = os.path.join(conf.LOG, opts.dataset)
        utils.mkdirp(log_dir, conf.FILE_GROUP)
        out_file = os.path.join(log_dir, "edge-engine-%d.out" % (edge_engine_index))
        err_file = os.path.join(log_dir, "edge-engine-%d.err" % (edge_engine_index))

        if opts.gdb:
            utils.run(opts.print_only, "sudo", "gdb", "--args", b, *args)
        else:
            utils.run_background_output(opts.print_only, out_file, err_file, "sudo", b, *args)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--port", default=conf.SG_PORT)
    parser.add_option("--algorithm", default=conf.SG_ALGORITHM)
    parser.add_option("--nmic", default=conf.SG_NMIC)
    parser.add_option("--dataset", default=conf.SG_DATASET)
    parser.add_option("--tile-processor-mode", default =
            conf.SG_TILE_PROCESSOR_MODE)
    parser.add_option("--tile-processor-input-mode", default =
            conf.SG_TILE_PROCESSOR_INPUT_MODE)
    parser.add_option("--tile-processor-output-mode", default =
            conf.SG_TILE_PROCESSOR_OUTPUT_MODE)
    parser.add_option("--in-memory-mode", default=conf.SG_IN_MEMORY_MODE,
            action="store_true", dest="in_memory_mode")
    parser.add_option("--run-on-mic", default=conf.SG_RUN_ON_MIC,
            action="store_true", dest="run_on_mic")
    parser.add_option("--debug", default=conf.SG_DEBUG, action="store_true",
            dest="debug")
    parser.add_option("--gdb", default=False, action="store_true", dest="gdb")
    parser.add_option("--print-only", default=conf.SG_PRINT_ONLY,
            action="store_true", dest="print_only")
    parser.add_option("--enable-perfmon", action="store_true", dest="perfmon",
            default = conf.SG_PERFMON)
    parser.add_option("--enable-perf-event-collection", action="store_true",
            dest="enable_perf_event_collection", default =
            conf.SG_ENABLE_PERF_EVENT_COLLECTION)
    parser.add_option("--max-iterations", default = conf.SG_MAX_ITERATIONS)
    parser.add_option("--count-vertex-reducer", default = conf.SG_NREDUCER)
    parser.add_option("--count-vertex-fetcher", default = conf.SG_NFETCHER)
    parser.add_option("--count-indexreader", default = conf.SG_NINDEXREADER)
    parser.add_option("--count-globalreducer", default = conf.SG_NGLOBALREDUCER)
    parser.add_option("--count-globalfetcher", default = conf.SG_NGLOBALFETCHER)
    parser.add_option("--local-fetcher-mode", default = conf.SG_LOCAL_FETCHER_MODE)
    parser.add_option("--count-followers", default = conf.SG_COUNT_FOLLOWERS)
    (opts, args) = parser.parse_args()

    print("# Starting ./edge-engine")
    for mic_index in range(0, int(opts.nmic)):
        startSingleEdgeEngine(mic_index, opts)
