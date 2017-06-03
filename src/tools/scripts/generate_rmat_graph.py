#!/usr/bin/env python2

import os
import time
import sys
import glob
import struct
import optparse
import math
import utils
import shutil
import build

import config_engine as conf

def generate_graph(opts):
    def run(args):
        utils.run(opts.print_only, "time", *args)

    # make sure proper config is loaded
    run(["sudo", "./startup-config"])
    # kill everything first:
    cmd = "sudo killall grc-rmat-generator grc-rmat-tiler"
    os.system(cmd)

    if opts.run_on_mic:
        for mic_index in range(0, opts.nmic):
            cmd = "sshpass -p phi ssh root@mic%s killall grc-rmat-generator" % (mic_index)
            print(cmd)
            os.system(cmd)

    if opts.debug:
        grc_tiler = conf.DBIN_RMAT_TILER
        grc_edge_generator = conf.DBIN_RMAT_GENERATOR
    else:
        grc_tiler = conf.RBIN_RMAT_TILER
        grc_edge_generator = conf.RBIN_RMAT_GENERATOR

    # copy binary if running on mic:
    if opts.run_on_mic:
        if opts.debug:
            grc_edge_generator = os.path.join(conf.BUILD_ROOT, "Debug-k1om/tools/grc/grc-rmat-generator")
        else:
            grc_edge_generator = os.path.join(conf.BUILD_ROOT, "Release-k1om/tools/grc/grc-rmat-generator")
        for mic_index in range(0, opts.nmic):
            cmd = "sshpass -p phi scp %s root@mic%s:" % (grc_edge_generator, mic_index)
            os.system(cmd)

    # populate hashed dirs
    num_dir = conf.SG_NUM_HASH_DIRS

    partition_dirs = []
    meta_dirs = []
    tile_dirs = []
    global_dir = conf.getGlobalsDir(opts.dataset, False)

    # remove in degree-generation-phase:
    if opts.phase == "generate_vertex_degrees":
        shutil.rmtree(global_dir, True)
        utils.mkdirp(global_dir, conf.FILE_GROUP)

    for i in range(0, len(conf.SG_GRC_PARTITION_DIRS)):
        partition_dir = conf.getGrcPartitionDir(opts.dataset, i, False)
        meta_dir = conf.getGrcMetaDir(opts.dataset, i, False)
        tile_dir = conf.getGrcTileDir(opts.dataset, i, False)

        # remove in degree-generation-phase:
        if opts.phase == "generate_vertex_degrees":
            shutil.rmtree(partition_dir, True)
            utils.mkdirp(partition_dir, conf.FILE_GROUP)
            utils.populate_hash_dirs(num_dir, partition_dir)

        shutil.rmtree(meta_dir, True)
        shutil.rmtree(tile_dir, True)

        utils.mkdirp(meta_dir, conf.FILE_GROUP)
        utils.mkdirp(tile_dir, conf.FILE_GROUP)

        utils.populate_hash_dirs(num_dir, meta_dir)
        utils.populate_hash_dirs(num_dir, tile_dir)

        partition_dirs.append(partition_dir)
        meta_dirs.append(meta_dir)
        tile_dirs.append(tile_dir)

    use_rle_int = 0
    if opts.use_rle:
        use_rle_int = 1
    run_on_mic_int = 0
    if opts.run_on_mic:
        run_on_mic_int = 1

    count_vertices = conf.SG_GRAPH_SETTINGS_RMAT[opts.dataset]["count_vertices"]
    count_edges = conf.SG_GRAPH_SETTINGS_RMAT[opts.dataset]["count_edges"]

    # the degree-phase doesn't need too many partition-managers:
    count_partition_managers = 0
    if opts.phase == "generate_vertex_degrees":
        opts.count_partition_managers = conf.SG_GRC_RMAT_TILER_DEGREES_NPARTITION_MANAGERS
    else:
        opts.count_partition_managers = conf.SG_GRC_RMAT_TILER_TILING_NPARTITION_MANAGERS

    # start MIC-components:
    edges_per_mic = count_edges / opts.nmic
    for mic_index in range(0, opts.nmic):
        edges_start = edges_per_mic * mic_index
        edges_end = edges_per_mic * (mic_index + 1)
        port = opts.base_port + mic_index * 100
        args_rmat_generator = [
                "--port", port,
                "--edges-start", edges_start,
                "--edges-end", edges_end,
                "--count-threads", opts.count_generator_threads,
                "--count-vertices", count_vertices,
                "--count-partition-managers", opts.count_partition_managers,
                "--generator-phase", opts.phase
            ]
        if opts.run_on_mic:
            args_rmat_generator = ["nohup", "./grc-rmat-generator"] + args_rmat_generator + [
                    ">", "rmat-generator.out",
                    "2>", "rmat-generator.err",
                    "<", "/dev/null", "&"]
            utils.run_sshpass(opts.print_only, "phi", "root", "mic%s" % (mic_index), *args_rmat_generator)
        else: 
            utils.run_background(opts.print_only, "sudo", grc_edge_generator, *args_rmat_generator)

    time.sleep(3)

    # start host-components:
    args_rmat_tiler = [
            grc_tiler,
            "--graphname", opts.dataset,
            "--base-port", opts.base_port,
            "--count-partition-managers", opts.count_partition_managers,
            "--count-threads", opts.count_threads,
            "--count-vertices", count_vertices,
            "--count-edges", count_edges,
            "--generator-phase", opts.phase,
            "--path-global", global_dir,
            "--paths-meta", ":".join(meta_dirs),
            "--paths-tile", ":".join(tile_dirs),
            "--paths-partition", ":".join(partition_dirs),
            "--count-edge-generators", opts.nmic,
            "--use-run-length-encoding", use_rle_int,
            "--run-on-mic", run_on_mic_int
        ]
    if opts.gdb_tiler:
        args_rmat_tiler = ["gdb", "--args"] + args_rmat_tiler

    utils.run(opts.print_only, "time", "sudo", *args_rmat_tiler);
    

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--dataset", default=conf.SG_DATASET)
    parser.add_option("--nmic", default=conf.SG_GRC_NMIC)
    parser.add_option("--run-on-mic", default=conf.SG_GRC_RMAT_RUN_ON_MIC)
    parser.add_option("--count-threads", default=conf.SG_GRC_NTHREADS_TILER)
    parser.add_option("--no-rle", dest="use_rle", action="store_false",
                      default=conf.SG_GRC_USE_RLE)
    parser.add_option("--debug", dest="debug",
                      action="store_true", default=conf.SG_DEBUG)
    parser.add_option("--print-only", dest="print_only",
                      action="store_true", default=conf.SG_PRINT_ONLY)
    parser.add_option("--gdb-generator", dest="gdb_partitioner",
                      action="store_true", default=conf.SG_GRC_GDB_PARTITIONER)
    parser.add_option("--gdb-tiler", dest="gdb_tiler",
                      action="store_true", default=conf.SG_GRC_GDB_TILER)
    parser.add_option("--base-port", default=conf.SG_GRC_RMAT_PORT)
    parser.add_option("--count-generator-threads", default=conf.SG_GRC_RMAT_GEN_THREADS)
    parser.add_option("--phase")
    (opts, args) = parser.parse_args()

    if not (opts.phase == "generate_vertex_degrees" or opts.phase == "generate_tiles" or opts.phase == "all"):
        print("Wrong phase passed %s!" % (opts.phase))
        parser.print_help()
        exit(1)

    print("# Generating graph")
    build.build(False, False)

    # in case of both phases, run one after another:
    if(opts.phase == "all"):
      opts.phase = "generate_vertex_degrees"
      generate_graph(opts)
      opts.phase = "generate_tiles"
      generate_graph(opts)
    else:
      generate_graph(opts)
