#!/usr/bin/env python2

import os
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

    # if opts.run_partitioner:
    #     print("Partitioning is forbidden at this time!\n")
    #     exit(1)

    if opts.debug:
        grc_partitioner = conf.DBIN_PARTIONER
        grc_tiler = conf.DBIN_TILER
    else:
        grc_partitioner = conf.RBIN_PARTIONER
        grc_tiler = conf.RBIN_TILER

    input_weighted = 0
    if conf.SG_INPUT_WEIGHTED.get(opts.dataset, False):
        input_weighted = 1

    # populate hashed dirs
    num_dir = conf.SG_NUM_HASH_DIRS

    partition_dirs = []
    meta_dirs = []
    tile_dirs = []
    global_dir = conf.getGlobalsDir(opts.dataset, opts.weighted_output)

    if opts.run_partitioner:
        shutil.rmtree(global_dir, True)

    utils.mkdirp(global_dir, conf.FILE_GROUP)

    if(opts.weighted_output):
        unweighted_stat = os.path.join(conf.getGlobalsDir(opts.dataset, False), "stat.dat")
        weighted_stat = os.path.join(conf.getGlobalsDir(opts.dataset, True), "stat.dat")
        shutil.copyfile(unweighted_stat, weighted_stat)

        unweighted_deg = os.path.join(conf.getGlobalsDir(opts.dataset, False), "vertex_deg.dat")
        weighted_deg = os.path.join(conf.getGlobalsDir(opts.dataset, True), "vertex_deg.dat")
        shutil.copyfile(unweighted_deg, weighted_deg)

        unweighted_global_to_orig = os.path.join(conf.getGlobalsDir(opts.dataset, False), "vertex_global_to_orig.dat")
        weighted_global_to_orig = os.path.join(conf.getGlobalsDir(opts.dataset, True), "vertex_global_to_orig.dat")
        shutil.copyfile(unweighted_global_to_orig, weighted_global_to_orig)

    for i in range(0, len(conf.SG_GRC_PARTITION_DIRS)):
        partition_dir = conf.getGrcPartitionDir(opts.dataset, i, False)
        meta_dir = conf.getGrcMetaDir(opts.dataset, i, opts.weighted_output)
        tile_dir = conf.getGrcTileDir(opts.dataset, i, opts.weighted_output)

        if opts.run_partitioner:
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

    output_weighted = 0
    if opts.weighted_output:
        output_weighted = 1

    use_rle_int = 0
    if opts.use_rle:
        use_rle_int = 1

    generator = ""
    delimiter = ""
    count_vertices = 0
    count_edges = 0
    use_original_ids = 0

    input_file = ""
    if opts.rmat:
        generator = "rmat"
        count_vertices = conf.SG_GRAPH_SETTINGS_RMAT[opts.dataset][
            "count_vertices"]
        count_edges = conf.SG_GRAPH_SETTINGS_RMAT[opts.dataset]["count_edges"]
        use_original_ids = 1 if conf.SG_GRAPH_SETTINGS_RMAT[opts.dataset][
            "use_original_ids"] else 0
    elif opts.binary:
        generator = "binary"
        input_file = conf.SG_INPUT_FILE[opts.dataset]["binary"]
        count_vertices = conf.SG_GRAPH_SETTINGS_DELIM[opts.dataset][
            "count_vertices"]
        use_original_ids = 1 if conf.SG_GRAPH_SETTINGS_DELIM[opts.dataset][
            "use_original_ids"] else 0
    else:
        generator = "delim_edges"
        input_file = conf.SG_INPUT_FILE[opts.dataset]["delim"]
        count_vertices = conf.SG_GRAPH_SETTINGS_DELIM[opts.dataset][
            "count_vertices"]
        delimiter = conf.SG_GRAPH_SETTINGS_DELIM[opts.dataset]["delimiter"]
        use_original_ids = 1 if conf.SG_GRAPH_SETTINGS_DELIM[opts.dataset][
            "use_original_ids"] else 0

    if not opts.rmat:
        if not os.path.exists(input_file):
            print("Failed to find %s" % input_file)
            exit(1)

    # run partitioner
    if opts.run_partitioner:
        if not opts.rmat:
            if not os.path.exists(input_file):
                print("Failed to find %s" % input_file)
                exit(1)
        args = [grc_partitioner,
                "--source", input_file,
                "--graphname", opts.dataset,
                "--generator", generator,
                "--rmat-count-edges", count_edges,
                "--count-vertices", count_vertices,
                "--input-weighted", input_weighted,
                "--paths-partition", ":".join(partition_dirs),
                "--nthreads", conf.SG_GRC_NTHREADS_PARTITIONER,
                "--nwritethreads", conf.SG_GRC_NWRITE_THREADS,
                "--npartition-managers", conf.SG_GRC_NPARTITION_MANAGERS,
                "--delimiter", delimiter,
                "--use-original-ids", use_original_ids,
                "--path-global", global_dir]
        if opts.gdb_partitioner:
            args = ["gdb", "--args"] + args
        run(args)

    # run tiler
    if opts.run_tiler:
        args = [grc_tiler,
                "--graphname", opts.dataset,
                "--count-vertices", count_vertices,
                "--paths-partition", ":".join(partition_dirs),
                "--path-global", global_dir,
                "--paths-meta", ":".join(meta_dirs),
                "--paths-tile", ":".join(tile_dirs),
                "--nthreads", conf.SG_GRC_NTHREADS_TILER,
                "--npartition-managers", conf.SG_GRC_NPARTITION_MANAGERS,
                "--input-weighted", input_weighted,
                "--output-weighted", output_weighted,
                "--use-run-length-encoding", use_rle_int,
                "--traversal", opts.traversal]
        if opts.gdb_tiler:
            args = ["gdb", "--args"] + args
        run(args)


def generate_graph_in_memory(opts):
    def run(args):
        utils.run(opts.print_only, "time", *args)

    # make sure proper config is loaded
    run(["sudo", "./startup-config"])

    if opts.debug:
        grc_in_memory = conf.DBIN_GRC_IN_MEMORY
    else:
        grc_in_memory = conf.RBIN_GRC_IN_MEMORY

    input_weighted = 0
    if conf.SG_INPUT_WEIGHTED.get(opts.dataset, False):
        input_weighted = 1

    # populate hashed dirs
    num_dir = conf.SG_NUM_HASH_DIRS

    meta_dirs = []
    tile_dirs = []
    global_dir = conf.getGlobalsDir(opts.dataset, opts.weighted_output)

    utils.mkdirp(global_dir, conf.FILE_GROUP)

    if (opts.weighted_output):
        unweighted_stat = os.path.join(conf.getGlobalsDir(opts.dataset, False),
                                       "stat.dat")
        weighted_stat = os.path.join(conf.getGlobalsDir(opts.dataset, True),
                                     "stat.dat")
        shutil.copyfile(unweighted_stat, weighted_stat)

        unweighted_deg = os.path.join(conf.getGlobalsDir(opts.dataset, False),
                                      "vertex_deg.dat")
        weighted_deg = os.path.join(conf.getGlobalsDir(opts.dataset, True),
                                    "vertex_deg.dat")
        shutil.copyfile(unweighted_deg, weighted_deg)

        unweighted_global_to_orig = os.path.join(
            conf.getGlobalsDir(opts.dataset, False),
            "vertex_global_to_orig.dat")
        weighted_global_to_orig = os.path.join(
            conf.getGlobalsDir(opts.dataset, True), "vertex_global_to_orig.dat")
        shutil.copyfile(unweighted_global_to_orig, weighted_global_to_orig)

    for i in range(0, len(conf.SG_GRC_OUTPUT_DIRS)):
        meta_dir = conf.getGrcMetaDir(opts.dataset, i, opts.weighted_output)
        tile_dir = conf.getGrcTileDir(opts.dataset, i, opts.weighted_output)

        shutil.rmtree(meta_dir, True)
        shutil.rmtree(tile_dir, True)

        utils.mkdirp(meta_dir, conf.FILE_GROUP)
        utils.mkdirp(tile_dir, conf.FILE_GROUP)

        utils.populate_hash_dirs(num_dir, meta_dir)
        utils.populate_hash_dirs(num_dir, tile_dir)

        meta_dirs.append(meta_dir)
        tile_dirs.append(tile_dir)

    output_weighted = 0
    if opts.weighted_output:
        output_weighted = 1

    use_rle_int = 0
    if opts.use_rle:
        use_rle_int = 1

    generator = ""
    delimiter = ""
    count_vertices = 0
    count_edges = 0
    use_original_ids = 0

    input_file = ""
    if opts.rmat:
        generator = "rmat"
        count_vertices = conf.SG_GRAPH_SETTINGS_RMAT[opts.dataset][
            "count_vertices"]
        count_edges = conf.SG_GRAPH_SETTINGS_RMAT[opts.dataset]["count_edges"]
        use_original_ids = 1 if conf.SG_GRAPH_SETTINGS_RMAT[opts.dataset][
            "use_original_ids"] else 0
    elif opts.binary:
        generator = "binary"
        input_file = conf.SG_INPUT_FILE[opts.dataset]["binary"]
        count_vertices = conf.SG_GRAPH_SETTINGS_DELIM[opts.dataset][
            "count_vertices"]
        use_original_ids = 1 if conf.SG_GRAPH_SETTINGS_DELIM[opts.dataset][
            "use_original_ids"] else 0
    else:
        generator = "delim_edges"
        input_file = conf.SG_INPUT_FILE[opts.dataset]["delim"]
        count_vertices = conf.SG_GRAPH_SETTINGS_DELIM[opts.dataset][
            "count_vertices"]
        delimiter = conf.SG_GRAPH_SETTINGS_DELIM[opts.dataset]["delimiter"]
        use_original_ids = 1 if conf.SG_GRAPH_SETTINGS_DELIM[opts.dataset][
            "use_original_ids"] else 0

    if not opts.rmat:
        if not os.path.exists(input_file):
            print("Failed to find %s" % input_file)
            exit(1)
    args = [
        grc_in_memory,
        "--source", input_file,
        "--count-vertices", count_vertices,
        "--generator", generator,
        "--graphname", opts.dataset,
        "--path-globals", global_dir,
        "--paths-meta", ":".join(meta_dirs),
        "--paths-tile", ":".join(tile_dirs),
        "--nthreads", conf.SG_GRC_NTHREADS_PARTITIONER,
        "--npartition-managers", conf.SG_GRC_NPARTITION_MANAGERS,
        "--input-weighted", input_weighted,
        "--output-weighted", output_weighted,
        "--rmat-count-edges", count_edges,
        "--use-run-length-encoding", use_rle_int,
        "--use-original-ids", use_original_ids,
        "--traversal", opts.traversal,
        "--delimiter", delimiter,
    ]
    if opts.gdb:
        args = ["gdb", "--args"] + args
    run(args)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--dataset", default=conf.SG_DATASET)
    parser.add_option("--rmat", action="store_true", default=False)
    parser.add_option("--binary", action="store_true", default=False)
    parser.add_option("--in-memory", action="store_true", default=False)
    parser.add_option("--rmat-scale-vertices", default=16)
    parser.add_option("--rmat-scale-edges", default=20)
    parser.add_option("--no-partitioner", dest="run_partitioner",
                      action="store_false", default=conf.SG_GRC_RUN_PARTITIONER)
    parser.add_option("--weighted-output", dest="weighted_output",
                      action="store_true", default=False)
    parser.add_option("--no-rle", dest="use_rle", action="store_false",
                      default=conf.SG_GRC_USE_RLE)
    parser.add_option("--no-tiler", dest="run_tiler",
                      action="store_false", default=conf.SG_GRC_RUN_TILER)
    parser.add_option("--debug", dest="debug",
                      action="store_true", default=conf.SG_DEBUG)
    parser.add_option("--print-only", dest="print_only",
                      action="store_true", default=conf.SG_PRINT_ONLY)
    parser.add_option("--gdb-partitioner", dest="gdb_partitioner",
                      action="store_true", default=conf.SG_GRC_GDB_PARTITIONER)
    parser.add_option("--gdb", dest="gdb", action="store_true", default=False)
    parser.add_option("--gdb-tiler", dest="gdb_tiler",
                      action="store_true", default=conf.SG_GRC_GDB_TILER)
    parser.add_option("--traversal", default="hilbert")
    (opts, args) = parser.parse_args()

    print("# Generating graph")
    build.build(False, False)
    if opts.in_memory:
        generate_graph_in_memory(opts)
    else:
        generate_graph(opts)
