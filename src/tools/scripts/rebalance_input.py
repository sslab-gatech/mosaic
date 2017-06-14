#!/usr/bin/env python3

import os
import optparse
import shutil
import utils
import config_engine as conf
import post_grc
import post_pgrc

def cleanupNvme(opts):
    print("# Cleanup files at:")
    for i in range(0, int(opts.nmic)):
        directory = os.path.join(conf.SG_DATAPATH_EDGE_ENGINE[i],
                                 conf.getWeightedName(opts.dataset, opts.weighted))
        print("#  Clean %s" % (directory))
        if os.path.exists(directory):
            shutil.rmtree(directory)

        directory = os.path.join(conf.SG_DATAPATH_VERTEX_ENGINE[i],
                                 conf.getWeightedName(opts.dataset, opts.weighted))
        print("#  Clean %s" % (directory))
        if os.path.exists(directory):
            shutil.rmtree(directory)

    # create dirs
    for i in range(0, int(opts.nmic)):
        directory = os.path.join(conf.SG_DATAPATH_EDGE_ENGINE[i],
                                 conf.getWeightedName(opts.dataset, opts.weighted))
        print("#  Setup %s" % (directory))
        utils.mkdirp(directory, conf.FILE_GROUP)

        directory = os.path.join(conf.SG_DATAPATH_VERTEX_ENGINE[i],
                                 conf.getWeightedName(opts.dataset, opts.weighted))
        print("#  Setup %s" % (directory))
        utils.mkdirp(directory, conf.FILE_GROUP)

def rebalance(opts):
    globals_dir = conf.getGlobalsDir(opts.dataset, opts.weighted)
    partition = conf.getGrcPartitionDir(opts.dataset, 0, opts.weighted)

    original_meta = []
    original_tile = []
    for i in range(0, len(conf.SG_GRC_OUTPUT_DIRS)):
        original_meta.append(conf.getGrcMetaDir(opts.dataset, i, opts.weighted))
        original_tile.append(conf.getGrcTileDir(opts.dataset, i, opts.weighted))

    output_meta = []
    output_tile = []

    for i in range(0, int(opts.nmic)):
        output_meta.append(conf.getMicSubdir(conf.SG_DATAPATH_VERTEX_ENGINE[i],
                                             opts.dataset, "meta", i, opts.weighted))
        output_tile.append(conf.getMicSubdir(conf.SG_DATAPATH_EDGE_ENGINE[i],
                                             opts.dataset, "tile", i, opts.weighted))

    if opts.pdump is True:
        post_pgrc.post_graph_load(original_meta, output_meta,
                                  original_tile, output_tile,
                                  globals_dir, opts.shuffle)
    else:
        post_grc.post_graph_load(original_meta, output_meta,
                             original_tile, output_tile,
                             globals_dir, opts.shuffle)
    # Copy over meta to the edge_engine output directory:
    # 1. Create meta directory for specific MIC on edge_engine output
    # 2. Copy tile_stats.dat and stat.dat 
    for i in range(0, int(opts.nmic)):
        edge_engine_meta_dir = conf.getMicSubdir(conf.SG_DATAPATH_EDGE_ENGINE[i],
                opts.dataset, "meta", i, opts.weighted)
        vertex_engine_meta_dir = conf.getMicSubdir(
                conf.SG_DATAPATH_VERTEX_ENGINE[i], opts.dataset, "meta", i,
                opts.weighted)
        if(edge_engine_meta_dir == vertex_engine_meta_dir):
            continue

        if not os.path.exists(edge_engine_meta_dir):
            utils.mkdirp(edge_engine_meta_dir, conf.FILE_GROUP)

        # Copy the files over.
        stat_filename = os.path.join(vertex_engine_meta_dir, "stat.dat")
        tile_stat_filename = os.path.join(vertex_engine_meta_dir, "tile_stats.dat")
        cmd = "cp %s %s" % (stat_filename, edge_engine_meta_dir)
        os.system(cmd)
        cmd = "cp %s %s" % (tile_stat_filename, edge_engine_meta_dir)
        os.system(cmd)
        print(cmd)
    print("Done copying all stat files for edge engines")

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--dataset", default=conf.SG_DATASET)
    parser.add_option("--weighted", action="store_true", dest="weighted", default=False)
    parser.add_option("--nmic", default=conf.SG_NMIC, type="int")
    parser.add_option("--shuffle", action="store_true", default=False)
    parser.add_option("--no-pdump", dest="pdump", action="store_false", default=True)
    (opts, args) = parser.parse_args()

    if(opts.dataset == "rmat-32"):
        print("No rebalancing of rmat-32 at this time!\n")
        exit(0)

    cleanupNvme(opts)
    rebalance(opts)
