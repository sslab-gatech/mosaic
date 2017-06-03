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

def generate_index(opts):
    def run(args):
        utils.run(opts.print_only, "time", *args)

    # make sure proper config is loaded
    run(["sudo", "./startup-config"])

    if opts.debug:
        b = conf.DBIN_TILE_INDEXER
    else:
        b = conf.RBIN_TILE_INDEXER

    meta_dirs = []
    global_dir = conf.getGlobalsDir(opts.dataset, False)

    for i in range(0, int(opts.nmic)):
        meta_dirs.append(conf.getMicSubdir(conf.SG_DATAPATH_VERTEX_ENGINE[i],
            opts.dataset, "meta", i))

    # run indexer
    args = [b,
            "--nthreads", 1,
            "--paths-meta", ":".join(meta_dirs),
            "--path-global", global_dir]

    if opts.gdb:
        args = ["gdb", "--args"] + args
    run(args)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--dataset", default = conf.SG_DATASET)
    parser.add_option("--nmic", default = conf.SG_NMIC)
    parser.add_option("--debug", dest="debug",
                      action="store_true", default=conf.SG_DEBUG)
    parser.add_option("--gdb", dest="gdb",
                      action="store_true", default=False)
    parser.add_option("--print-only", action="store_true", dest="print_only",
            default = conf.SG_PRINT_ONLY)
    (opts, args) = parser.parse_args()

    print("# Generating graph")
    build.build(False, False)
    generate_index(opts)
