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

def run_unittests(opts, test):
    def run(args):
        utils.run(False, "time", *args)

    if opts.debug:
        b = os.path.join(conf.BUILD_ROOT, "Debug-x86_64/test/%s" % test)
    else:
        b = os.path.join(conf.BUILD_ROOT, "Release-x86_64/test/%s" % test)

    # run test
    args = [b]

    if opts.gdb:
        args = ["gdb", "--args"] + args
    run(args)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--debug", dest="debug",
                      action="store_true", default=False)
    parser.add_option("--gdb", dest="gdb",
                      action="store_true", default=False)
    (opts, args) = parser.parse_args()

    build.build(False, False)
    run_unittests(opts, "tile_processor_test")
    run_unittests(opts, "bool_array_test")
    run_unittests(opts, "partition_test")
    run_unittests(opts, "traversal_test")
