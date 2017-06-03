#!/usr/bin/env python
import os
import sys
import glob
import struct
import optparse
import math
import config_engine

def startBenchmark(nreader, datapath, dataset, run_on_mic, debug, print_only):
    meta_dir = config_engine.getMicSubdir(datapath, dataset, "meta", 0)
    tile_dir = config_engine.getMicSubdir(datapath, dataset, "tile", 0)

    edge_engine_args = "--port 0 --algorithm \"\" --nreader %s --nprocessor 0 --mic-index 0 --nmic 1 --partition ~/ --meta %s --tile %s" % (nreader, meta_dir, tile_dir)

    if run_on_mic:
        cmd = "sshpass -p phi ssh root@mic%s \"nohup ./benchmark %s > benchmark.out 2> benchmark.err < /dev/null &\"" % (0, edge_engine_args)
        print cmd
        if not print_only:
            os.system(cmd)
    else:
        pwd = os.path.dirname(os.path.realpath(__file__))
        build_dir = os.path.join(pwd, "../../../build/")
        if debug:
            build_dir = os.path.join(build_dir, "Debug-x86_64")
        else:
            build_dir = os.path.join(build_dir, "Release-x86_64")
        edge_engine_exec = os.path.join(build_dir, "micro/benchmark")

        cmd = "sudo %s %s" % (edge_engine_exec, edge_engine_args)
        print cmd
        if not print_only:
            os.system(cmd)


if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--nreader", help="", default = config_engine.SG_NREADER)
    parser.add_option("--dataset", help="", default = config_engine.SG_DATASET)
    parser.add_option("--run-on-mic", action="store_true", dest="run_on_mic", help="", default = config_engine.SG_RUN_ON_MIC)
    parser.add_option("--debug", action="store_true", dest="debug", help="", default = config_engine.SG_DEBUG)
    parser.add_option("--print-only", action="store_true", dest="print_only", help="", default = config_engine.SG_PRINT_ONLY)
    (opts, args) = parser.parse_args()

    print("# Starting ./benchmark")
    nreader = opts.nreader
    dataset = opts.dataset
    run_on_mic = opts.run_on_mic
    debug = opts.debug
    print_only = opts.print_only

    datapath = config_engine.SG_DATAPATH_EDGE_ENGINE[0]
    startBenchmark(nreader, datapath, dataset, run_on_mic, debug, print_only)

