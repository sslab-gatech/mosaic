#!/usr/bin/env python
import os
import optparse
import config_engine
import time
import utils

PCIE_CLOUD_CTRL_SCRIPT = os.path.join(config_engine.ROOT, "../pcie-cloud/scripts/ctrl.py")

def run(cmd, print_only):
    print cmd
    if not print_only:
        os.system(cmd)

def check(print_only, dataset, iterations):
    log_correct = os.path.join(config_engine.LOG_ROOT, "%s-correct" % (config_engine.getWeightedName(dataset)))
    log = os.path.join(config_engine.LOG_ROOT, config_engine.getWeightedName(dataset))
    cmd = "./check_output.py %s %s %s" %(log, log_correct, iterations)
    run(cmd, print_only)

def setup(print_only):
    cmd = "./killall_engines.py"
    run(cmd, print_only)
    cmd = "sudo ./startup-config"
    run(cmd, print_only)
    if config_engine.SG_RUN_ON_MIC:
        if opts.fs_down:
            cmd = PCIE_CLOUD_CTRL_SCRIPT + " fs_down"
            run(cmd, print_only)
            cmd = PCIE_CLOUD_CTRL_SCRIPT + " fs_up"
#            cmd = PCIE_CLOUD_CTRL_SCRIPT + " fs_up --debug=host:fs"
            run(cmd, print_only)
        time.sleep(1)
        run("./ls_all.py", print_only)
        run("./copy_binaries.py", print_only)

def startAll(print_only, opts):
    if opts.local:
        cmd = "./run_mosaic.py --dataset twitter-small --algorithm pagerank --max-iteration %s --enable-log" % (opts.iterations)
        run(cmd, print_only)
    else:
        cmd = "./run_edge_engines.py --dataset twitter-small --algorithm pagerank"
        run(cmd, print_only)
        time.sleep(10)
        cmd = "./run_vertex_processor.py --dataset twitter-small --algorithm pagerank --enable-log"
        run(cmd, print_only)


if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--no-clean", action="store_false", dest="clean", help="", default = True)
    parser.add_option("--print-only", action="store_true", dest="print_only", help="", default = config_engine.SG_PRINT_ONLY)
    parser.add_option("--no-fs-down", action="store_false", dest="fs_down", default = True)
    parser.add_option("--local", action="store_true", dest="local", default = False)
    parser.add_option("--iterations", default = config_engine.SG_MAX_ITERATIONS)
    (opts, args) = parser.parse_args()

    print("# Starting everything")
    print_only = opts.print_only
    clean = opts.clean

    if clean:
        setup(print_only)
    startAll(print_only, opts)
    check(print_only, config_engine.SG_DATASET, opts.iterations)

