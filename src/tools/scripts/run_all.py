#!/usr/bin/env python
import os
import optparse
import config_engine
import time
import build
import killall_engines
from datetime import datetime
import utils

PCIE_CLOUD_CTRL_SCRIPT = os.path.join(config_engine.ROOT, "../pcie-cloud/scripts/ctrl.py")

def run(cmd, print_only):
    print cmd
    if not print_only:
        os.system(cmd)

def setup(print_only):
    killall_engines.killAll(print_only)
    build.build(False, False);
    run("sudo ./startup-config", print_only)
    if config_engine.SG_RUN_ON_MIC:
        if opts.fs_down:
            cmd = PCIE_CLOUD_CTRL_SCRIPT + " fs_down"
            run(cmd, print_only)
            cmd = PCIE_CLOUD_CTRL_SCRIPT + " fs_up"
            # cmd = PCIE_CLOUD_CTRL_SCRIPT + " fs_up --debug=host:fs"
            run(cmd, print_only)
        time.sleep(1)
        run("./ls_all.py", print_only)
        run("./copy_binaries.py", print_only)

def startAll(opts):
    if not opts.no_edge_engine:
        cmd = "./run_edge_engines.py --dataset %s --algorithm %s --max-iterations %s --enable-perfmon %s" % (opts.dataset, opts.algorithm, opts.max_iterations, opts.enable_perfmon)
        if opts.debug:
            cmd += " --debug"
        run(cmd, opts.print_only)
        # cmd = "sudo perf record -g -o perf.stat ./run_vertex_processor.py"
        if config_engine.SG_RUN_ON_MIC:
            time.sleep(10)
        else:
            time.sleep(1)
    if opts.no_edge_engine:
        raw_input("Press enter to start vertex engine.")
    cmd = "./run_vertex_processor.py --dataset %s --algorithm %s --max-iterations %s --enable-perfmon %s" % (opts.dataset, opts.algorithm, opts.max_iterations, opts.enable_perfmon)

    if opts.run:
        cmd += " --run %s" % (opts.run)
    if opts.benchmark_mode:
        cmd += " --benchmark-mode"
    if opts.fault_tolerant_mode:
        cmd += " --fault-tolerant-mode"
    if opts.gdb:
        cmd += " --gdb"
    if opts.debug:
        cmd += " --debug"
    iostat_arg = "%s-%s-%s" % (datetime.now().strftime("%Y%M%d-%H%M%S"),
                               opts.dataset, opts.algorithm)
    with utils.CollectlLog(iostat_arg, opts.iostat):
      run(cmd, opts.print_only)


if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--algorithm", default = config_engine.SG_ALGORITHM)
    parser.add_option("--dataset", default = config_engine.SG_DATASET)
    parser.add_option("--max-iterations", default = config_engine.SG_MAX_ITERATIONS)
    parser.add_option("--benchmark-mode", action="store_true", dest="benchmark_mode", default = False)
    parser.add_option("--no-fs-down", action="store_false", dest="fs_down", default = True)
    parser.add_option("--no-clean", action="store_false", dest="clean", default = True)
    parser.add_option("--gdb", action="store_true", dest="gdb", default = False)
    parser.add_option("--no-edge-engine", action="store_true", dest="no_edge_engine", default = False)
    parser.add_option("--debug", action="store_true", dest="debug", default = False)
    parser.add_option("--print-only", action="store_true", dest="print_only", default = config_engine.SG_PRINT_ONLY)
    parser.add_option("--iostat", action="store_true", dest="iostat", default = False)
    parser.add_option("--enable-perfmon", action="store_true", dest="enable_perfmon", default = config_engine.SG_PERFMON)
    parser.add_option("--fault-tolerant-mode", action="store_true", dest="fault_tolerant_mode", default = config_engine.SG_ENABLE_FAULT_TOLERANCE)
    parser.add_option("--run", default = "")
    (opts, args) = parser.parse_args()

    print("# Starting everything")
    if opts.clean:
        setup(opts.print_only)
    startAll(opts)

