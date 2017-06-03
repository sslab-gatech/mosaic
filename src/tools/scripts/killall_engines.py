#!/usr/bin/env python2

import os
import optparse
import config_engine

def killAllEdgeEngine(print_only):
    if config_engine.SG_RUN_ON_MIC:
      for mic_index in range(0, int(config_engine.NUM_XEON_PHIS)):
        cmd = "sshpass -p phi ssh root@mic%s \"killall edge-engine\"" % (mic_index)
        print cmd
        if not print_only:
            os.system(cmd)
    cmd = "sudo killall edge-engine"
    print cmd
    if not print_only:
        os.system(cmd)

def killAllVertexEngine(print_only):
    cmd = "sudo killall vertex-engine"
    print cmd
    if not print_only:
        os.system(cmd)

def killAll(print_only):
    print("# Stopping ./edge-engine")
    killAllEdgeEngine(print_only)

    print("# Stopping ./vertex-engine")
    killAllVertexEngine(print_only)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--print-only", action="store_true", dest="print_only", help="", default = config_engine.SG_PRINT_ONLY)
    (opts, args) = parser.parse_args()

    killAll(opts.print_only)

