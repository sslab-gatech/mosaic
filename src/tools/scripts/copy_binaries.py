#!/usr/bin/env python
import os
import optparse
import config_engine

def copyBinaries(nmic, debug, print_only):
    release_binary = os.path.join(config_engine.BUILD_ROOT, "Release-k1om/lib/core/edge-engine")
    debug_binary = os.path.join(config_engine.BUILD_ROOT, "Debug-k1om/lib/core/edge-engine")

    for mic_index in range(0, int(config_engine.NUM_XEON_PHIS)):
        if debug:
            cmd = "sshpass -p phi scp %s root@mic%s:" % (debug_binary, mic_index)
            print cmd
            if not print_only:
                os.system(cmd)
        else:
            cmd = "sshpass -p phi scp %s root@mic%s:" % (release_binary, mic_index)
            print cmd
            if not print_only:
                os.system(cmd)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--nmic", help="", default = config_engine.SG_NMIC)
    parser.add_option("--debug", action="store_true", dest="debug", help="", default = config_engine.SG_DEBUG)
    parser.add_option("--print-only", action="store_true", dest="print_only", help="", default = config_engine.SG_PRINT_ONLY)
    (opts, args) = parser.parse_args()

    nmic = opts.nmic
    debug = opts.debug
    print_only = opts.print_only
    
    copyBinaries(nmic, debug, print_only)

