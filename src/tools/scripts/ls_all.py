#!/usr/bin/env python
import os
import optparse
import config_engine

def lsNvmeMic(nmic, print_only):
    for edge_engine_index in range(0, int(nmic)):
        mic_index = config_engine.SG_EDGE_ENGINE_TO_MIC[edge_engine_index]
        nvme = config_engine.SG_DATAPATH_EDGE_ENGINE[edge_engine_index]
        cmd = "sshpass -p phi ssh root@mic%s \"ls -al %s;\"" % (mic_index, nvme)
        print cmd
        if not print_only:
            os.system(cmd)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--nmic", help="", default = config_engine.SG_NMIC)
    parser.add_option("--print-only", action="store_true", dest="print_only", help="", default = config_engine.SG_PRINT_ONLY)
    (opts, args) = parser.parse_args()

    nmic = opts.nmic
    print_only = opts.print_only

    lsNvmeMic(nmic, print_only)

