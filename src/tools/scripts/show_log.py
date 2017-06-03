#!/usr/bin/env python
import os
import optparse
import config_engine

def showLogPerMic(mic_index, edge_eindex, log, err, print_only):
    if log:
        log_file = "edge-engine-%d.out" % (edge_eindex)
        cmd = "sshpass -p phi ssh root@mic%s \"cat %s;\"" % (mic_index, log_file)
        print cmd
        if not print_only:
            os.system(cmd)
    if err:
        err_file = "edge-engine-%d.err" % (edge_eindex)
        cmd = "sshpass -p phi ssh root@mic%s \"cat %s;\"" % (mic_index, err_file)
        print cmd
        if not print_only:
            os.system(cmd)

def showLog(nmic, log, err, print_only):
    for edge_engine_index in range(0, int(nmic)):
        mic_index = config_engine.SG_EDGE_ENGINE_TO_MIC[edge_engine_index]
        showLogPerMic(mic_index, edge_engine_index, log, err, print_only)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--nmic", help="", default = config_engine.SG_NMIC)
    parser.add_option("--per-mic", action="store_false", dest="per_mic", help="", default = False)
    parser.add_option("--print-only", action="store_true", dest="print_only", help="", default = config_engine.SG_PRINT_ONLY)
    parser.add_option("--disable_log", action="store_false", dest="log", help="", default = True)
    parser.add_option("--disable_err", action="store_false", dest="err", help="", default = True)
    (opts, args) = parser.parse_args()

    nmic = opts.nmic
    print_only = opts.print_only
    log = opts.log
    err = opts.err

    if (opts.per_mic):
        mic_index = config_engine.SG_EDGE_ENGINE_TO_MIC[nmic]
        showLogPerMic(mic_index, nmic, log, err, print_only)
    else:
        showLog(nmic, log, err, print_only)

