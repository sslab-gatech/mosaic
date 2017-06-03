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

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--dataset", default=conf.SG_DATASET)
    parser.add_option("--weighted", action="store_true", dest="weighted", default=False)
    parser.add_option("--nmic", default=conf.SG_NMIC, type="int")
    (opts, args) = parser.parse_args()

    if(opts.dataset == "rmat-32"):
        print("No cleaning of rmat-32 at this time!\n")
        exit(0)

    cleanupNvme(opts)
