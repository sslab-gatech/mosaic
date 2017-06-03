#!/usr/bin/env python

import os
import utils
import numpy
import optparse
from argparse import Namespace
import config_engine as conf

def parseLog(opts):
    print(opts.algorithm)
    if conf.SG_ALGORITHM_WEIGHTED[opts.algorithm]:
        opts.dataset = conf.getWeightedName(opts.dataset, True)
    log_dir = os.path.join(conf.LOG, opts.dataset)
    logfiles = [f for f in os.listdir(log_dir) if os.path.isfile(os.path.join(log_dir, f)) and opts.algorithm in f]
    logfiles.sort()
    for logfile in logfiles:
        with open(os.path.join(log_dir, logfile)) as f:
            lines = f.readlines()
            values = []
            for line in lines:
                target_str = "Round Time for iteration"
                finish_str = "Exit vertex-engine!"
                if finish_str in line:
                    break
                if target_str in line:
                    time_str = line.split(' ')[-1]
                    time_str = time_str.rstrip()
                    # Remove 'msec' at the end of the string
                    time = time_str[:-4]
                    values.append(float(time))
            if(len(values) > 0):
                sum_values = numpy.sum(numpy.array(values))
                median = numpy.median(numpy.array(values))
                mean = numpy.mean(numpy.array(values))
                stderr = numpy.std(numpy.array(values))
                logfile_splits = logfile.split('-')
                run_on_mic = logfile_splits[2]
                sel_sched = logfile_splits[3]
                iterations = logfile_splits[4]
                extra_args = ""
                if(len(logfile_splits) > 6):
                    extra_args = "-".join(logfile_splits[6:])[0:-4]
                print(run_on_mic, sel_sched, iterations, extra_args,  median, mean, stderr, sum_values)

if __name__ == "__main__":
    datasets = ["twitter-small", "twitter-full", "uk2007", "rmat-24", "rmat-27", "wdc2014", "rmat-32"]
    enabled_datasets = {
            "twitter-small": True,
            "twitter-full": True,
            "uk2007": True,
            "rmat-24": True,
            "rmat-27": True,
            "wdc2014": True,
            "rmat-32": True
            }
    algorithms = ["pagerank", "bfs", "cc", "spmv", "tc", "sssp", "bp"]

    for dataset in datasets:
        if enabled_datasets[dataset]:
            print(dataset)
            for algorithm in algorithms:
                opts = Namespace(
                        run_on_mic = False, algorithm = algorithm, dataset = dataset)
                parseLog(opts)

