#!/usr/bin/env python
import os
import optparse
import time
import config_engine as conf

def run(cmd, print_only):
    print cmd
    if not print_only:
        os.system(cmd)

def run_configuration(dataset, algorithm, iterations, opts):
    if conf.SG_ALGORITHM_WEIGHTED[algorithm]:
        if dataset == "rmat-32":
            return
        dataset = conf.getWeightedName(dataset, True)
    if opts.use_mic:
        cmd = "./run_all.py --dataset %s --algorithm %s --max-iteration %s --run perfstat --no-fs-down" % (dataset, algorithm, iterations)
    else:
        cmd = "./run_mosaic.py --dataset %s --algorithm %s --max-iteration %s --run perfstat" % (dataset, algorithm, iterations)
    run(cmd, opts.print_only)


if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--use-mic", action="store_true", dest="use_mic", default = False)
    parser.add_option("--print-only", action="store_true", dest="print_only", default = conf.SG_PRINT_ONLY)
    (opts, args) = parser.parse_args()

    print("# Starting benchmark")
    print_only = opts.print_only

    datasets = ["twitter-small", "twitter-full", "uk2007", "rmat-24", "rmat-27", "wdc2014", "rmat-32"]
    iterations = {
            "twitter-small": 1000,
            "twitter-full": 1000,
            "uk2007": 1000,
            "rmat-24": 1000,
            "rmat-27": 1000,
            "wdc2014": 1000,
            "rmat-32": 8
            }
    iterations_per_algorithm = {
            "pagerank": 20,
            "bfs": 1000,
            "cc": 1000,
            "spmv": 20,
            "tc": 40,
            "sssp": 1000,
            "bp": 20,
            }
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
            for algorithm in algorithms:
                iters = min(iterations[dataset], iterations_per_algorithm[algorithm])
                run_configuration(dataset, algorithm, iters, opts)

