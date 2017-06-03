#!/usr/bin/env python
import os
import sys
import subprocess
import glob
import optparse

def rm_paths(dir_paths):
    for dir_path in dir_paths.split(':'):
        rm_path(dir_path)

def rm_path(path):
    print("#   rm -rf %s" % path)
    os.system("rm -rf %s" % path)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--partition", help="paths to partition data separated by ':'", default = "")
    parser.add_option("--meta",      help="paths to tile meta data separated by ':'")
    parser.add_option("--tile",      help="paths to tile edge data separated by ':'")
    parser.add_option("--global-dir",help="path to global meta data")
    (opts, args) = parser.parse_args()

    # run
    print("# rm -rf partition, meta, and tile directories...")
    if opts.partition != "" :
        rm_paths(opts.partition)

    rm_path(opts.global_dir)

    for dir_paths in (opts.meta, opts.tile):
        rm_paths(dir_paths)

