#!/usr/bin/env python
import os
import sys
import subprocess
import glob
import optparse

num_dir = 512

def created_hashed_dir(dir_path):
    # remove old files
    os.system("rm -rf %s" % dir_path)

    # create hashed directory
    #  - ${dir_path}/%03x [0, num_dir]
    for i in range(0, num_dir):
        tgt_dir = os.path.join(dir_path, "%03x" % i)
        os.system("mkdir -p %s" % tgt_dir)

def create_dirs(dir_paths):
    for dir_path in dir_paths.split(':'):
        print("#   Preparing hashed directories at %s" % dir_path)
        created_hashed_dir(dir_path)

def create_dir(dir_path):
    os.system("mkdir -p %s" % dir_path)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--partition", help="paths to partition data separated by ':'")
    parser.add_option("--meta",      help="paths to tile meta data separated by ':'")
    parser.add_option("--tile",      help="paths to tile edge data separated by ':'")
    parser.add_option("--global-dir",help="path to global meta data")
    (opts, args) = parser.parse_args()

    # check options
    if opts.meta is None:
        print("Missing options: meta")
        parser.print_help()
        exit(1)
    if opts.tile is None:
        print("Missing options: tile")
        parser.print_help()
        exit(1)

    # run
    print("# Preparing partition, meta, and tile directories...")
    if not opts.partition is None:
        create_dirs(opts.partition)
    create_dirs(opts.meta)
    create_dirs(opts.tile)
    create_dir(opts.global_dir)

