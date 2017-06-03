#!/usr/bin/env python3

import os
import sys
import glob
import struct
import optparse
import math
import random
import utils
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import config_engine as conf

tile_filename = "tiles.dat"
meta_filename = "meta.dat"
stat_filename = "tile_stats.dat"

tile_prefix = "eb-"
meta_prefix = "ebi-"
stat_prefix = "ebs-"

random_state = random.getstate()

if os.getcwd() != os.path.dirname(__file__):
    print("Not confident this code is working outside the /scripts dir")
    exit(1)

def fix_block_id(tgt_file, block_id):
    with open(tgt_file, 'r+b') as f:
        # uintn64_t block_id
        f.write(struct.pack('@Q', block_id))

def reshuffle_files(original_paths, output_paths, prefix, output_file_name,
                    truncate, alignment, file_truncate, file_truncate_size, note=""):
    def _dump_per_dir(output_path, output_file_name,
            out_dir, truncate, alignment, file_truncate, file_truncate_size):
        output_file = os.path.join(output_path, output_file_name)

        for single_file in out_dir:
            file_content = ""
            with open(single_file, "rb") as f:
                file_content = f.read()

            if truncate:
                aligned_size = alignment - (len(file_content) % alignment)
                if aligned_size % alignment != 0:
                    file_content += b'\0' * aligned_size

            with open(output_file, "ab") as f:
                f.write(file_content)

        if file_truncate:
            append_bytes = b'\0' * file_truncate_size

            with open(output_file, "ab") as f:
                f.write(append_bytes)

        os.sync()

    count_output = len(output_paths)

    filename_to_file = dict()

    files_per_output = list()
    for i in range(0, count_output):
        files_per_output.append(list())

    files = list()
    for original_path in original_paths:
        prefix_pattern = "*/" + prefix + "*"
        partial_files = glob.glob(os.path.join(original_path, prefix_pattern))
        for single_file in partial_files:
            filename = os.path.basename(single_file)
            filename_to_file[filename] = single_file
            files.append(filename)

    sorted_files = sorted(files)

    current_output = 0
    for single_file in sorted_files:
        files_per_output[current_output].append(filename_to_file[single_file])
        current_output = (current_output + 1) % count_output

    dir_index = 0
    rfutures = []
    with ThreadPoolExecutor(max_workers=len(output_paths)) as shuffle_exec:
        for output_path in output_paths:
            rfutures.append(shuffle_exec.submit(_dump_per_dir,
                                              output_path, output_file_name,
                                              files_per_output[dir_index],
                                              truncate, alignment,
                                              file_truncate,
                                              file_truncate_size))
            #_dump_per_dir(output_path, output_file_name, files_per_output[dir_index], truncate, alignment)
            dir_index += 1

    for f in as_completed(rfutures):
        pass
    return ("done with %s" % note)

def copy_global_stats_file(global_dir, meta_dirs):
    stat_filename = os.path.join(global_dir, "stat.dat")
    for meta_dir in meta_dirs:
        cmd = "cp %s %s" % (stat_filename, meta_dir)
        os.system(cmd)

def setup_directories(output_meta_dirs, output_tile_dirs):
    for output_meta_dir in output_meta_dirs:
        utils.mkdirp(output_meta_dir, conf.FILE_GROUP)

    for output_tile_dir in output_tile_dirs:
        utils.mkdirp(output_tile_dir, conf.FILE_GROUP)

def post_graph_load(original_meta_dirs, output_meta_dirs,
                    original_tile_dirs, output_tile_dirs,
                    globals_dir, shuffle):
    random.seed(1)
    random_state = random.getstate()

    setup_directories(output_meta_dirs, output_tile_dirs)

    futures = []
    with ProcessPoolExecutor(max_workers=(3*len(output_tile_dirs))) as executor:
        #print("# Rearranging tile-files...")
        #reshuffle_files(original_tile_dirs, output_tile_dirs, tile_prefix, tile_filename, True, 4096)
        futures.append(executor.submit(reshuffle_files,
                                       original_tile_dirs, output_tile_dirs,
                                       tile_prefix, tile_filename, True, 4096, True, 1024 * 1024,
                                      "tile-files"))
        #print("# Rearranging meta-files...")
        #reshuffle_files(original_meta_dirs, output_meta_dirs, meta_prefix, meta_filename, True, 4096)
        futures.append(executor.submit(reshuffle_files,
                                       original_meta_dirs, output_meta_dirs,
                                      meta_prefix, meta_filename, True, 4096, True, 1024 * 1024,
                                      "meta-files"))
        #print("# Rearranging stat-files...")
        #reshuffle_files(original_meta_dirs, output_meta_dirs, stat_prefix, stat_filename, False, 0)
        futures.append(executor.submit(reshuffle_files,
                                      original_meta_dirs, output_meta_dirs,
                                      stat_prefix, stat_filename, False, 0, False, 0,
                                      "stat-files"))

        for f in as_completed(futures):
            print (f.result())

    copy_global_stats_file(globals_dir, output_meta_dirs)

if __name__ == "__main__":
    parser = optparse.OptionParser()
    parser.add_option("--globals-dir", help="path to global meta data")
    parser.add_option("--original-meta", help="paths to tile meta data separated by ':'")
    parser.add_option("--output-meta", help="paths to tile meta data separated by ':'")
    parser.add_option("--original-tile", help="paths to tile edge data separated by ':'")
    parser.add_option("--output-tile", help="paths to tile edge data separated by ':'")
    parser.add_option("--partition", help="paths to partition data, seperated by ':'")
    parser.add_option("--shuffle", action="store_true", default = False)
    (opts, args) = parser.parse_args()

    shuffle = opts.shuffle

    original_meta_dirs = opts.original_meta.split(':')
    output_meta_dirs = opts.output_meta.split(':')

    original_tile_dirs = opts.original_tile.split(':')
    output_tile_dirs = opts.output_tile.split(':')

    print("# Rearrange meta, tile and stat-files...")
    post_graph_load(original_meta_dirs, output_meta_dirs,
                    original_tile_dirs, output_tile_dirs,
                    shuffle)

