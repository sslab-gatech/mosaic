#!/usr/bin/env python3

import os
import optparse
import shutil
import utils
import numpy
import config_engine as conf
import post_grc
import post_pgrc

COUNT_NVME = 6

def parse(filename):
    intermediate_filename = filename + ".aggregated"
    output_filename = filename + ".plot"
    # Preprocess by only parsing relevant column.
    cmd = "awk '{ print $3, $6}' %s > %s" % (filename, intermediate_filename)
    os.system(cmd)

    if os.path.isfile(intermediate_filename):
        with open(intermediate_filename) as f:
            with open(output_filename, "w") as f_out:
                lines = f.readlines()
                kbs_read = []
                count_aggregated_results = 0
                count = 0
                diffs = [0] * 6
                # Init diffs.
                i = 0
                for line in lines:
                    target_str = "nvme"
                    if target_str in line:
                        previous_sector_diff = diffs[i] 
                        sectors_read = line.split(' ')[1]
                        diff = int(sectors_read) - int(previous_sector_diff)
                        diffs[i] = int(sectors_read)
                        kbs_read.append(int(diff * 512 * 10))
                        count_aggregated_results += 1
                        i = (i + 1) % COUNT_NVME

                        if count_aggregated_results == COUNT_NVME:
                            aggregated_result = numpy.sum(numpy.array(kbs_read))
                            f_out.write("%d, %d\n" %(count, aggregated_result))
                            kbs_read = []
                            count_aggregated_results = 0
                            count += 1

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--file")
    (opts, args) = parser.parse_args()

    parse(opts.file)
