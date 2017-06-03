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

def parse(folder):
    filename = os.path.join(folder, "iostat.out")
    intermediate_filename = os.path.join(folder, "iostat.out.stripped")
    # Preprocess by only parsing relevant column.
    cmd = "awk '{ print $1, $5}' %s > %s" % (filename, intermediate_filename)
    os.system(cmd)

    output_filename = "plot.gp"
    if os.path.isfile(intermediate_filename):
        with open(intermediate_filename) as f:
            with open(output_filename, "w") as f_out:
                lines = f.readlines()
                kbs_read = []
                count_aggregated_results = 0
                count = 0
                for line in lines:
                    target_str = "nvme"
                    if target_str in line:
                        kb_read = line.split(' ')[-1]
                        kbs_read.append(int(kb_read))
                        count_aggregated_results += 1

                        if count_aggregated_results == COUNT_NVME:
                            aggregated_result = numpy.sum(numpy.array(kbs_read))
                            f_out.write("%d, %d\n" %(count, aggregated_result))
                            kbs_read = []
                            count_aggregated_results = 0
                            count += 1

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--folder")
    (opts, args) = parser.parse_args()

    parse(opts.folder)
