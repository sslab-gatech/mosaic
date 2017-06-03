#!/usr/bin/env python
import optparse

def convertAdjacencyListToEdgeList(input_file, output_file):
    with open(input_file) as f_in:
        with open(output_file, 'w') as f_out:
            for line in iter(f_in):
                values = line.strip().split(" ")
                if(len(values) >= 2):
                    # only convert if the second value is not 0, otherwise no edge
                    if not int(values[1]) == 0:
                        for tgt_index in range(2, len(values)):
                            f_out.write("%s %s\n" % (values[0], values[tgt_index]))

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--input")
    parser.add_option("--output")
    (opts, args) = parser.parse_args()

    convertAdjacencyListToEdgeList(opts.input, opts.output)

