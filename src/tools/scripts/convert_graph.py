#!/usr/bin/env python3
import optparse
import struct

def convert(opts):
    edges_count = 0
    with open(opts.output, "wb") as out_file:
        for line in open(opts.input):
            if line.startswith("#"):
                continue
            (src, tgt) = line.split()
            out_file.write(struct.pack("<l", int(src)))
            out_file.write(struct.pack("<l", int(tgt)))
            edges_count = edges_count + 1
    print("Edges read: %d" % edges_count)

if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--input")
    parser.add_option("--output")
    (opts, args) = parser.parse_args()

    convert(opts)
