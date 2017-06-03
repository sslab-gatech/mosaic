#!/usr/bin/env python
import os
import optparse
import config_engine

def build(clean, distclean):
    make_dir = os.path.join(config_engine.HERE, "../../")

    make_cmd = ""
    if clean:
        make_cmd = "make clean;"
    elif distclean:
        make_cmd = "make distclean;make cmake;"
    make_cmd += "make;"

    cmd = "cd %s;%s" % (make_dir, make_cmd)
    os.system(cmd)


if __name__ == "__main__":
    # parse options
    parser = optparse.OptionParser()
    parser.add_option("--clean", action="store_true", dest="clean", default = False)
    parser.add_option("--distclean", action="store_true", dest="distclean", default = False)
    (opts, args) = parser.parse_args()

    build(opts.clean, opts.distclean)

