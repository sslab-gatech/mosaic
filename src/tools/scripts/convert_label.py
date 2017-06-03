#!/usr/bin/env python
import sys
import math
import numpy

class ConvertLabel(object):
    def __init__(self, file_1, file_2):
        self.file_1=file_1
        self.file_2=file_2

    def convert(self):
        file_1_dict = {}
        file_2_dict = {}
        with open(self.file_1) as f:
            for line in iter(f):
                values= line.split(" ")
                v = values[0].strip()
                pr = values[1].strip()
                file_1_dict[v] = pr

        print("file read")

        labels = {}
        for key, value in file_1_dict.iteritems():
            labels[value] = value

        for key, value in file_1_dict.iteritems():
            labels[value] = min(labels[value], key)

        with open(self.file_2, 'w') as f:
            for key, value in file_1_dict.iteritems():
                f.write("%s %s\n" % (key, labels[value]))
            f.close()

if __name__ == "__main__":
    cl= ConvertLabel(sys.argv[1], sys.argv[2])
    cl.convert()

