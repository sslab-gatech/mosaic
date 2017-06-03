#!/usr/bin/env python
import sys
import math
import numpy

class CheckLabel(object):
    def __init__(self, file_1, file_2):
        self.file_1=file_1 
        self.file_2=file_2 

    def check(self):
        file_1_dict = {}
        file_2_dict = {}
        labels_1 = set()
        labels_2 = set()
        with open(self.file_1) as f:
            for line in iter(f):
                values= line.split(" ")
                v = values[0].strip()
                label = values[1].strip()
                file_1_dict[v] = label
                labels_1.add(label)

        print("File 1 done")
        with open(self.file_2) as f:
            for line in iter(f):
                values= line.split(" ")
                v = values[0].strip()
                label = values[1].strip()
                file_2_dict[v] = label
                labels_2.add(label)
        print("File 2 done")
        print("labels_1: %s" % len(labels_1))
        print("labels_2: %s" % len(labels_2))

        i = 0
        for key, value in file_1_dict.iteritems():
            if(not int(file_2_dict[key]) == int(value)):
                print(i, key, value, file_2_dict[key])
                return
            i += 1

if __name__ == "__main__":
    cl= CheckLabel(sys.argv[1], sys.argv[2])
    cl.check()

