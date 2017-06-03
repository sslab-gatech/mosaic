#!/usr/bin/env python
import sys
import os
import math
import numpy

def isclose(a, b, rel_tol=1e-04, abs_tol=0.0):
    return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

def generateFilenameOld(iteration):
    return "result_" + str(iteration) + ".dat"

def generateFilenameNew(iteration):
    return "result-" + format(iteration, '08d') + ".dat"

class CheckOutput(object):
    def __init__(self, dir_1, dir_2, iterations):
        self.dir_1 = dir_1
        self.dir_2 = dir_2
        self.iterations = iterations

    def check(self):
        for iteration in range(0, self.iterations):
            file_name_old = generateFilenameNew(iteration)
            file_name_new = generateFilenameNew(iteration)
            print("Checking %s vs %s" % (file_name_old, file_name_new))
            file_1 = os.path.join(self.dir_1, file_name_old)
            file_2 = os.path.join(self.dir_2, file_name_new)

            file_1_dict = {}
            file_2_dict = {}
            with open(file_1) as f:
                for line in iter(f):
                    values= line.split(" ")
                    v = values[0].strip()
                    pr = values[1].strip()
                    file_1_dict[v] = pr

            with open(file_2) as f:
                for line in iter(f):
                    values= line.split(" ")
                    v = values[0].strip()
                    pr = values[1].strip()
                    file_2_dict[v] = pr

            print("Sizes %s %s" % (len(file_1_dict), len(file_2_dict)))
            i = 0
            for key, value in file_1_dict.iteritems():
                if(not isclose(float(file_2_dict[key]), float(value))):
                    print(i, key, value, file_2_dict[key])
                    return
                i += 1
            print("%s correct" % iteration)

if __name__ == "__main__":
    co = CheckOutput(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    co.check()

