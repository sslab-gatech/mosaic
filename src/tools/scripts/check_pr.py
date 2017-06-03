#!/usr/bin/env python
import sys
import math
import numpy

def nearly_equal(a,b,sig_fig=4):
    return ( a==b or
             int(a*10**sig_fig) == int(b*10**sig_fig)
           )

class CheckPageRank(object):
    def __init__(self, file_1, file_2):
        self.file_1=file_1 
        self.file_2=file_2 

    def check(self):
        file_1_dict = {}
        file_2_dict = {}
        with open(self.file_1) as f:
            for line in iter(f):
                values= line.split(" ")
                v = values[0].strip()
                pr = values[1].strip()
                file_1_dict[v] = pr

        print("File 1 done")
        with open(self.file_2) as f:
            for line in iter(f):
                values= line.split(" ")
                v = values[0].strip()
                pr = values[1].strip()
                file_2_dict[v] = pr
        print("File 2 done")

        i = 0
        for key, value in file_1_dict.iteritems():
            if(not nearly_equal(float(file_2_dict[key]), float(value))):
                print(i, key, value, file_2_dict[key])
                return
            i += 1

if __name__ == "__main__":
    cpr= CheckPageRank(sys.argv[1], sys.argv[2])
    cpr.check()

