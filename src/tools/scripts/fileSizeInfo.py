#!/usr/bin/env python
import sys
import math
import numpy as np
import os

class FileSizeInfo(object):
    def __init__(self, directory):
        self.directory = directory

    def printStatistics(self):
      files = [f for f in os.listdir(self.directory) if os.path.isfile(os.path.join(self.directory, f))]
      sizes = np.zeros(len(files))
      i = 0
      for f in files:
        sizes[i] = os.stat(os.path.join(self.directory, f)).st_size
        i = i + 1
      median = np.median(sizes)
      max = np.max(sizes)
      min = np.min(sizes)
      avg = np.average(sizes)
      stddev = np.std(sizes)
      print("Min: %d, Max %d, Median: %d, Mean: %f, Stddev: %f"  % (min, max, median, avg, stddev))


if __name__ == "__main__":
    fsi = FileSizeInfo(sys.argv[1])
    fsi.printStatistics()

