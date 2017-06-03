#!/usr/bin/env python
import sys
import math
import numpy as np

class CountVertices(object):
    def __init__(self, filename, identifier):
        self.meta_info = {
            'twitter_rv': {
                'seperator' : '\t'
           },
            'buzznet': {
                'seperator': ','
            }
        }
        self.filename = filename
        self.identifier = identifier
        self.sep = self.meta_info[self.identifier]["seperator"]
        self.vertices = set()

    def readVertices(self):
        with open(self.filename) as f:
            for line in iter(f):
                line = line.strip()
                vertices = line.split(self.sep)
                src = vertices[0]
                tgt = vertices[1]
                if src not in self.vertices:
                  self.vertices.add(src)
                if tgt not in self.vertices:
                  self.vertices.add(tgt)

    def printStatistics(self):
      print(len(self.vertices))

if __name__ == "__main__":
    ctp = CountVertices(sys.argv[1], sys.argv[2])
    ctp.readVertices()
    ctp.printStatistics()

