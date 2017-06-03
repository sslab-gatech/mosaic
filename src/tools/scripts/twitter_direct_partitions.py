#!/usr/bin/env python3
import sys
import numpy
import math

class CountTwitterPartitions(object):

    def __init__(self, filename, identifier):
        self.meta_info = {
            'twitter_rv': {
                'count': 40103281,
                'seperator' : '\t'
            },
            'twitter_rv_small': {
                'count': 11316811,
                'seperator': ','
            }
        }
        self.filename = filename
        self.identifier = identifier
        self.vertex_per_partition = 2**14
        partition_count = self.meta_info[self.identifier]["count"] / self.vertex_per_partition
        partition_count = math.ceil(partition_count) + 1
        #print(partition_count)
        self.partitions = numpy.zeros((partition_count, partition_count))

    def readNumberOfVertices(self):
        vertex_set = set()
        with open(self.filename) as f:
            for line in iter(f):
                line = line.strip()
                vertices = line.split(self.meta_info[self.identifier]["seperator"])
                src = vertices[0]
                if src not in vertex_set:
                    vertex_set.add(src)
        self.vertex_count = len(vertex_set)
        print(self.vertex_count)

    def readTwitterSetIntoPartitions(self):
        with open(self.filename) as f:
            for line in iter(f):
                line = line.strip()
                vertices = line.split(self.meta_info[self.identifier]["seperator"])
                src = vertices[0]
                tgt = vertices[1]
                src_index = math.floor((int(src) % self.meta_info[self.identifier]["count"]) / self.vertex_per_partition)
                tgt_index = math.floor((int(tgt) % self.meta_info[self.identifier]["count"]) / self.vertex_per_partition)
                #print(src_index, tgt_index)
                self.partitions[src_index][tgt_index] += 1

    def output(self):
        min_count = numpy.min(self.partitions)
        max_count = numpy.max(self.partitions)
        mean = numpy.mean(self.partitions)
        median = numpy.median(self.partitions)
        stddev = numpy.std(self.partitions)
        print(min_count, max_count, mean, median, stddev)


if __name__ == "__main__":
    ctp = CountTwitterPartitions(sys.argv[1], sys.argv[2])
    #ct.readNumberOfVertices()
    ctp.readTwitterSetIntoPartitions()
    ctp.output()
