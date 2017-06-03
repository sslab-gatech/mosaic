#!/usr/bin/env python3
import sys
import math
import numpy as np

class CountTwitterPartitions(object):
    def __init__(self, filename, identifier, bits_per_vertex):
        self.meta_info = {
            'twitter_rv': {
                'count': 40103281,
                'seperator' : '\t'
            },
            'twitter_small': {
                'count': 11316811,
                'seperator': ','
            },
            'buzznet': {
                'count': 101168,
                'seperator': ','
            }
        }
        self.filename = filename
        self.identifier = identifier
        self.vertex_per_partition = 2 ** bits_per_vertex
        self.sep = self.meta_info[self.identifier]["seperator"]
        self.count = self.meta_info[self.identifier]["count"]
        self.edge_blocks = []

        self.partition_count = int(2**math.ceil(math.log(self.count // self.vertex_per_partition) / math.log(2)))
        print(self.partition_count)

        self.partitions = [[[] for x in range(self.partition_count)] for x in range(self.partition_count)]

    def generateIndex(self, i, j):
        return (hash(i) % self.count , hash(j) % self.count)

    def getPartition(self, x, y):
        return (x % self.partition_count, y % self.partition_count)

    def readTwitterSetIntoPartitions(self):
        with open(self.filename) as f:
            for line in iter(f):
                line = line.strip()
                vertices = line.split(self.sep)
                (src, tgt) = self.generateIndex(vertices[0], vertices[1])
                (src_part, tgt_part) = self.getPartition(src, tgt)

                #print(vertices[0], vertices[1])
                #print(src, tgt)
                #print(src_part, tgt_part)
                self.partitions[src_part][tgt_part].append((src, tgt))

    def generateEdgeSets(self):
        # Hilbert-order the tiles
        # then build edge-set until reaching (len(src) = 4096 || len(tgt) == 4096)
        edge_block = ({
                "src":set(),
                "tgt":set(),
                "edges":[]
            })
        for i in range(0, self.partition_count**2):
            (x, y) = d2xy(self.partition_count, i)
            for edge in self.partitions[x][y]:
                if len(edge_block["src"]) == self.vertex_per_partition or len(edge_block["tgt"]) == self.vertex_per_partition:
                    self.edge_blocks.append(edge_block)
                    edge_block = ({
                            "src":set(),
                            "tgt":set(),
                            "edges":[]
                        })
                edge_block["src"].add(edge[0])
                edge_block["tgt"].add(edge[1])
                edge_block["edges"].append(edge)
        self.edge_blocks.append(edge_block)

    def print_statistics_partitions(self):
        min_count = numpy.min(self.partitions)
        max_count = numpy.max(self.partitions)
        mean = numpy.mean(self.partitions)
        median = numpy.median(self.partitions)
        stddev = numpy.std(self.partitions)
        print(min_count, max_count, mean, median, stddev)

    def print_statistics_edges(self):
        l = np.array(range(len(self.edge_blocks)))
        i = 0
        for eb in self.edge_blocks:
            l[i] = len(eb["edges"])
            i += 1
        min = np.min(l)
        max = np.max(l)
        mean = np.mean(l)
        median = np.median(l)
        stddev = np.std(l)
        print(min, max, mean, median, stddev)
        # now print percentiles:
        for i in range(10, 100, 10):
            print('%s: %s' % (i, np.percentile(l, i)))


# code taken from https://en.wikipedia.org/wiki/Hilbert_curve
# rotate/flip a quadrant appropriately
def rot(n, x, y, rx, ry):
    if ry == 0:
        if rx == 1:
            x = n - 1 - x
            y = n - 1 - y
        return y, x
    return x, y

def d2xy(n, d):
    assert(d <= n**2 - 1)
    t = d
    x, y = 0, 0
    s = 1
    while s < n:
        rx = 1 & (t // 2)
        ry = 1 & (t ^ rx)
        x, y = rot(s, x, y, rx, ry)
        x += s * rx
        y += s * ry
        t //= 4
        s *= 2
    return x, y

# convert d to (x,y)
def xy2d (n, x, y):
    rx, ry, d = 0, 0, 0
    s = n // 2
    while s > 0:
        rx = (x & s) > 0
        ry = (y & s) > 0
        d += s * s * ((3 * rx) ^ ry)
        x, y = rot(s, x, y, rx, ry)

        s = s // 2
    return d


if __name__ == "__main__":
    ctp = CountTwitterPartitions(sys.argv[1], sys.argv[2], int(sys.argv[3]))
    ctp.readTwitterSetIntoPartitions()
    ctp.generateEdgeSets()
    ctp.print_statistics_edges()

