#!/usr/bin/env python3

import os
import config_engine as conf
import numpy

OTHERS = {"rmat-32":
          ["/mnt/sdb1/graph/",
           "/mnt/sdc1/graph/",
           "/mnt/sdd1/graph/",
           "/mnt/sde1/graph/",
           "/mnt/sdf1/graph/",
           "/mnt/sdg1/graph/",
           "/mnt/sdh1/graph/"]
         }

def get_tile_files(dir_path):
  for root, dirs, files in os.walk(dir_path):
    for file in files:
      yield os.path.getsize((os.path.join(root, file)))

def get_stats(dataset, dirs):
  files_size = []
  for d in dirs:
    l = [x for x in get_tile_files(os.path.join(d, "output", dataset, "tile"))]
    files_size.extend(l)
    array = numpy.array(files_size)
  if(len(array) > 0):
     mean = numpy.mean(array) / 1024
     std = numpy.std(array) / 1024
     med = numpy.median(array) / 1024
     print ("%s: mean: %f KB std: %f KB med: %f KB" % (dataset, mean, std, med))

def get_datasets():
  datasets = []
  datasets = datasets + list(conf.SG_GRAPH_SETTINGS_DELIM.keys())
  datasets = datasets + list(conf.SG_GRAPH_SETTINGS_RMAT.keys())
  return datasets

def main():
  datasets = get_datasets()
  for dataset in datasets:
    get_stats(dataset, conf.SG_GRC_OUTPUT_DIRS)
  for dataset, dirs in OTHERS.items():
    get_stats(dataset, dirs)


if __name__ == '__main__':
  main()
