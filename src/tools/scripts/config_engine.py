#!/usr/bin/env python3
import os
import sys
import socket
import pprint
import importlib
import string

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
CONF = os.path.join(ROOT, "config")
HOST = socket.gethostname()
LOG = os.path.join(ROOT, "log")

sys.path.append(CONF)

def is_proper_conf(key):
  return key.isupper() and not key.startswith("_")

def import_config(opt):
  pn = os.path.join(CONF, opt + ".py")
  if not os.path.exists(pn):
    return
  c = importlib.import_module(opt)
  for (k, v) in c.__dict__.items():
    if is_proper_conf(k):
      globals()[k] = v

# NOTE. load configs either from SC_CONF env or via arguments
confs = [HOST]
# NOTE: Disabled to not interfere with other scripts
# if len(sys.argv) == 2:
#   confs = sys.argv[1:]

if "SC_CONF" in os.environ:
  assert(len(sys.argv) == 1)
  confs = os.environ["SC_CONF"].split(",")

confs = ["default"] + ["config_" + c for c in confs]
for c in confs:
  import_config(c)

def getWeightedName(dataset, weighted = None):
    if weighted is None:
        weighted = SG_ALGORITHM_WEIGHTED[SG_ALGORITHM]
    if weighted:
        return "%s-%s" % (dataset, "weighted")
    return dataset

def getGlobalsDir(dataset, weighted = None):
    if weighted is None:
        weighted = SG_ALGORITHM_WEIGHTED[SG_ALGORITHM]
    return os.path.join(SG_GLOBALS_PATH, getWeightedName(dataset, weighted))

def getFaultToleranceDir(dataset, weighted = None):
    if weighted is None:
        weighted = SG_ALGORITHM_WEIGHTED[SG_ALGORITHM]
    return os.path.join(SG_FAULT_TOLERANCE_PATH, getWeightedName(dataset, weighted))

def getPerfEventsDir(dataset, weighted = None):
    if weighted is None:
        weighted = SG_ALGORITHM_WEIGHTED[SG_ALGORITHM]
    return os.path.join(SG_PATH_PERF_EVENTS, getWeightedName(dataset, weighted))

def getGrcPartitionDir(dataset, index, weighted = None):
    if weighted is None:
        weighted = SG_ALGORITHM_WEIGHTED[SG_ALGORITHM]
    return os.path.join(SG_GRC_PARTITION_DIRS[index], "interim", getWeightedName(dataset, weighted))

def getGrcMetaDir(dataset, index, weighted = None):
    if weighted is None:
        weighted = SG_ALGORITHM_WEIGHTED[SG_ALGORITHM]
    return os.path.join(SG_GRC_OUTPUT_DIRS[index], "output", getWeightedName(dataset, weighted), "meta/mic0")

def getGrcTileDir(dataset, index, weighted = None):
    if weighted is None:
        weighted = SG_ALGORITHM_WEIGHTED[SG_ALGORITHM]
    return os.path.join(SG_GRC_OUTPUT_DIRS[index], "output", getWeightedName(dataset, weighted), "tile/mic0")

def getMicSubdir(datapath, dataset, identifier, mic_index, weighted = None):
    if weighted is None:
        weighted = SG_ALGORITHM_WEIGHTED[SG_ALGORITHM]
    return os.path.join(datapath, getWeightedName(dataset, weighted), identifier, "mic%s" % (mic_index))

if __name__ == '__main__':
  import copy
  conf = {}
  for (k, v) in copy.copy(globals()).items():
    if is_proper_conf(k):
      conf[k] = v
  pprint.pprint(conf)
  exit(0)
