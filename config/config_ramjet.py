#!/usr/bin/env python3

import os
from os.path import join
import subprocess
import multiprocessing
from constants import *

LOG_ROOT = "/data/graph/log"
NUM_XEON_PHIS = 4

FILE_GROUP = "adm"

NINEP_NVME = {}

# common config
SG_PORT = 5000
SG_ALGORITHM = "pagerank"
SG_NMIC = 4
SG_RUN_ON_MIC = 0
SG_DEBUG = False
SG_PRINT_ONLY = False
SG_DATASET = "twitter-small"

# vertex-engine config
SG_MAX_ITERATIONS = 20
SG_NAPPLIER = 24
SG_NGLOBALREDUCER = 2
SG_NGLOBALFETCHER = 4
SG_NREDUCER = 2
SG_NFETCHER = 2
SG_NINDEXREADER = 1
SG_IN_MEMORY_MODE = False
SG_ENABLE_LOG = False
SG_ENABLE_FAULT_TOLERANCE = False

# maps edge-engines to the mic they are running on, i.e. mic0 runs edge-engine 0 etc.
SG_EDGE_ENGINE_TO_MIC = [ 0, 2, 1, 3 ]

SG_DATAPATH_VERTEX_ENGINE = {}
# enable different nvme's for different MICs
SG_DATAPATH_VERTEX_ENGINE[0] = "/data/graph/data"
SG_DATAPATH_VERTEX_ENGINE[1] = "/data/graph/data"
SG_DATAPATH_VERTEX_ENGINE[2] = "/data/graph/data"
SG_DATAPATH_VERTEX_ENGINE[3] = "/data/graph/data"
SG_GDB_VERTEX_ENGINE = False

# edge-engine config
SG_DATAPATH_EDGE_ENGINE = {}
# enable different nvme's for different MICs
SG_DATAPATH_EDGE_ENGINE[0] = "/data/graph/data"
SG_DATAPATH_EDGE_ENGINE[1] = "/data/graph/data"
SG_DATAPATH_EDGE_ENGINE[2] = "/data/graph/data"
SG_DATAPATH_EDGE_ENGINE[3] = "/data/graph/data"

SG_RB_SIZE_PROCESSED = 1 * GB + 512 * MB
SG_RB_SIZE_READ_TILES = 3 * GB
SG_RB_SIZE_READ_TILES_IN_MEMORY = 32 * GB
SG_RB_SIZE_HOST_TILES = 3 * GB
SG_NREADER = 1
SG_NPROCESSOR = 2

SG_NREADER_MIC = 5
SG_NPROCESSOR_MIC = 55

SG_COUNT_FOLLOWERS = 1; # The number of hyperthreads to use per tile processor.

SG_GLOBALS_PATH = "/data/graph/globals"
SG_FAULT_TOLERANCE_PATH = "/data/graph/fault-tolerance"

SG_ORIG_DATA_PATH = "/data/graph"
SG_ORIG_DATA_PATH_INPUT = os.path.join(SG_ORIG_DATA_PATH, "input")
SG_ORIG_DATA_PATH_OUTPUT = os.path.join(SG_ORIG_DATA_PATH, "output")
SG_ORIG_DATA_PATH_PARTITION = os.path.join(SG_ORIG_DATA_PATH, "interim")

SG_INPUT_FILE = {
        "test": {
            "delim": os.path.join(SG_ORIG_DATA_PATH_INPUT, "test/test.csv"),
            "binary": os.path.join(SG_ORIG_DATA_PATH_INPUT, "test/test.bin"),
        },
        "buzznet": {
            "delim": os.path.join(SG_ORIG_DATA_PATH_INPUT, "buzznet/edges.csv"),
            "binary": os.path.join(SG_ORIG_DATA_PATH_INPUT, "buzznet/edges.bin"),
        },
        "twitter-small": {
            "delim": os.path.join(SG_ORIG_DATA_PATH_INPUT, "twitter-small/twitter_rv_small.net"),
            "binary": os.path.join(SG_ORIG_DATA_PATH_INPUT, "twitter-small/twitter_rv_small.bin"),
        },
        "twitter-full": {
            "delim": os.path.join(SG_ORIG_DATA_PATH_INPUT, "twitter-full/twitter_rv.net"),
            "binary": os.path.join(SG_ORIG_DATA_PATH_INPUT, "twitter-full/twitter_rv.bin"),
        },
        "uk2007": {
            "delim": os.path.join(SG_ORIG_DATA_PATH_INPUT, "uk2007/uk2007.net"),
            "binary": os.path.join(SG_ORIG_DATA_PATH_INPUT, "uk2007/uk2007.bin"),
        },
        "wdc2014": {
            "delim": os.path.join(SG_ORIG_DATA_PATH_INPUT, "wdc14/hyperlink14.net"),
            "binary": os.path.join(SG_ORIG_DATA_PATH_INPUT, "wdc14/hyperlink14.bin"),
        },
}

SG_INPUT_WEIGHTED = {
        "buzznet": False,
        "rmat24": False,
        "rmat27": False,
        "test": False,
        "twitter-small": False,
        "twitter-full": False,
        "uk2007": False,
        "wdc2014": False
}

SG_GRC_PARTITION_DIRS = [
        "/data/graph",
]

SG_GRC_OUTPUT_DIRS = [
        "/data/graph",
]
