#!/usr/bin/env python3

import os
import subprocess
import multiprocessing
from constants import *

def join(*args):
    return os.path.normpath(os.path.join(*args))

# dir config
SRC_ROOT = join(os.path.dirname(__file__), "../src")
BUILD_ROOT = join(SRC_ROOT, "../build")
TOOLS_ROOT = join(SRC_ROOT, "tools/scripts")
DATA_ROOT = join(SRC_ROOT, "../data")
LOG_ROOT = join(SRC_ROOT, "../log")

# default user
FILE_GROUP = None

# bin config
DBIN_PARTIONER = join(BUILD_ROOT, "Debug-x86_64/tools/grc/grc-partitioner")
DBIN_TILER = join(BUILD_ROOT, "Debug-x86_64/tools/grc/grc-tiler")
DBIN_GRC_IN_MEMORY = join(BUILD_ROOT, "Debug-x86_64/tools/grc/grc-in-memory")
DBIN_RMAT_TILER = join(BUILD_ROOT, "Debug-x86_64/tools/grc/grc-rmat-tiler")
DBIN_RMAT_GENERATOR = join(BUILD_ROOT, "Debug-x86_64/tools/grc/grc-rmat-generator")
DBIN_TILE_INDEXER = join(BUILD_ROOT, "Debug-x86_64/tools/post-grc/post-grc-indexer")
DBIN_EDGE_ENGINE = join(BUILD_ROOT, "Debug-x86_64/lib/core/edge-engine")
DBIN_VERTEX_ENGINE = join(BUILD_ROOT, "Debug-x86_64/lib/core/vertex-engine")
DBIN_MOSAIC = join(BUILD_ROOT, "Debug-x86_64/lib/core/mosaic")

RBIN_PARTIONER = join(BUILD_ROOT, "Release-x86_64/tools/grc/grc-partitioner")
RBIN_TILER = join(BUILD_ROOT, "Release-x86_64/tools/grc/grc-tiler")
RBIN_GRC_IN_MEMORY = join(BUILD_ROOT, "Release-x86_64/tools/grc/grc-in-memory")
RBIN_RMAT_TILER = join(BUILD_ROOT, "Release-x86_64/tools/grc/grc-rmat-tiler")
RBIN_RMAT_GENERATOR = join(BUILD_ROOT, "Release-x86_64/tools/grc/grc-rmat-generator")
RBIN_TILE_INDEXER = join(BUILD_ROOT, "Release-x86_64/tools/post-grc/post-grc-indexer")
RBIN_EDGE_ENGINE = join(BUILD_ROOT, "Release-x86_64/lib/core/edge-engine")
RBIN_VERTEX_ENGINE = join(BUILD_ROOT, "Release-x86_64/lib/core/vertex-engine")
RBIN_MOSAIC = join(BUILD_ROOT, "Release-x86_64/lib/core/mosaic")

# common config
SG_NUM_HASH_DIRS = 512          # #hash dirs
SG_PORT = 5000                  # The base port for Mosaic, is used to connect to the Xeon Phi.
SG_ALGORITHM = "pagerank"       # The default algorithm.
SG_NMIC = 2                     # The default number of edge-engines (on the Xeon Phi or Host) to use.
SG_RUN_ON_MIC = 0               # Whether to instantiate on host or Xeon Phi.
SG_PRINT_ONLY = False           # Whether to only print the commands or also execute them.
SG_DATASET = "twitter-small"    # The default dataset.
SG_IN_MEMORY_MODE = False       # Whether to use the in memory mode, all data will be read into the ringbuffers and never deleted from there.

SG_DEBUG = False                # Activates the debug mode to run the debug binary and print fine-grained logging.
SG_GDB_VERTEX_ENGINE = False    # Starts the vertex engine via gdb.
SG_PERFMON = False              # Controls whether or not to dump the performance monitor every second.

SG_GLOBALS_PATH = join(DATA_ROOT, "globals")                 # The path to the globals (degrees etc.).
SG_FAULT_TOLERANCE_PATH = join(DATA_ROOT, "fault-tolerance") # Where to output the checkpointed vertex array to.

# Perf event collection.
SG_ENABLE_PERF_EVENT_COLLECTION = False # Whether to enable the perf event collection to trace JSON checkpoints.
SG_PATH_PERF_EVENTS = join(DATA_ROOT, "perf-events") # Where to output the JSON perf traces to.

# vertex-engine config
SG_MAX_ITERATIONS = 20  # Number of iterations.
SG_NAPPLIER = 32        # Number of appliers, global,
SG_NGLOBALREDUCER = 2   # Number of global reducers.
SG_NGLOBALFETCHER = 8   # Number of global fetchers.
SG_NREDUCER = 2         # Number of reducers, instantiated PER edge-engine, local. Total number of reducers: SG_NMIC * SG_NREDUCER
SG_NFETCHER = 2         # Number of fetchers, instantiated PER edge-engine, local. Total number of reducers: SG_NMIC * SG_NFETCHER
SG_NINDEXREADER = 1     # Number of index readers PER edge-engine.
SG_ENABLE_LOG = False   # Outputs the vertex values per iteration into a file, separating values by '\n'.
SG_ENABLE_FAULT_TOLERANCE = False # Whether to enable the fault tolerance mode, flushing the vertex values to disk asynchronously per iteration.
# Benchmark options that allow enabling/disabling specific parts of the system.
SG_ENABLE_GLOBAL_FETCHER = True  # Whether to use the global fetchers, if disabled the values will be fetched from the global array directly by the fetchers.
SG_ENABLE_TILE_PARTITIONING = True # Whether to use the tile partitioning, i.e. send multiple tiles for every tile larger than the threshold.

# Controls the local fetcher, options are GlobalFetcher, DirectAccess,
# ConstantValue or Fake.
SG_LOCAL_FETCHER_MODE = "DirectAccess"
# Controls the local reducer, options are GlobalReducer, Locking or Atomic
SG_LOCAL_REDUCER_MODE = "GlobalReducer"
# Controls the global fetcher, options are Active or ConstantValue.
SG_GLOBAL_FETCHER_MODE = "Active"

# Maps edge-engines to the mic they are running on, i.e. mic0 runs edge-engine 0 etc.
SG_EDGE_ENGINE_TO_MIC = [ 0, 1, 2, 3 ] 

# Edge-engine config
SG_NREADER = 1     # When running on the host: Number of readers to instantiate to read the tiles into memory.
SG_NPROCESSOR = 4  # When running on the host: Number of tile processors to actually process the tiles with.

SG_NREADER_MIC = 2    # When running on the Xeon Phi: Number of readers to instantiate to read the tiles into memory.
SG_NPROCESSOR_MIC = 3 # When running on the Xeon Phi: Number of tile processors to actually process the tiles with.

SG_COUNT_FOLLOWERS = 1; # The number of hyperthreads to use per tile processor.

# Ring buffer size configuration for the edge-engine.
SG_RB_SIZE_PROCESSED = 8 * GB             # The size of the ringbuffer to save the result into.
SG_RB_SIZE_READ_TILES = 1 * GB            # The size of the ringbuffer to read tiles from disk into.
SG_RB_SIZE_READ_TILES_IN_MEMORY = 32 * GB # The size of the ringbuffer to read tiles from disk into when running in the in-memory mode AND on the host.
SG_RB_SIZE_HOST_TILES = 8 * GB            # The size of the ringbuffer to receive the tile information from the host (fetcher) into.

# Benchmarking modes.
# Whether to activate the tile processor or not, options are: Active, Noop.
SG_TILE_PROCESSOR_MODE = "Active"
# Where to obtain the values for the tile processor from, options are:
# VertexFetcher, FakeVertexFetcher, ConstantValue.
SG_TILE_PROCESSOR_INPUT_MODE = "VertexFetcher"
# Where to write the output of the tile processor to, options are:
# VertexReducer, FakeVertexReducer, Noop.
SG_TILE_PROCESSOR_OUTPUT_MODE = "VertexReducer"

# datapath
SG_DATAPATH_EDGE_ENGINE = {
        0: join(DATA_ROOT, "datapath", "nvme0"),
        1: join(DATA_ROOT, "datapath", "nvme1")
        }
SG_DATAPATH_VERTEX_ENGINE = SG_DATAPATH_EDGE_ENGINE

# grc config
SG_GRC_NWRITE_THREADS = 16
SG_GRC_NTHREADS_PARTITIONER = 24
SG_GRC_NTHREADS_TILER = 128
SG_GRC_NPARTITION_MANAGERS = 16
SG_GRC_GDB_PARTITIONER = False
SG_GRC_GDB_TILER = False
SG_GRC_RUN_PARTITIONER = True
SG_GRC_RUN_TILER = True
SG_GRC_USE_RLE = True

SG_GRC_RMAT_PORT = 7000
SG_GRC_NMIC = 4
SG_GRC_RMAT_RUN_ON_MIC = True
SG_GRC_RMAT_GEN_THREADS = 228
SG_GRC_RMAT_TILER_DEGREES_NPARTITION_MANAGERS = 16
SG_GRC_RMAT_TILER_TILING_NPARTITION_MANAGERS = 16

# input
SG_ORIG_DATA_PATH = DATA_ROOT
SG_ORIG_DATA_PATH_INPUT = join(SG_ORIG_DATA_PATH, "input")
SG_ORIG_DATA_PATH_OUTPUT = join(SG_ORIG_DATA_PATH, "output")
SG_ORIG_DATA_PATH_PARTITION = join(SG_ORIG_DATA_PATH, "interim")

SG_INPUT_WEIGHTED = {
    "twitter-small": False,
}
SG_INPUT_FILE = {
    "twitter-small": {
        "delim": join(DATA_ROOT, "twitter-small/twitter_rv_small.net"),
        "binary": join(DATA_ROOT, "twitter-small/twitter_rv_small.bin"),
    }
}

# define which algorithms need a weighted dataset
SG_ALGORITHM_WEIGHTED = {
    "pagerank": False,
    "bfs": False,
    "cc": False,
    "spmv": False,
    "sssp": True,
    "bp": True,
    "tc": False
}

SG_ALGORITHM_ENABLE_SELECTIVE_SCHEDULING = {
    "pagerank": False,
    "bfs": True,
    "cc": True,
    "spmv": False,
    "sssp": True,
    "bp": False,
    "tc": False
}

SG_DATASET_DISABLE_SELECTIVE_SCHEDULING = [
    "rmat-32"
]

SG_DELIM_TAB = "tab"
SG_DELIM_COMMA = "comma"
SG_DELIM_SPACE = "space"
SG_DELIM_SEMICOLON = "semicolon"

SG_GRAPH_SETTINGS_DELIM = {
    "twitter-small": {"count_vertices": 2391579, "delimiter": SG_DELIM_TAB, "use_original_ids": False},
    "twitter-full": {"count_vertices": 41652230, "delimiter": SG_DELIM_TAB, "use_original_ids": False},
    "buzznet": {"count_vertices": 101169, "delimiter": SG_DELIM_COMMA, "use_original_ids": False},
    "test": {"count_vertices": 4, "delimiter": SG_DELIM_COMMA, "use_original_ids": False},
    "uk2007": {"count_vertices": 105896555, "delimiter": SG_DELIM_SPACE, "use_original_ids": False},
    "yahoo": {"count_vertices": 1413511394, "delimiter": SG_DELIM_SPACE, "use_original_ids": True},
    "wdc2012": {"count_vertices": 3563602789, "delimiter": SG_DELIM_TAB, "use_original_ids": True},
    "wdc2014": {"count_vertices": 1724573718, "delimiter": SG_DELIM_TAB, "use_original_ids": True}
}

SG_GRAPH_SETTINGS_RMAT = {
    "rmat-16": {"count_vertices": 2**16, "count_edges": 2**20, "use_original_ids": True},
    "rmat-22": {"count_vertices": 2**22, "count_edges": 2**26, "use_original_ids": True},
    "rmat-24": {"count_vertices": 2**24, "count_edges": 2**28, "use_original_ids": True},
    "rmat-27": {"count_vertices": 2**27, "count_edges": 2**31, "use_original_ids": True},
    "rmat-32-orig": {"count_vertices": 2**32, "count_edges": 2**36, "use_original_ids": True},
    "rmat-32": {"count_vertices": 2**32, "count_edges": 10**12, "use_original_ids": True},
    "rmat-33": {"count_vertices": 2**33, "count_edges": 2**40, "use_original_ids": True},
}

SG_GRC_PARTITION_DIRS = [
    join(DATA_ROOT, "partition1"),
    join(DATA_ROOT, "partition2"),
]

SG_GRC_OUTPUT_DIRS = [
    DATA_ROOT,
]
