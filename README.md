# Mosaic: Processing a Trillion-Edge Graph on a Single Machine (EuroSys'17)

This repository is the main codebase for the Mosaic project, containing both
the utility used to generate and convert graphs as well as the main graph
engine, Mosaic.

This version of Mosaic should be runnable on most Linux-based systems.
Mosaic builds for both the CPU-based version as well as the Xeon Phi-based
version from almost the same code-base, although the Xeon Phi-based version
requires a specialized hardware setup and a modified Kernel to support our
9p-based file system.
If you would like to setup the Xeon Phi-based version, please get in touch with
us.

# Prerequisites 
* Cmake 2.8 or newer
* A recent gcc (4.8+) or icc 
* [Googletest](https://github.com/google/googletest) needs to be installed, this
can be done following [these
instructions](https://github.com/google/googletest/blob/master/googletest/README.md).

# Build
To build Mosaic, follow these steps:
```
$ cd src
$ make cmake
$ make
```
This will build the binaries of Mosaic, in a _release_ and _debug_
configuration into the _build_-folder.

# Configuration
Mosaic supports multiple configurations, for different machines, to co-exist
and to be picked via the hostname of the machine.
As such, we recommend localizing the configuration in the `config/` directory
to suit the needs of your environment (e.g., if your server is called `foo`,
create `config/config_foo.py`).
We provide an example with the configuration of our `ramjet` server, as
presented in the paper (`config/config_ramjet.py`).

# Datasets and Preparation
In the paper, we use six datasets (3 real-world, 3 synthetic):
* uk-2007: http://law.di.unimi.it/webdata/uk-2007-05/
* Twitter: http://an.kaist.ac.kr/traces/WWW2010.html
* Hyperlinks 2014: http://webdatacommons.org/hyperlinkgraph/2014-04/download.html
* rmat-24, rmat-27 and rmat-trillion are generated using the Mosaic generator
  itself, with the same parameters as graph500.

A smaller test dataset we use is [twitter_rv_small](http://www.ftpstatus.com/file_properties.php?sname=ftp.tugraz.at&fid=44).

These graphs are already included in the configuration of Mosaic.
If you wish to add another graph dataset, it has to be added in the
configuration files.
This is done adding the appropriate settings in `config/default.py` (i.e., the
variables `SG_INPUT_WEIGHTED`, `SG_INPUT_FILE` and `SG_GRAPH_SETTINGS_DELIM`),
following a similar pattern as the other graphs.

## Binary Input
For faster generation, Mosaic can read graphs from a binary format (the same as
GridGraph, in case one happens to use both for benchmarking purposes).
We provide a script to convert txt-based graphs to the binary format:
```
$ cd src/tools/scripts
$ ./convert_graph.py --input /data/graph/input/twitter-small/twitter_rv_small.net --output /data/graph/input/twitter-small/twitter_rv_small.bin
```

# Generate Graphs
With the configuration setup correctly, generating the Mosaic-internal format
of a graph can be started via the `src/tools/scripts/generate_graph.py` script.

Examples of this conversion:
```
$ cd src/tools/scripts
$ ./generate_graph.py --dataset uk2007 --binary --in-memory
$ ./generate_graph.py --dataset uk2007 --binary --in-memory --weighted
$ ./generate_graph.py --dataset rmat-27 --rmat --in-memory
```

# Executing Mosaic
## Preparation
Mosaic supports using multiple disk-paths for input files.
This is controlled via the parameter `SG_NMIC` which sets the number of edge
processors that will be started.
Each edge processor can read either of the *tile* and *meta* data from a
different partition.
This is controlled by the `SG_DATAPATH_VERTEX_ENGINE` configuration (for the
meta-data) and the `SG_DATAPATH_EDGE_ENGINE` configuration for the tile data.

After setting these parameters, the input-data from the converted graph
datasets needs to be copied into the correct file system locations (as set by
the `SG_DATAPATH_VERTEX_ENGINE` and `SG_DATAPATH_EDGE_ENGINE` configuration).
This is done via the *rebalance* script, which needs to be run anytime the
data path or the number of edge processors (`SG_NMIC` configuration) is changed.
```
$ cd src/tools/scripts
$ ./rebalance_input.py --dataset twitter-small
$ ./rebalance_input.py --dataset twitter-small-weighted
```

## Optimization: Active Tiles
This optimization enables Mosaic to skip the computation for tiles without
active vertices and edges.
To build the index structure needed for this optimization (a very useful
optimization for BFS, Connected Components, ...), run the following commands
(specific to every dataset, here `twitter-small`):
```
$ cd src/tools/scripts
$ ./run_tiles_indexer.py --dataset twitter-small
```

## Running Mosaic
Finally, Mosaic is ready to run, e.g. for executing Pagerank or Single-Source
Shortest Path on the `twitter-small` dataset:
```
$ cd src/tools/scripts
$ ./run_mosaic.py --dataset twitter-small --algorithm pagerank --max-iteration 20
$ ./run_mosaic.py --dataset twitter-small-weighted --algorithm sssp --max-iteration 50
```
Mosaic will present statistics on the execution time per iteration and
optionally log the results for later analysis.

## Analyzing Results
To run a benchmark suite of algorithms and datasets as well as a quick analysis
of these, we provide two scripts, you might need to customize the datasets and
algorithms executed and analyzed by these scripts to match your configuration:
```
$ cd src/tools/scripts
$ ./run_benchmark.py
$ ./parse_log.py
```

# Authors
* Steffen Maass [steffen.maass@gatech.edu](mailto:steffen.maass@gatech.edu)
* Changwoo Min [changwoo@gatech.edu](mailto:changwoo@gatech.edu)
* Sanidhya Kashyap [sanidhya@gatech.edu](mailto:sanidhya@gatech.edu)
* Woonhak Kang [woonhak.kang@gatech.edu](mailto:woonhak.kang@gatech.edu)
* Mohan Kumar [mohankumar@gatech.edu](mailto:mohankumar@gatech.edu)
* Taesoo Kim [taesoo@gatech.edu](taesoo@gatech.edu)

# Citation
* EuroSys'17 (*Best Student Paper*):
```
@inproceedings{maass:mosaic,
  title        = {{Mosaic: Processing a Trillion-Edge Graph on a Single Machine}},
  author       = {Steffen Maass and Changwoo Min and Sanidhya Kashyap and Woonhak Kang and Mohan Kumar and Taesoo Kim},
  booktitle    = {Proceedings of the 12th European Conference on Computer Systems (EuroSys)},
  month        = apr,
  year         = 2017,
  address      = {Belgrade, RS},
}
```
