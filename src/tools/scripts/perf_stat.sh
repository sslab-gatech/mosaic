#!/bin/sh

# $1 --> sample coming from iobench_stat.sh
# $2 --> filename

while :
do
  sudo perf stat -a -d -o $2 --append sleep $1
done
