#!/bin/bash

nmic=5
filename="../../../config/config_ramjet.py"

replace_str () {
  line=`grep -n "${1}" ${3} | head -1 | cut -d':' -f1`
  sed -i "${line}d" $3
  sed -i "${line}i ${1} = ${2}" ${3}
}

for i in `seq 1 ${nmic}`
do
  # change the num_xeon_phi and sg_nmic in config_ramjet.py
  replace_str "NUM_XEON_PHIS" ${i} ${filename}
  replace_str "SG_NMIC" ${i} ${filename}
  # sleep
  sleep 2
  # rebalance
  ./rebalance_input.py --dataset=wdc2014
  # wait for some time
  sleep 5
  for t in `seq 5 5 50`
  do
    # change the sg_nrprocessor_mic in config_ramjet.py
    echo "$i: $t" >> out-${i}-scalability | tee
    echo "=================" >> out-${i}-scalability | tee
    replace_str "SG_NPROCESSOR_MIC" ${t} ${filename}
    ./killall_engines.py
    sleep 10
    ./run_all.py --dataset=wdc2014 --max-iterations=5 --algorithm=pagerank --benchmark-mode 2>> out-${i}-scalability | tee
    # sleep for a long time
    sleep 20
  done
done
