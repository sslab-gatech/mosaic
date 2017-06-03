#!/bin/bash

cat $1 |grep Second |awk '{print $4","}' |sed 's/tiles-fetch://' > fetch.dat && \
sshpass -p phi scp root@mic0:edge-engine-0.err ./ && \
cat edge-engine-0.err |grep Second |awk '{print $10 $11}' |sed 's/tiles-read://' |sed 's/tiles-proc://' > engine.dat && \
paste fetch.dat engine.dat > tile-stat.dat &&
./plot.gp && \
echo "Done Processing"

