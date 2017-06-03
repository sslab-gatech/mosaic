#!/bin/bash

# load common config
LEVEL=../..
source $LEVEL/scripts/config.sh
CURDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# copy files to mic
echo "### copy test binary to mic0"
$SSHPASS ssh $MIC_X "rm -rf ~/ring_buffer/*; mkdir -p ~/ring_buffer"
$SSHPASS scp $CURDIR/kill.sh $MIC_X:~/ring_buffer/
$SSHPASS scp $CURDIR/*_mic $MIC_X:~/ring_buffer/

