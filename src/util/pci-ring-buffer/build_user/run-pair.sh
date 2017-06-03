#!/bin/bash
USAGE="Usage: run.sh [host|mic for where] [0|1 for blocking]"

# argument
if [ "$#" -ne 2 ]; then
    echo $USAGE
    exit 1
fi
WHERE=$1
case $WHERE in
    host)
	NCPU=`nproc`
	;;
    mic)
	NCPU=57
	;;
    *)
	echo $USAGE
	exit 1
	;;
esac
BLOCKING=$2

BTIME=5

run_tc () {
    echo "## $1"
    $1; sleep 1
    echo ">> $1"
}

# load common config
LEVEL=../..
source $LEVEL/scripts/config.sh
CURDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

# set log file names
STARTIME=`date +%F-%R-%S`
LOG=${STARTIME}-rb-pair-${WHERE}-${DIRECTION}.log
rm -f ${LOG}

# run benchmark
case $NCPU in
    57)
        # mic
        NTASKS=(57 1 2 4 8 12 24 36 48)
	$CURDIR/cpmic.sh
        ;;
    24)
        # host: bumblebee
        NTASKS=(24 12 1 2 4 8)
        ;;
    240)
        # host: mos001
        NTASKS=(240 120 1 2 4 15 30 45 60 75 90 105)
        ;;
    *)
        echo "## Unknown CPU number: $NCPU"
        exit 1
        ;;
esac
REQ_SIZES=(64 128 256 512 1K 4K 16K 64K 1M 4M)
sync; sleep 1

for RS in "${REQ_SIZES[@]}"
do
    for NTHR in "${NTASKS[@]}"
    do
	# Kill running benchmarks
	echo "## Kill running benchmarks"
	$SSHPASS ssh $MIC_X "~/ring_buffer/kill.sh"
	sudo $CURDIR/kill.sh
	sleep 2

	# Run a benchmark
	case $WHERE in
	    host)
		echo "## Run a benchmark at host " `hostname`
		${CURDIR}/rb-pair_host --time ${BTIME} --nthreads ${NTHR} --elm_cnt=5000 --elm_size ${RS} --blocking ${BLOCKING}| tee -a ${LOG}
		;;
	    mic)
		echo "## Run a benchmark at " $MIC_X
		$SSHPASS ssh $MIC_X "~/ring_buffer/rb-pair_mic --time ${BTIME} --nthreads ${NTHR} --elm_cnt=5000 --elm_size ${RS} --blocking ${BLOCKING}"  | tee -a ${LOG}
		;;
	esac
	sleep 3
    done
done
