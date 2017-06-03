#!/bin/bash
USAGE="Usage: run.sh [host|mic for where] [0|1 for blocking]"

# argument
if [ "$#" -ne 2 ]; then
    echo $USAGE
    exit 1
fi
WHERE=$1
BLOCKING=$2
NCPU=`nproc`
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
LOG=${STARTIME}-rbs-flow-${WHERE}.log
rm -f ${LOG}

# copy mic files
${CURDIR}/cpmic.sh

# run benchmark
MIC_NTASKS=(57 1 2 4 8 12 24 36 48)
case $NCPU in
    24)
        # host: bumblebee
        HOST_NTASKS=(24 1 2 4 8 12 24 24 24)
        ;;
    240)
        # host: mos001
	HOST_NTASKS=(57 1 2 4 8 12 24 36 48)
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
    for ((i=0;i<${#MIC_NTASKS[@]};++i));
    do
	MTHR=${MIC_NTASKS[i]}
	HTHR=${HOST_NTASKS[i]}
	# Kill running benchmarks
	echo "## Kill running benchmarks"
	$SSHPASS ssh $MIC_X "~/ring_buffer/kill.sh"
	sudo $CURDIR/kill.sh
	sleep 2

	# Run a benchmark
	case $WHERE in
	    host)
		echo "## mic[${MTHR}] --> host[${HTHR}]"
		$SSHPASS ssh $MIC_X "~/ring_buffer/rbs-flow_mic --time ${BTIME} --nthreads ${MTHR} --mic_nthreads ${MTHR} --elm_cnt 5000 --elm_size ${RS} --master 1 --master_node 1 --lport 9999 --rport 9999 --blocking ${BLOCKING}" &
		sleep 5
		sudo ${CURDIR}/rbs-flow_host  --time ${BTIME} --nthreads ${HTHR} --mic_nthreads ${MTHR} --elm_cnt 5000 --elm_size ${RS} --master 0 --master_node 1 --lport 9999 --rport 9999 --blocking ${BLOCKING} | tee -a ${LOG}
		;;
	    mic)
		echo "## host[${HTHR}] --> mic[${MTHR}]"
		sudo ${CURDIR}/rbs-flow_host  --time ${BTIME} --nthreads ${HTHR} --mic_nthreads ${MTHR} --elm_cnt 5000 --elm_size ${RS} --master 1 --master_node 0 --lport 9999 --rport 9999 --blocking ${BLOCKING}&
		sleep 5
		$SSHPASS ssh $MIC_X "~/ring_buffer/rbs-flow_mic --time ${BTIME} --nthreads ${MTHR} --mic_nthreads ${MTHR} --elm_cnt 5000 --elm_size ${RS} --master 0 --master_node 0 --lport 9999 --rport 9999 --blocking ${BLOCKING}" | tee -a ${LOG}
		;;
	esac
	sleep 3
    done
done
