#!/bin/sh
#
# Check if iostat, sar and vmstat are in the user's path.  If not, set to
# true so the script doesn't complain.
#

which iopp > /dev/null 2>&1
if [ $? -eq 0 ]; then
	IOPP=iopp
else
	IOPP=true
fi

which iostat > /dev/null 2>&1
if [ $? -eq 0 ]; then
	IOSTAT=iostat
else
	IOSTAT=true
fi

which mpstat > /dev/null 2>&1
if [ $? -eq 0 ]; then
	MPSTAT=mpstat
else
	MPSTAT=true
fi

which sar > /dev/null 2>&1
if [ $? -eq 0 ]; then
	SAR=sar
else
	SAR=true
fi

which vmstat > /dev/null 2>&1
if [ $? -eq 0 ]; then
	VMSTAT=vmstat
else
	VMSTAT=true
fi

which perf > /dev/null 2>&1
if [ $? -eq 0 ]; then
  PERF=perf_stat.sh
else
  PERF=true
fi

if [ $# -lt 1 ]; then
    echo "usage: $0 --outdir <output_dir> --sample <sample_length>"
    echo "       $0 --stop"
    echo "	<output_dir> will be created if it doesn't exist"
    exit 1
fi

while :
do
	case $# in
	0)
		break
		;;
	esac

	option=$1
	shift

	orig_option=$option
	case $option in
	--*)
		;;
	-*)
		option=-$option
		;;
	esac

	case $option in
	--*=*)
		optarg=`echo $option | sed -e 's/^[^=]*=//'`
		arguments="$arguments $option"
		;;
	 --outdir | --sample)
		optarg=$1
		shift
		arguments="$arguments $option=$optarg"
		;;
	--stop)
		echo "Killing ${SAR} ${IOPP} ${IOSTAT} ${MPSTAT} ${VMSTAT}..."
		sudo killall -q ${SAR} sadc ${IOPP} ${IOSTAT} ${MPSTAT} ${VMSTAT} 
        sudo pkill perf_stat
        ./show_log.py |grep Second | grep -v "edges-processed 0" > edges.out
        rm edges.dat
		exit;
		;;
	esac

	case $option in
	--outdir)
		OUTPUT_DIR=$optarg
		;;
	--sample)
		SAMPLE_LENGTH=$optarg
		;;
	esac
done

if [ -z $OUTPUT_DIR ]; then
	echo "use --outdir"
	exit 1
fi

if [ -z $SAMPLE_LENGTH ]; then
	echo "use --sample"
	exit 1
fi

# create the output directory in case it doesn't exist
mkdir -p $OUTPUT_DIR

echo "starting system statistics data collection"

# collect all sar data in binary form
${SAR} -o $OUTPUT_DIR/sar_raw.out $SAMPLE_LENGTH > ${OUTPUT_DIR}/sar.out &

# collect i/o data per process
${IOPP} -c ${SAMPLE_LENGTH} > ${OUTPUT_DIR}/iopp.out &

# collect i/o data per physical device
${IOSTAT} -d $SAMPLE_LENGTH > $OUTPUT_DIR/iostat.out &
${IOSTAT} -d -x $SAMPLE_LENGTH > $OUTPUT_DIR/iostatx.out &

# collect mpstat data
${MPSTAT} -P ALL ${SAMPLE_LENGTH} > ${OUTPUT_DIR}/mpstat.out &

#collect blktrace data -- gihwan 
#db_device="/dev/sdf1"
#echo "blktrace " 
#sudo blktrace -d ${db_device} -o ${OUTPUT_DIR}/blktrace.out & 

# collect vmstat data
VMSTAT_OUTPUT=$OUTPUT_DIR/vmstat.out
OS=`uname`
if [ "x${OS}" = "xSunOS" ]; then
	${VMSTAT} $SAMPLE_LENGTH $ITERATIONS > $VMSTAT_OUTPUT &
else
	${VMSTAT} -n $SAMPLE_LENGTH > $VMSTAT_OUTPUT &
fi

#collect perf stats
./${PERF} ${SAMPLE_LENGTH} ${OUTPUT_DIR}/perf.out &

touch ${OUTPUT_DIR}/edges.out
ln -s ${OUTPUT_DIR}/edges.dat edges.out

exit 0
