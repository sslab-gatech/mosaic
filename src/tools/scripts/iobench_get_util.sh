#!/bin/bash

stat_path=$1
if [[ $# -eq 0 ]]
	then
	echo "$0 <stat-collected-path>"
	exit 1
fi

CPU_USR=`sar -f ${stat_path}/sar_raw.out |grep -e '^Average' |awk '{print $3}'`
CPU_SYS=`sar -f ${stat_path}/sar_raw.out |grep -e '^Average' |awk '{print $5}'`
CPU_IOW=`sar -f ${stat_path}/sar_raw.out |grep -e '^Average' |awk '{print $6}'`
CPU_IDL=`sar -f ${stat_path}/sar_raw.out |grep -e '^Average' |awk '{print $8}'`

echo "${CPU_USR}, ${CPU_SYS}, ${CPU_IOW}, ${CPU_IDL}"

