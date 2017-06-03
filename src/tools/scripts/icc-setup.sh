#!/bin/bash

OPTS=(/opt/intel/bin
      /opt/intel/compilers_and_libraries_2016/linux/bin
      /opt/intel/compilers_and_libraries_2016.2.181/linux/bin)

for d in ${OPTS[@]}; do
  if [[ -e $d/compilervars.sh ]]; then
    INTEL=$d
  fi
done

if [[ ! -n $INTEL ]]; then
  echo "failed to locate copmilervars.sh"
  exit 1
fi

export PATH=$INTEL:$PATH
source $INTEL/compilervars.sh intel64
