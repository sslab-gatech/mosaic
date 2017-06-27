SHELL:=/bin/bash

PROJ_ROOT := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))/..
BUILD_DIR := ${PROJ_ROOT}/build
SRC_DIR := ${PROJ_ROOT}/src
SCRIPTS_DIR := ${SRC_DIR}/tools/scripts
CONF_DIR := ${SRC_DIR}/tools/config

MAKE := make --no-print-directory

# compiler settings
CC := k1om-mpss-linux-gcc
ifeq ("${CC}","icc")
  CC_SETUP := source ${SCRIPTS_DIR}/icc-setup.sh
  export CC := icc
  export CXX := icpc
else
#  CC_SETUP := source /opt/mpss/3.5.1/environment-setup-k1om-mpss-linux
  CC_SETUP := :
  export CC := gcc
  export CXX := g++
endif

# target settings
BUILD_MODES := Debug Release
# TARGET_ARCHS := x86_64 k1om
TARGET_ARCHS := x86_64

# misc.
NLCPU := ${shell nproc}

# misc
CPU_TOPOLOGY := ${SCRIPTS_DIR}/cpu-topology.py
CPU_TOPOLOGY_MAPS := \
	${SRC_DIR}/include/util/cpu_topology_host.h \
	${SRC_DIR}/include/util/cpu_topology_mic.h \
	${SCRIPTS_DIR}/topology/cpu_topology.py

# make targets
.PHONY: all
all: make

.PHONY: cmake
cmake:
	@for OUT in $(OUT_TYPES); do ( : ; \
		for APP in $(APP_LISTS); do ( : ; \
			mkdir -p ${DATA_DIR}/graph/$$OUT/$$APP; \
		); done; \
	); done
	@for ARCH in $(TARGET_ARCHS); do ( : ; \
		for MODE in $(BUILD_MODES); do ( : ; \
			mkdir -p ${BUILD_DIR}/$$MODE-$$ARCH; \
			cd ${BUILD_DIR}/$$MODE-$$ARCH; \
			${CC_SETUP}; \
			TARGET_ARCH=$$ARCH cmake ${SRC_DIR} \
				-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
				-DCMAKE_BUILD_TYPE=$$MODE; \
		); done; \
	); done
	exit

.PHONY: make
make: cpumaps
	@if [[ ! -e ${BUILD_DIR} ]]; then make cmake; fi
	@for ARCH in $(TARGET_ARCHS); do ( : ; \
		for MODE in $(BUILD_MODES); do ( : ; \
			echo "-----------------"; \
			echo $$ARCH "-" $$MODE; \
			cd ${BUILD_DIR}/$$MODE-$$ARCH; \
			${CC_SETUP}; \
			$(MAKE)  -j${NLCPU}; \
		); done; \
	); done
#$(MAKE)  #-j${NLCPU}; \

cpumaps: ${CPU_TOPOLOGY_MAPS}

${SRC_DIR}/include/util/cpu_topology_host.h:
	${CPU_TOPOLOGY} /proc/cpuinfo c > $@

${SRC_DIR}/include/util/cpu_topology_mic.h:
	${CPU_TOPOLOGY} ${CONF_DIR}/cpuinfo.mic c > $@

${SCRIPTS_DIR}/topology/cpu_topology.py:
	mkdir -p ${SCRIPTS_DIR}/topology
	${CPU_TOPOLOGY} /proc/cpuinfo python > $@

.PHONY: clean
clean:
	@for ARCH in $(TARGET_ARCHS); do ( : ; \
		for MODE in $(BUILD_MODES); do ( : ; \
			if [ -e ${BUILD_DIR}/$$MODE-$$ARCH/Makefile ]; \
			then \
				cd ${BUILD_DIR}/$$MODE-$$ARCH; \
				make clean; \
			fi; \
		); done; \
	); done

TAGS:
	etags `find . -type f -name '*.cc' -or -name '*.cpp'  -or -name '*.c' -or -name '*.h'`

.PHONY: cscope
cscope:
	find ${SRC_DIR} -name '*.cc'  \
	            -or -name '*.cpp' \
	            -or -name '*.c'   \
	            -or -name '*.h' > cscope.files
	cscope -b -q -R

.PHONY: distclean
distclean:
	rm -rf ${BUILD_DIR}
	rm -f  ${CPU_TOPOLOGY_MAPS}
	rm -f cscope.out tags

.PHONY: help
help:
	@cat README.md

.PHONY: format
format:
	./format.sh

