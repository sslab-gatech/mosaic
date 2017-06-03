###
# project setting
project (scalable-graph)
cmake_minimum_required (VERSION 2.8)
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

# Build the host-only version
SET(MOSAIC_HOST_ONLY TRUE)

###
# compiler options
# - set compiler-dependent options
IF(NOT $ENV{CC} MATCHES "icc")
  set(COMP_DEP_OPT "-ftree-vectorize -ftree-vectorizer-verbose=0")
ENDIF()

SET(CMAKE_CROSSCOMPILING 1)
SET(FORCE_UNSUPPORTED_COMPILER ON)
#SET(CMAKE_SYSROOT   /opt/mpss/3.5.1/sysroots/k1om-mpss-linux/)

# - set target-dependent options
IF($ENV{TARGET_ARCH} MATCHES "k1om")
  set(TARGET_OPT "-m64 --sysroot=/opt/mpss/3.5.1/sysroots/k1om-mpss-linux -DTARGET_ARCH_K1OM")
  set(CMAKE_C_COMPILER "k1om-mpss-linux-gcc")
  set(CMAKE_CXX_COMPILER "k1om-mpss-linx-g++")
ELSE()
  set(TARGET_OPT "-L/usr/lib64")
  set(CMAKE_C_COMPILER "gcc")
  set(CMAKE_CXX_COMPILER "g++")
ENDIF()

IF(MOSAIC_HOST_ONLY)
  string(CONCAT extra_define ${TARGET_OPT} " -DMOSAIC_HOST_ONLY")
  set(TARGET_OPT ${extra_define})
ENDIF()

# - set general options

#set(COMMON_FLAGS  "-march=native -mtune=native -fno-rtti ${TARGET_OPT} ${COMP_DEP_OPT}")
set(COMMON_FLAGS  "-fno-rtti ${TARGET_OPT} ${COMP_DEP_OPT}")
set(DEBUG_FLAGS   "-O0 -g -fno-omit-frame-pointer")
set(RELEASE_FLAGS "-O3")

set(CMAKE_C_FLAGS "${COMMON_FLAGS} ${DEBUG_FLAGS}")
set(CMAKE_C_FLAGS_RELEASE "${COMMON_FLAGS} ${RELEASE_FLAGS}")

set(CMAKE_CXX_FLAGS "-std=gnu++0x ${COMMON_FLAGS} ${DEBUG_FLAGS}")
set(CMAKE_CXX_FLAGS_RELEASE "-std=gnu++0x ${COMMON_FLAGS} ${RELEASE_FLAGS}")

# - set debug options
IF(CMAKE_BUILD_TYPE MATCHES Debug)
  # enables printing debug-information
  add_definitions(-DSCALABLE_GRAPHS_DEBUG)
  add_definitions(-DRING_BUFFER_DEBUG)
  # add_definitions(-DRING_BUFFER_TRACE_FINGERPRINT)
  # add_definitions(-DRING_BUFFER_TRACE_EVENT)
ENDIF()

# - enable for semi-production-level testing
# add_definitions(-DRING_BUFFER_CONF_NO_MMAP)
# add_definitions(-DRING_BUFFER_CONF_NO_DOUBLE_MMAP)

#IF($ENV{TARGET_ARCH} MATCHES "k1om")
#  add_definitions(-DRING_BUFFER_TRACE_EVENT)
#ENDIF()

#  add_definitions(-DRING_BUFFER_TRACE_EVENT)
add_definitions(-DPROF_TILE_INFO)
###
# - where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

set(SCALABLE_GRAPH_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/util/pci-ring-buffer/include)

include_directories(${SCALABLE_GRAPH_INCLUDE_DIR})

add_subdirectory(lib)
add_subdirectory(tools)
add_subdirectory(util)
add_subdirectory(unittest)
IF($ENV{TARGET_ARCH} MATCHES "k1om")
ELSE()
  add_subdirectory(test)
ENDIF()
IF(MOSAIC_HOST_ONLY)
ELSE()
add_subdirectory(test/rb)
ENDIF()
