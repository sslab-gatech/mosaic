#!/usr/bin/env python3
import os
import sys

'''
/proc/cpuinfo
processor       : => kernel cpu id
core id         : => pcpu id
physical id     : => socket
cpu cores       : => number of cores in a socket
'''

def get_num_socket(cpuinfo):
    socket_id_set = set()
    for cpu in cpuinfo:
        socket_id_set.add(int(cpu["physical id"]))
    return len(socket_id_set)

def build_cpu_topology(cpuinfo_file, lang):
    cpuinfo = [dict(map(str.strip, line.split(":", 1))
                    for line in block.splitlines())
               for block in open(cpuinfo_file).read().split("\n\n")
               if len(block.strip())]
    topology_map = {}
    socket_id_set = set()
    physical_cpu_id_set = set()
    for cpu in cpuinfo:
        socket_id  = cpu["physical id"]
        socket_id_set.add( int(socket_id))
        physical_cpu_id = cpu["core id"]
        physical_cpu_id_set.add( int(physical_cpu_id))
        os_cpu_id = cpu["processor"]
        key = ":".join([socket_id, physical_cpu_id])
        topology_map.setdefault(key, []).append(os_cpu_id)
    if lang == "c":
        print_cpu_topology_in_c(cpuinfo, topology_map, socket_id_set,
                                physical_cpu_id_set)
    elif lang == "python":
        print_cpu_topology_in_python(cpuinfo, topology_map, socket_id_set,
                                     physical_cpu_id_set)
    else:
        print("Unknow language: %s\n" % lang)

def print_cpu_topology_in_c(cpuinfo, topology_map, socket_id_set, physical_cpu_id_set):
    num_socket = get_num_socket(cpuinfo)
    num_physical_cpu_per_socket = int(cpuinfo[0]["cpu cores"])
    smt_level = int(len(cpuinfo) / (num_socket * num_physical_cpu_per_socket))

    print("enum {")
    print("    NUM_SOCKET = %s," % num_socket)
    print("    NUM_PHYSICAL_CPU_PER_SOCKET = %s,"
          % num_physical_cpu_per_socket)
    print("    SMT_LEVEL = %s," % smt_level)
    print("};")
    print("")

    print("const int OS_CPU_ID[%s][%s][%s] = {"
          % ("NUM_SOCKET", "NUM_PHYSICAL_CPU_PER_SOCKET", "SMT_LEVEL"))
    for socket_id in sorted(socket_id_set):
        print("    { /* socket id: %s */" % socket_id)
        for physical_cpu_id in sorted(physical_cpu_id_set):
            key = ":".join([str(socket_id), str(physical_cpu_id)])
            print("        { /* physical cpu id: %s */" % physical_cpu_id)
            print("          ", end = "")
            for (smt_id, os_cpu_id) in enumerate(topology_map[key]):
                print("%s, " % os_cpu_id, end = "")
            print("    },") # physical cpu id
        print("    },") # socket id
    print("};")

def print_cpu_topology_in_python(cpuinfo, topology_map, socket_id_set, physical_cpu_id_set):
    num_socket = get_num_socket(cpuinfo)
    num_physical_cpu_per_socket = int(cpuinfo[0]["cpu cores"])
    smt_level = int(len(cpuinfo) / (num_socket * num_physical_cpu_per_socket))

    print("#!/usr/bin/env python3")
    print("")
    print("NUM_SOCKET = %s" % num_socket)
    print("NUM_PHYSICAL_CPU_PER_SOCKET = %s" % num_physical_cpu_per_socket)
    print("SMT_LEVEL = %s" % smt_level)
    print("")
    print("OS_CPU_ID = {} # socket_id, physical_cpu_id, smt_id")
    for socket_id in sorted(socket_id_set):
        for physical_cpu_id in sorted(physical_cpu_id_set):
            key = ":".join([str(socket_id), str(physical_cpu_id)])
            for (smt_id, os_cpu_id) in enumerate(topology_map[key]):
                print("OS_CPU_ID[%s,%s,%s] = %s" %
                      (socket_id, physical_cpu_id, smt_id, os_cpu_id))
    print("")

if __name__ == "__main__":
    cpuinfo_file = sys.argv[1]
    lang         = sys.argv[2]
    build_cpu_topology(cpuinfo_file, lang)

