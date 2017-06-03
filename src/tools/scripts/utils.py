#!/usr/bin/env python3
import os
import sys
import subprocess
import time
import glob
import grp
from shlex import split

import config_engine as conf

ROOT  = os.path.abspath(os.path.dirname(__file__))

class IostatLog(object):
    def __init__(self, outdir, log=False):
      self.__outdir = outdir
      self.__script = os.path.join(ROOT, "iobench_stat.sh")
      self.__log = log
      self.__sample = 0.1

    def __enter__(self):
      if (self.__log):
        print ("Logging ...")
        shargs = (split("%s --outdir %s --sample %d" % (self.__script,
                self.__outdir, self.__sample)))
        print (shargs)
        sh(shargs, out=None, shell=False)

    def __exit__(self, typ, value, traceback):
      if (self.__log):
        sh(split("%s --stop" % self.__script), out=None, shell=False)
        print ("Logging ... done")

class CollectlLog(object):
    def __init__(self, outdir, log=False):
      self.__outdir = outdir
      self.__log = log

    def __enter__(self):
      if (self.__log):
        print ("Logging ...")
        cmd = "collectl -i 0.1 > %s &" % (self.__outdir)
        print (cmd)
        os.system(cmd)

    def __exit__(self, typ, value, traceback):
      if (self.__log):
        sh(split("killall collectl"), out=None, shell=False)
        print ("Logging ... done")

def sh(cmd, out=None, shell=True):
    print(";; %s" % cmd)
    p = subprocess.Popen(cmd, shell=shell, stdout=out, stderr=out)
    p.wait()
    return p

def run_sshpass(debug, password, user, server, *args):
    assert(not any('"' in str(a) or ' ' in str(a) for a in args))

    sargs = ['%s' % str(a) for a in args]
    cmd = ["sshpass", "-p", password, "ssh", "%s@%s" % (user, server), "\""] + sargs + ["\""]
    if debug:
        return
    cmd_string = " ".join(cmd)
    print(cmd_string)
    os.system(cmd_string)

def run_background(debug, *args):
    assert(not any('"' in str(a) or ' ' in str(a) for a in args))

    sargs = ['"%s"' % str(a) for a in args]
    if debug:
        return
    cmd = " ".join(sargs) + ' &'
    print(cmd)
    os.system(cmd)

def run_background_output(debug, out_file, err_file, *args):
    assert(not any('"' in str(a) or ' ' in str(a) for a in args))

    sargs = ['"%s"' % str(a) for a in args]
    if debug:
        return
    cmd = " ".join(sargs) + ' > ' + out_file + ' 2> ' + err_file + ' &'
    print(cmd)
    os.system(cmd)

def run(debug, *args):
    assert(not any('"' in str(a) or ' ' in str(a) for a in args))

    sargs = ['"%s"' % str(a) for a in args]
    if debug:
        return
    cmd = " ".join(sargs)
    print(cmd)
    os.system(cmd)

def run_output(debug, out_file, *args):
    assert(not any('"' in str(a) or ' ' in str(a) for a in args))

    sargs = ['"%s"' % str(a) for a in args]
    if debug:
        return
    cmd = " ".join(sargs) + ' 2>&1 | tee ' + out_file
    print(cmd)
    os.system(cmd)

def mkdirp(pn, group=None):
    if not os.path.exists(pn):
        os.makedirs(pn)
        os.chmod(pn, 0o777)
        #if not group is None:
        #    gid = grp.getgrnam(group).gr_gid
        #    #os.chown(pn, -1, gid)

def populate_hash_dirs(num, root):
    if not os.path.exists(root):
        mkdirp(root)

    # create hashed directory
    #  - ${dir_path}/%03x [0, num_dir]
    for i in range(0, num):
        pn = os.path.join(root, "%03x" % i)
        mkdirp(pn)

def getVertexEngineLogName(opts):
    log_dir = os.path.join(conf.LOG, opts.dataset)
    mkdirp(log_dir, conf.FILE_GROUP)
    return os.path.join(log_dir, "vertex-engine-%d-%d-%s-%s.log" %
            (opts.run_on_mic,
                conf.SG_ALGORITHM_ENABLE_SELECTIVE_SCHEDULING[opts.algorithm],
                opts.max_iterations, opts.algorithm))

def list_mic_path():
    return glob.glob("/sys/class/mic/mic*")

def list_mic_name():
    return map(lambda m: m[len("/var/mpss/"):],
               glob.glob("/var/mpss/mic?"))

def __is_mic_ready(micX):
    print("/sys/class/mic/%s/virtblk_file" % micX)
    return os.path.exists("%s/virtblk_file" % micX)

def __wait_for_mic_init(micX):
    while ( not __is_mic_ready(micX) ):
        print(";; Waiting for %s initalization ..." % micX)
        time.sleep(1)
        continue
    print(";; %s initialized" % micX)

def __is_nvme_ready(nvme):
    return os.path.exists("/dev/%s" % nvme)

def __wait_for_nvme_init(nvme):
    while (not __is_nvme_ready(nvme) ):
        print(";; Waiting for %s initalization ..." % nvme)
        time.sleep(1)
        continue
    print(";; %s initialized" % nvme)

def set_virtblk():
    __wait_for_nvme_init("nvme0n1")
    for mic in list_mic_path():
        __wait_for_mic_init(mic)
        sh('sudo sh -c "echo /dev/nvme0n1 > %s/virtblk_file"' % mic)
