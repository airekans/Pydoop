#!/usr/bin/env python
# encoding: utf-8
'''
pydoop -- a simple concurrent job execution library
'''

import sys
import os
from optparse import OptionParser
import importlib


def child_main(job_file, func_name, rpipe):
    if not job_file:
        print 'job file cannot be empty'
        return 1
    
    mod_name = os.path.basename(job_file).split('.')
    mod_name = mod_name[0]

    try:
        mod = importlib.import_module(mod_name)
    except ImportError:
        print 'cannot import module', mod_name
        return 1
    
    try:
        entry_func = mod.__dict__.get(func_name)
    except TypeError:
        return 1
    
    if not entry_func:
        print 'cannot find function %s in module %s' % (func_name, mod_name)
        return 1
    
    for line in rpipe:
        entry_func(line)

    rpipe.close()
    return 0

def main(argv=None):
    '''Command line options.'''
    
    program_name = os.path.basename(sys.argv[0])
 
    if argv is None:
        argv = sys.argv[1:]
    try:
        # setup option parser
        parser = OptionParser(usage = "%prog [options] JOBFILE")
        parser.add_option("-i", "--in", dest="infile", 
                          help="set input path",
                          metavar="FILE")
        parser.add_option("-o", "--out", dest="outfile",
                          help="set output path", metavar="FILE")
        parser.add_option("-w", "--worker-num", dest="worker_num",
                          help="number of workers", metavar="NUM",
                          type="int", default=4)
        parser.add_option("-f", "--func", dest="func",
                          help="entry function in the JOBFILE")
    except Exception, e:
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

    (opts, args) = parser.parse_args(argv)
    
    if len(args) == 0:
        parser.error('Please input JOBFILE')
    
    worker_num = opts.worker_num
    
    # MAIN BODY #
    child_pids = []
    for _i in xrange(worker_num):
        if opts.func:
            try:
                rfd, wfd = os.pipe()
                pid = os.fork()
            except OSError, e:
                print >> sys.stderr, e.strerror
                break
            
            if pid == 0:
                os.close(wfd)
                rpipe = os.fdopen(rfd, 'r')
                try:
                    sys.exit(child_main(args[0], opts.func, rpipe))
                except:
                    sys.exit(1)
            else:
                os.close(rfd)
                child_pids.append((pid, wfd))
        else:
            pass
        
    for _ in child_pids:
        pid, exit_status = os.wait()
        print 'child %d exit' % (pid),
        if os.WIFEXITED(exit_status):
            print 'normally'
        else:
            print 'imnormally'
    
    print 'All children have been exited'


if __name__ == "__main__":
    sys.exit(main())