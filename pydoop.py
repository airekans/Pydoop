#!/usr/bin/env python
# encoding: utf-8
'''
pydoop -- a simple concurrent job execution library
'''

import sys
import os
from optparse import OptionParser
try:
    from importlib import import_module as import_mod
except:
    import_mod = __import__


def import_func(mod_file, func_name):
    mod_name = os.path.basename(mod_file).split('.')
    mod_name = mod_name[0]

    try:
        mod = import_mod(mod_name)
    except ImportError:
        print 'cannot import module', mod_name
        return None
    
    try:
        entry_func = mod.__dict__.get(func_name)
    except TypeError:
        return None
    
    return entry_func


def child_main(entry_func, rpipe):
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
        parser = OptionParser(usage = "%prog [options] JOBFILE INFILE")
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
    
    if len(args) < 2:
        parser.error('Please input JOBFILE and INFILE')
    
    worker_num = opts.worker_num
    job_file = args[0]
    in_file = args[1]
    
    # MAIN BODY #
    # first try to import the specified function
    if opts.func:
        child_entry_func = import_func(job_file, opts.func)
        if child_entry_func is None:
            parser.error('Cannot import function' + opts.func)
    
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
                    sys.exit(child_main(child_entry_func, rpipe))
                except:
                    sys.exit(1)
            else:
                os.close(rfd)
                wpipe = os.fdopen(wfd, 'w')
                child_pids.append((pid, wpipe))
        else:
            pass
    
    with open(in_file) as infd:
        child_doings = {}
        child_num = len(child_pids)
        for i, line in enumerate(infd):
            pid, wpipe = child_pids[i % child_num]
            wpipe.write(str(i) + ' ' + line)
            if pid not in child_doings:
                child_doings[pid] = []
            child_doings[pid].append(i)
        
        for _, wpipe in child_pids:
            wpipe.close()
        
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
