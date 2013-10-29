#!/usr/bin/env python
# encoding: utf-8
'''
pydoop -- a simple concurrent job execution library
'''

import sys
import os
from optparse import OptionParser

def child_job(job_file, opts):
    print job_file


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
    for _i in xrange(worker_num):
        try:
            pid = os.fork()
        except OSError, e:
            print >> sys.stderr, e.strerror
            sys.exit(1)
        
        if pid == 0:
            child_job(args[0], opts)
            sys.exit(0)
        
    for _i in xrange(worker_num):
        pid, exit_status = os.wait()
        print 'child %d exit' % (pid),
        if os.WIFEXITED(exit_status):
            print 'normally'
        else:
            print 'imnormally'
    
    print 'All children have been exited'


if __name__ == "__main__":
    sys.exit(main())