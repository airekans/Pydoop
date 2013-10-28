#!/usr/bin/env python
# encoding: utf-8
'''
pydoop -- a simple concurrent job execution library
'''

import sys
import os
from optparse import OptionParser


def main(argv=None):
    '''Command line options.'''
    
    program_name = os.path.basename(sys.argv[0])
 
    if argv is None:
        argv = sys.argv[1:]
    try:
        # setup option parser
        parser = OptionParser()
        parser.add_option("-i", "--in", dest="infile", 
                          help="set input path",
                          metavar="FILE")
        parser.add_option("-o", "--out", dest="outfile",
                          help="set output path", metavar="FILE")
        parser.add_option("-w", "--worker-num", dest="worker_num",
                          help="number of workers", metavar="NUM",
                          default=4)
        
        # process options
        (opts, args) = parser.parse_args(argv)
        
        if opts.infile:
            print("infile = %s" % opts.infile)
        if opts.outfile:
            print("outfile = %s" % opts.outfile)
        
    except Exception, e:
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

    # MAIN BODY #

if __name__ == "__main__":
    sys.exit(main())