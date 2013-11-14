#!/usr/bin/env python
# encoding: utf-8
'''
pydoop -- a simple concurrent job execution library
'''

import sys
import os
from optparse import OptionParser
import select
import fcntl
try:
    from importlib import import_module as import_mod
except:
    import_mod = __import__


def set_nonblocking(fd):
    val = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, val | os.O_NONBLOCK)


class _StopDispatch(Exception):
    pass

class EventLoop(object):
    EV_IN, EV_OUT = 1, 2
    
    def add_event(self, fd, event, cb):
        pass
    
    def dispatch(self):
        pass
    
    def stop_dispatch(self):
        raise _StopDispatch()

class SelectLoop(EventLoop):
    def __init__(self):
        self.__rfds = []
        self.__wfds = []
        self.__rfd_cbs = {}
        self.__wfd_cbs = {}
    
    def add_event(self, fd, event, cb):
        if event == EventLoop.EV_IN:
            self.__rfds.append(fd)
            self.__rfd_cbs[fd] = cb
        elif event == EventLoop.EV_OUT:
            self.__wfds.append(fd)
            self.__wfd_cbs[fd] = cb
    
    def dispatch(self):
        rlist, wlist = self.__rfds, self.__wfds
        while True:
            rfds, wfds, _ = select.select(rlist, wlist, [])
            try:
                for rfd in rfds:
                    self.__rfd_cbs[rfd](rfd, self)
                    
                for wfd in wfds:
                    self.__wfd_cbs[wfd](wfd, self)
            except _StopDispatch:
                break

if 'epoll' in select.__dict__:
    class EpollLoop(EventLoop):
        def __init__(self):
            self.__epoll = select.epoll()
            self.__ev_map = {EventLoop.EV_IN: select.EPOLLIN,
                             EventLoop.EV_OUT: select.EPOLLOUT}
            self.__rev_ev_map = {select.EPOLLIN: EventLoop.EV_IN,
                                 select.EPOLLOUT: EventLoop.EV_OUT}
            self.__fd_cbs = {}
    
        def add_event(self, fd, event, cb):
            fd_no = fd
            if not isinstance(fd_no, int):
                fd_no = fd_no.fileno()
            
            if fd_no not in self.__fd_cbs:
                self.__fd_cbs[fd_no] = {}
            self.__fd_cbs[fd_no][event] = cb
            self.__epoll.register(fd_no, self.__ev_map[event])
        
        def dispatch(self):
            while True:
                events = self.__epoll.poll()
                try:
                    for fileno, ep_event in events:
                        event = self.__rev_ev_map[ep_event]
                        self.__fd_cbs[fileno][event](fileno, self)
                except _StopDispatch:
                    break
    
    _event_loop = EpollLoop()
else:
    _event_loop = SelectLoop()
    

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
    assert opts.func
    if opts.func:
        child_entry_func = import_func(job_file, opts.func)
        if child_entry_func is None:
            parser.error('Cannot import function' + opts.func)
    
    child_pids = []
    for _i in xrange(worker_num):
        if opts.func:
            try:
                data_rfd, data_wfd = os.pipe()
                pid = os.fork()
            except OSError, e:
                print >> sys.stderr, e.strerror
                break
            
            if pid == 0:
                os.close(data_wfd)
                rpipe = os.fdopen(data_rfd, 'r')
                try:
                    sys.exit(child_main(child_entry_func, rpipe))
                except:
                    sys.exit(1)
            else:
                os.close(data_rfd)
                wpipe = os.fdopen(data_wfd, 'w')
                child_pids.append((pid, wpipe))
        else:
            pass
    
    infd = open(in_file)
    try:
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
    finally:
        infd.close()
        
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
