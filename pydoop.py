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
from functools import partial
import errno
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
    
    def add_event(self, fd, events, cb):
        raise NotImplementedError()
    
    def del_event(self, fd):
        raise NotImplementedError()
    
    def dispatch(self):
        raise NotImplementedError()
    
    def stop_dispatch(self):
        raise _StopDispatch()

class SelectLoop(EventLoop):
    def __init__(self):
        self.__rfds = []
        self.__wfds = []
        self.__rfd_cbs = {}
        self.__wfd_cbs = {}
    
    def add_event(self, fd, events, cb):
        if not isinstance(events, (list, set)):
            events = [events]

        if EventLoop.EV_IN in events and fd not in self.__rfd_cbs:
                self.__rfds.append(fd)
                self.__rfd_cbs[fd] = cb
        if EventLoop.EV_OUT in events and fd not in self.__wfd_cbs:
                self.__wfds.append(fd)
                self.__wfd_cbs[fd] = cb
    
    def del_event(self, fd):
        try:
            self.__rfds.remove(fd)
            del self.__rfd_cbs[fd]
        except ValueError:
            pass

        try:
            self.__wfds.remove(fd)
            del self.__wfd_cbs[fd]
        except ValueError:
            pass
    
    def dispatch(self):
        while True:
            rfds, wfds, _ = select.select(self.__rfds, self.__wfds, [])
            try:
                for rfd in rfds:
                    self.__rfd_cbs[rfd](rfd, EventLoop.EV_IN, self)
                    
                for wfd in wfds:
                    self.__wfd_cbs[wfd](wfd, EventLoop.EV_OUT, self)
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
        
        def __get_fileno(self, fd):
            fd_no = fd
            if not isinstance(fd_no, int):
                fd_no = fd_no.fileno()
            return fd_no
    
        def add_event(self, fd, events, cb):
            if not isinstance(events, (list, set)):
                events = [events]

            fd_no = self.__get_fileno(fd)
            if fd_no in self.__fd_cbs:
                return
            
            ep_event = 0
            for ev in events:
                if ev in self.__ev_map:
                    ep_event = ep_event | self.__ev_map[ev]
            
            self.__fd_cbs[fd_no] = (events, cb)
            self.__epoll.register(fd_no, ep_event)
        
        def del_event(self, fd):
            fd_no = self.__get_fileno(fd)
            if fd_no in self.__fd_cbs:
                del self.__fd_cbs[fd_no]
            self.__epoll.unregister(fd_no)
        
        def dispatch(self):
            while True:
                events = self.__epoll.poll()
                try:
                    for fileno, ep_event in events:
                        event = self.__rev_ev_map[ep_event]
                        self.__fd_cbs[fileno][1](fileno, event, self)
                except _StopDispatch:
                    break
    
    _event_loop = EpollLoop()
else:
    _event_loop = SelectLoop()
    

class FdBuffer(object):
    
    def __init__(self):
        self.__buffer = ""
        self.__is_eof = False
        
    def read_from(self, fd):
        while True:
            tmp_buf = os.read(fd, 1024)
            if len(tmp_buf) == 0:
                self.__is_eof = True
                return
            
            self.__buffer += tmp_buf
            i = tmp_buf.find('\n')
            if i >= 0:
                return
    
    def eof(self):
        return self.__is_eof
    
    def has_line(self):
        return not self.empty() and (self.__is_eof or self.__buffer.find('\n') >= 0)
    
    def next_line(self):
        i = self.__buffer.find('\n')
        if self.__is_eof and i < 0:
            line = self.__buffer
            self.__buffer = ""
            return line
        else:
            assert i >= 0
            line = self.__buffer[:i + 1]
            self.__buffer = self.__buffer[i + 1:]
            return line
    
    def set_content(self, content):
        self.__buffer = content
    
    def content(self):
        return self.__buffer
    
    def skip(self, n):
        self.__buffer = self.__buffer[n:]
    
    def empty(self):
        return len(self.__buffer) == 0
    
    def len(self):
        return len(self.__buffer)

    
finished_children_num = 0
children_num = 0
def write_child_pipe(fd, _, ev_loop, buf, rfd, fd_buf):
    global finished_children_num
    if fd_buf.empty():
        if buf.has_line():
            fd_buf.set_content(buf.next_line())
        elif buf.eof():
            os.close(fd)
            finished_children_num += 1
            if finished_children_num == children_num:
                ev_loop.stop_dispatch()
        else:
            buf.read_from(rfd)
            fd_buf.set_content(buf.next_line())
    
    while fd_buf.len() > 0:
        try:
            n = os.write(fd, fd_buf.content())
            fd_buf.skip(n)
        except OSError, e:
            if e.errno == errno.EPIPE: # child has closed the pipe
                ev_loop.del_event(fd)
            elif e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
                return


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
    line = rpipe.readline()
    while line:
        entry_func(line)
        line = rpipe.readline()

    rpipe.close()
    return 0

def main(argv=None):
    '''Command line options.'''
    
    global children_num
    
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
            parser.error('Cannot import function ' + opts.func)

    try:
        infd = open(in_file)
    except:
        parser.error('Cannot open input file ' + in_file)
    
    infd_no = infd.fileno()
    infd_buf = FdBuffer()
    
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
                for _, wfd in child_pids:
                    os.close(wfd)
                
                rpipe = os.fdopen(data_rfd, 'r')
                try:
                    sys.exit(child_main(child_entry_func, rpipe))
                except:
                    sys.exit(1)
            else:
                os.close(data_rfd)
                set_nonblocking(data_wfd)
                write_cb = partial(write_child_pipe, buf=infd_buf,
                                   rfd=infd_no, fd_buf=FdBuffer())
                _event_loop.add_event(data_wfd, EventLoop.EV_OUT, write_cb)
                children_num += 1
                child_pids.append((pid, data_wfd))
        else:
            pass
    
    _event_loop.dispatch()
        
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
