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


class _StopDispatch(Exception):
    pass

class EventLoop(object):
    EV_IN, EV_OUT = 1, 2
    
    def __init__(self):
        self.__on_exit = None
    
    def add_event(self, fd, events, cb):
        raise NotImplementedError()
    
    def del_event(self, fd):
        raise NotImplementedError()
    
    def dispatch(self):
        try:
            self._do_dispatch()
        finally:
            if self.__on_exit:
                self.__on_exit()
    
    def _do_dispatch(self):
        raise NotImplementedError()
    
    def stop_dispatch(self):
        raise _StopDispatch()
    
    def set_on_exit_cb(self, cb):
        self.__on_exit = cb


class SelectLoop(EventLoop):
    def __init__(self):
        EventLoop.__init__(self)
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
    
    def _do_dispatch(self):
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
            EventLoop.__init__(self)
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
        
        def _do_dispatch(self):
            while True:
                events = self.__epoll.poll()
                event = []
                try:
                    for fileno, ep_event in events:
                        if ep_event & select.EPOLLIN:
                            event.append(EventLoop.EV_IN)
                        if ep_event & select.EPOLLOUT:
                            event.append(EventLoop.EV_OUT)
                        #event = self.__rev_ev_map[ep_event]
                        self.__fd_cbs[fileno][1](fileno, event, self)
                except _StopDispatch:
                    break
    
    _event_loop = EpollLoop()
else:
    _event_loop = SelectLoop()
    

class FdBuffer(object):
    
    def __init__(self):
        self.__buffer = ""
    
    def set_content(self, content):
        self.__buffer = content
    
    def content(self):
        return self.__buffer
    
    def skip(self, n):
        self.__buffer = self.__buffer[n:]
    
    def empty(self):
        return len(self.__buffer) == 0
    

def close_all_fds(fds):
    for fd in fds:
        try:
            os.close(fd)
        except OSError:
            pass


def child_main(entry_func, rpipe):
    line = rpipe.readline()
    while line:
        entry_func(line)
        line = rpipe.readline()

    rpipe.close()
    return 0

class Pool(object):
    
    def __init__(self, worker_num=4):
        self.__worker_num = worker_num
    
    def run(self, proc_func, infd):
        self.__finished_children_num = 0
        self.__infd = infd
        child_pids = []
        for _i in xrange(self.__worker_num):
            try:
                data_rfd, data_wfd = os.pipe()
                life_rfd, life_wfd = os.pipe()
                pid = os.fork()
            except OSError, e:
                print >> sys.stderr, e.strerror
                break
            
            if pid == 0:
                os.close(data_wfd)
                os.close(life_rfd)
                for _, fds in child_pids:
                    for fd in fds:
                        os.close(fd)
                
                rpipe = os.fdopen(data_rfd, 'r')
                try:
                    sys.exit(child_main(proc_func, rpipe))
                except:
                    sys.exit(1)
            else:
                os.close(data_rfd)
                os.close(life_wfd)
                set_nonblocking(data_wfd)
                set_nonblocking(life_rfd)
                write_cb = partial(self.write_child_pipe, fd_buf=FdBuffer())
                _event_loop.add_event(data_wfd, EventLoop.EV_OUT, write_cb)
                _event_loop.add_event(life_rfd, EventLoop.EV_IN,
                                      self.read_life_signal)
                child_pids.append((pid, (data_wfd, life_rfd)))
        
        _event_loop.set_on_exit_cb(partial(close_all_fds, 
                                           [fds[0] for _, fds in child_pids]))

        try:
            _event_loop.dispatch()
        except KeyboardInterrupt:
            print 'User requests exit.'
        
        print 'All children have been exited'

    def read_life_signal(self, fd, _, ev_loop):
        try:
            os.read(fd, 1)
        except:
            pass
    
        os.close(fd)
        pid, exit_status = os.wait()
        print 'child %d exit' % (pid),
        if os.WIFEXITED(exit_status):
            print 'normally'
        else:
            print 'imnormally'
    
        self.__finished_children_num += 1
        if self.__finished_children_num == self.__worker_num:
            ev_loop.stop_dispatch()
        
    def write_child_pipe(self, fd, _, ev_loop, fd_buf):
        if fd_buf.empty():
            try:
                line = self.__infd.readline()
            except ValueError:
                line = None
    
            if line:
                fd_buf.set_content(line)
            else:
                os.close(fd)
                
        while not fd_buf.empty():
            try:
                n = os.write(fd, fd_buf.content())
                fd_buf.skip(n)
            except OSError, e:
                if e.errno == errno.EPIPE: # child has closed the pipe
                    ev_loop.del_event(fd)
                    os.close(fd)
                elif e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
                    return
    
        
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
                          default="main",
                          help="entry function in the JOBFILE, default to main")
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
    
    pool = Pool(worker_num)
    pool.run(child_entry_func, infd)

if __name__ == "__main__":
    sys.exit(main())
