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
import errno
import operator
import signal
import logging
import time
try:
    from importlib import import_module as import_mod
except:
    import_mod = __import__

try:
    from functools import partial
except: # functools is introduced in 2.5, so we have to define a partial for 2.4
    def partial(func, *args):
        return lambda *more_args, **kws: func(*(args + more_args), **kws)

logging.basicConfig(level=logging.DEBUG)

def set_nonblocking(fd):
    val = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, val | os.O_NONBLOCK)

def import_func(mod_file, func_name):
    mod_name = os.path.basename(mod_file).split('.')
    mod_name = mod_name[0]

    try:
        mod = import_mod(mod_name)
    except ImportError:
        print >> sys.stderr, 'cannot import module', mod_name
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
            logging.debug('event loop ended')
    
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
                loop_evs = []
                try:
                    for fileno, ep_event in events:
                        if ep_event & select.EPOLLIN:
                            loop_evs.append(EventLoop.EV_IN)
                        if ep_event & select.EPOLLOUT:
                            loop_evs.append(EventLoop.EV_OUT)

                        self.__fd_cbs[fileno][1](fileno, loop_evs, self)
                except _StopDispatch:
                    break
    
    _EventLoop = EpollLoop
else:
    _EventLoop = SelectLoop
    

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
            logging.debug('close_all_fds: close %d', fd)
            os.close(fd)
        except OSError:
            pass


class Process(object):
    
    def __init__(self, func, log_file_prefix=None):
        self.__func = func
        self.__pid = -1
        self.__log_file_prefix = log_file_prefix
    
    def __close_fd(self, fd):
        try:
            logging.debug('Process.__close_fd: close %d', fd)
            os.close(fd)
        except:
            pass
    
    def __redirect_output(self, filename):
        try:
            out_file = open(filename, 'a+b')
        except IOError:
            return False
        
        try:
            out_fd = out_file.fileno()
            os.dup2(out_fd, 1)
            os.dup2(out_fd, 2)
            return True
        except (IOError, OSError):
            return False
        finally:
            out_file.close()
        
    def get_pid(self):
        return self.__pid
        
    def start(self, close_fds=[]):
        pipe_fds = []
        try:
            data_rfd, data_wfd = os.pipe()
            pipe_fds += [data_rfd, data_wfd]
            life_rfd, life_wfd = os.pipe()
            pipe_fds += [life_rfd, life_wfd]
            pid = os.fork()
        except OSError, e:
            logging.warning('Process fork error: %s', e.strerror)
            for fd in pipe_fds:
                self.__close_fd(fd)
            return 0, -1, -1
        
        if pid == 0: # child
            self.__close_fd(data_wfd)
            self.__close_fd(life_rfd)
            for fd in close_fds:
                self.__close_fd(fd)
            
            if self.__log_file_prefix:
                real_pid = os.getpid()
                log_filename = self.__log_file_prefix + str(real_pid)
                if self.__redirect_output(log_filename):
                    logging.debug('redirect stdout and stderr to ' + 
                                  log_filename)
                else:
                    logging.warning('failed to redirect stdout/err to ' +
                                    log_filename)
                
            try:
                ret = self.__func(data_rfd, life_wfd)
            except:
                import traceback
                traceback.print_exc()
                os._exit(1)
                
            os._exit(ret)
        else: # parent
            logging.debug('Process.run: parent close %d %d', data_rfd, life_wfd)
            os.close(data_rfd)
            os.close(life_wfd)
            self.__pid = pid
            return pid, data_wfd, life_rfd
        
    def join(self, options=0):
        if self.__pid == -1:
            return
        
        return os.waitpid(self.__pid, options)
    
    def kill(self, signum):
        if self.__pid == -1:
            return
        
        try:
            os.kill(self.__pid, signum)
        except:
            return

class _TimeoutException(Exception):
    pass

def _alarm_handler(signum, frame):
    raise _TimeoutException()

class _WorkerProcess(Process):
    
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
        
        self.assigned_task_num = 0
        self.success_task_num = 0
        self.timeout_task_num = 0
        self.failed_task_num = 0
    
    def get_finished_task_num(self):
        return self.success_task_num + self.failed_task_num + \
            self.timeout_task_num


class Pool(object):
    
    TIMEOUT_FLAG = 't'
    SUCCESS_FLAG = '2'
    FAILED_FLAG = 'f'
    
    def __init__(self, worker_num=4, timeout=0):
        self.__worker_num = worker_num
        try:
            self.__timeout = int(timeout)
            if self.__timeout < 0:
                self.__timeout = 0
        except:
            self.__timeout = 0

    @staticmethod
    def __child_main(work_func, timeout, data_rfd, life_wfd):
        if timeout > 0:
            signal.signal(signal.SIGALRM, _alarm_handler)
            def loop(rp, wp):
                line = rp.readline()
                while line:
                    task_result = Pool.SUCCESS_FLAG
                    try:
                        signal.alarm(timeout)
                        work_func(line)
                    except _TimeoutException:
                        task_result = Pool.TIMEOUT_FLAG
                    except:
                        task_result = Pool.FAILED_FLAG
                    finally:
                        signal.alarm(0)
                    
                    wp.write(task_result)
                    if task_result != Pool.SUCCESS_FLAG:
                        msg = 'Pool.__child_main: elem: %s flag: %s' % \
                            (line, task_result)
                        logging.warning(msg)
                        
                    line = rp.readline()
        else:
            def loop(rp, wp):
                line = rp.readline()
                while line:
                    task_finished = True
                    try:
                        work_func(line)
                    except:
                        task_finished = False
                    
                    if task_finished:
                        wp.write(Pool.SUCCESS_FLAG)
                    else:
                        wp.write(Pool.FAILED_FLAG)
                        msg = 'Pool.__child_main: failed elem: %s' % \
                            line
                        logging.warning(msg)

                    line = rp.readline()
        
        rpipe = os.fdopen(data_rfd, 'r')
        wpipe = os.fdopen(life_wfd, 'w')
        try:
            loop(rpipe, wpipe)
        finally:
            logging.debug('Pool.__child_main: exit and close %d %d', 
                          wpipe.fileno(), rpipe.fileno())
            wpipe.close()
            rpipe.close()
    
        return 0
    
    def run(self, proc_func, elems, log_file_prefix=None):
        self.__finished_children_num = 0
        self.__event_loop = _EventLoop()
        self.__total_task_num = -1
        if not hasattr(elems, 'next'):
            # assume it's a container implementing __iter__ method
            self.__elems = iter(elems)
            self.__total_task_num = len(elems)
        else:
            self.__elems = elems
        self.__fd_proc = {}
        self.__children = []
        close_fds = []

        for _i in xrange(self.__worker_num):
            child_proc = _WorkerProcess(partial(Pool.__child_main, proc_func,
                                                self.__timeout),
                                        log_file_prefix)
            pid, data_wfd, life_rfd = \
                child_proc.start([fd for fd in close_fds])
                
            if pid == 0:
                print >> sys.stderr, 'failed to create child process'
                continue
            else:
                self.__children.append(child_proc)
                
                set_nonblocking(data_wfd)
                set_nonblocking(life_rfd)
                self.__event_loop.add_event(data_wfd, EventLoop.EV_OUT, 
                                            self.write_child_pipe)
                self.__event_loop.add_event(life_rfd, EventLoop.EV_IN,
                                            self.read_task_report)

                close_fds += [data_wfd, life_rfd]
                self.__fd_proc[life_rfd] = child_proc
                
                child_proc.wfd_buf = FdBuffer()
                self.__fd_proc[data_wfd] = child_proc
                
        
        self.__event_loop.set_on_exit_cb(partial(close_all_fds, 
                                           [fd for fd in close_fds]))

        try:
            self.__event_loop.dispatch()
        except KeyboardInterrupt:
            print 'User requests exit.'

            for child_proc in self.__children:
                try:
                    pid, _ = child_proc.join(os.WNOHANG)
                    if pid == 0:
                        child_proc.kill(signal.SIGTERM)
                except OSError:
                    continue
            
            time.sleep(1)
            for child_proc in self.__children:
                try:
                    child_proc.join(os.WNOHANG)
                except OSError:
                    continue

        return reduce(operator.add, 
                      [p.success_task_num for p in self.__children], 0)

    def read_task_report(self, fd, _, ev_loop):
        child_proc = self.__fd_proc[fd]
        is_child_end = False

        try:
            c = os.read(fd, 1)
            if len(c) > 0:
                if c == Pool.SUCCESS_FLAG:
                    child_proc.success_task_num += 1
                elif c == Pool.TIMEOUT_FLAG:
                    child_proc.timeout_task_num += 1
                elif c == Pool.FAILED_FLAG:
                    child_proc.failed_task_num += 1
                else: # if recv something else, consider it failed
                    child_proc.failed_task_num += 1
            else:
                is_child_end = True
        except OSError, e:
            if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
                logging.debug('Pool.read_task_report: read error %d', e.errno)
                return
            else:
                assert False, 'unexpected errno: %d' % e.errno
                is_child_end = True
        except:
            is_child_end = True
            
        if is_child_end:
            try:
                pid, exit_status = child_proc.join(os.WNOHANG)
            except OSError:
                return # not exit yet
            
            if pid == 0: # not exit yet
                return
            
            finished_task_num = child_proc.get_finished_task_num()
            if finished_task_num < child_proc.assigned_task_num:
                # It means the child has exited too soon.
                # TODO: we may fork another child again.
                logging.warning(('child %d has exited prematurely. ' +
                                'assigned: %d finished: %d') %
                                    (child_proc.get_pid(),
                                     child_proc.assigned_task_num,
                                     finished_task_num))

            logging.debug('Pool.read_task_report: close %d', fd)
            ev_loop.del_event(fd)
            os.close(fd)
            assert pid == child_proc.get_pid()
            if os.WIFEXITED(exit_status) and os.WEXITSTATUS(exit_status) == 0:
                logging.info('child %d exit normally', pid)
            else:
                logging.info('child %d exit abnormally', pid)
        
            self.__finished_children_num += 1
            if self.__finished_children_num == len(self.__children):
                ev_loop.stop_dispatch()
        
    def write_child_pipe(self, fd, _, ev_loop):
        child_proc = self.__fd_proc[fd]
        fd_buf = child_proc.wfd_buf
        
        if fd_buf.empty():
            try:
                elem = self.__elems.next()
            except StopIteration:
                elem = None
    
            if elem is not None:
                if not isinstance(elem, str):
                    elem = str(elem) + '\n'
                fd_buf.set_content(elem)
                child_proc.assigned_task_num += 1
            else:
                ev_loop.del_event(fd)
                logging.debug('Pool.write_child_pipe: close %d', fd)
                os.close(fd)
                
        while not fd_buf.empty():
            try:
                n = os.write(fd, fd_buf.content())
                fd_buf.skip(n)
            except OSError, e:
                if e.errno == errno.EPIPE: # child has closed the pipe
                    ev_loop.del_event(fd)
                    logging.debug('Pool.write_child_pipe: close %d with EPIPE', fd)
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
        parser.add_option("-l", "--log-prefix", dest="log_prefix",
                          help="the log file prefix workers used to log")
        parser.add_option("-w", "--worker-num", dest="worker_num",
                          help="number of workers", metavar="NUM",
                          type="int", default=4)
        parser.add_option("-f", "--func", dest="func",
                          default="main",
                          help="entry function in the JOBFILE, default to main")
        parser.add_option("-a", "--args", dest="args",
                          help="additional arguments passed to the entry func")
    except Exception, e:
        indent = len(program_name) * " "
        sys.stderr.write(program_name + ": " + repr(e) + "\n")
        sys.stderr.write(indent + "  for help use --help")
        return 2

    (opts, args) = parser.parse_args(argv)
    
    if len(args) < 2:
        parser.error('Please input JOBFILE and INFILE')
    
    worker_num = opts.worker_num
    log_prefix = opts.log_prefix
    func_args = opts.args or []
    if func_args:
        func_args = func_args.split(',')

    job_file = args[0]
    in_file = args[1]
    
    # first try to import the specified function
    assert opts.func
    if opts.func:
        child_entry_func = import_func(job_file, opts.func)
        if child_entry_func is None:
            parser.error('Cannot import function ' + opts.func)
    
    if func_args:
        work_func = child_entry_func
        child_entry_func = \
            lambda *args, **kwargs: work_func(*(list(args) + func_args), **kwargs)

    try:
        infd = open(in_file)
    except:
        parser.error('Cannot open input file ' + in_file)
    
    pool = Pool(worker_num)
    pool.run(child_entry_func, infd, log_prefix)

if __name__ == "__main__":
    sys.exit(main())
