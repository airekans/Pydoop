import unittest
import pydoop
import os
import errno
import tempfile
import shutil
import time
from functools import partial


class Test(unittest.TestCase):

    def setUp(self):
        self.__epoll_loop = pydoop.EpollLoop()
        self.__select_loop = pydoop.SelectLoop()
        _cur_dir = os.path.dirname(__file__)
        self._data_path = os.path.join(_cur_dir, 'test_data')
 
    def tearDown(self):
        pass
    

    def testEventloopCtor(self):
        cls_obj = pydoop.EventLoop()
        self.assertTrue(cls_obj)


    def testEpollloopCtor(self):
        cls_obj = pydoop.EpollLoop()
        self.assertTrue(cls_obj)


    def testEpollloopAddEvent(self):
        rfd, _ = os.pipe()
        pydoop.set_nonblocking(rfd)
        on_read = lambda fd, ev, ev_loop: None
        self.__epoll_loop.add_event(rfd, pydoop.EventLoop.EV_IN, on_read)
        #self.assertTrue(self.EpollLoop.add_event('fd', 'event', 'cb'))

    def testEpollloopAddEventWithDifferentFds(self):
        rfd, wfd = os.pipe()
        pydoop.set_nonblocking(rfd)
        pydoop.set_nonblocking(wfd)
        on_read = lambda fd, ev, ev_loop: None
        on_write = lambda fd, ev, ev_loop: None
        self.__epoll_loop.add_event(rfd, pydoop.EventLoop.EV_IN, on_read)
        try:
            self.__epoll_loop.add_event(rfd, pydoop.EventLoop.EV_OUT, on_write)
        except:
            self.fail()

    def testEpollloopDispatchRead(self):
        rfd, wfd = os.pipe()
        pydoop.set_nonblocking(rfd)
        
        is_read_call = [False]
        def on_read(fd, event, ev_loop):
            self.assertEqual([pydoop.EventLoop.EV_IN], event)
            is_read_call[0] = True
            res = os.read(fd, 1)
            self.assertEqual(' ', res)
            ev_loop.stop_dispatch()

        self.__epoll_loop.add_event(rfd, pydoop.EventLoop.EV_IN, on_read)
        
        os.write(wfd, ' ')
        self.__epoll_loop.dispatch()
        self.assertTrue(is_read_call[0])


    def testEpollloopDispatchWrite(self):
        rfd, wfd = os.pipe()
        pydoop.set_nonblocking(rfd)
        pydoop.set_nonblocking(wfd)
        
        is_write_call = [False]
        expected = ' '
        def on_write(fd, event, ev_loop):
            self.assertEqual([pydoop.EventLoop.EV_OUT], event)
            is_write_call[0] = True
            os.write(fd, expected)
            ev_loop.stop_dispatch()

        self.__epoll_loop.add_event(wfd, pydoop.EventLoop.EV_OUT, on_write)
        
        self.__epoll_loop.dispatch()
        self.assertTrue(is_write_call[0])
        actual = os.read(rfd, 1)
        self.assertEqual(expected, actual)


    def testSelectloopCtor(self):
        cls_obj = pydoop.SelectLoop()
        self.assertTrue(cls_obj)


    def testSelectloopAddEvent(self):
        rfd, _ = os.pipe()
        pydoop.set_nonblocking(rfd)
        on_read = lambda fd, ev_loop: None
        self.__select_loop.add_event(rfd, pydoop.EventLoop.EV_IN, on_read)


    def testSelectloopDispatch(self):
        rfd, wfd = os.pipe()
        pydoop.set_nonblocking(rfd)
        
        is_read_call = [False]
        def on_read(fd, event, ev_loop):
            self.assertEqual(pydoop.EventLoop.EV_IN, event)
            is_read_call[0] = True
            res = os.read(fd, 1)
            self.assertEqual(' ', res)
            ev_loop.stop_dispatch()

        self.__select_loop.add_event(rfd, pydoop.EventLoop.EV_IN, on_read)

        os.write(wfd, ' ')
        self.__select_loop.dispatch()
        self.assertTrue(is_read_call[0])


    def testSelectloopDispatchWrite(self):
        rfd, wfd = os.pipe()
        pydoop.set_nonblocking(rfd)
        pydoop.set_nonblocking(wfd)
        
        is_write_call = [False]
        expected = ' '
        def on_write(fd, event, ev_loop):
            self.assertEqual(pydoop.EventLoop.EV_OUT, event)
            is_write_call[0] = True
            os.write(fd, expected)
            ev_loop.stop_dispatch()

        self.__select_loop.add_event(wfd, pydoop.EventLoop.EV_OUT, on_write)
        
        self.__select_loop.dispatch()
        self.assertTrue(is_write_call[0])
        actual = os.read(rfd, 1)
        self.assertEqual(expected, actual)

    def testPoolCtor(self):
        pool = pydoop.Pool(4)
        self.assertTrue(pool)


def assert_errno(func, error_num):
    try:
        func()
        assert False # should not reach here
    except OSError, e:
        assert e.errno == error_num
    except:
        import traceback
        traceback.print_exc()
        assert False

def assert_eof(fd):
    res = fd.read(1)
    assert len(res) == 0

cur_dir = os.path.dirname(__file__)
_data_path = os.path.join(cur_dir, 'test_data')

def testPoolRun():
    pool = pydoop.Pool(4)
    infd = open(os.path.join(_data_path, 'input.txt'))
    expected_lines = [l for l in infd]
    def func(l):
        assert l in expected_lines

    infd = open(os.path.join(_data_path, 'input.txt'))
    actual = pool.run(func, infd)
    assert len(expected_lines) == actual, '%d != %d' % (len(expected_lines), actual)
    assert_errno(partial(os.waitpid, 0, os.WNOHANG), errno.ECHILD)
    assert_eof(infd)

def testPoolRunWithWorkerFailure():
    pool = pydoop.Pool(4)
    infd = open(os.path.join(_data_path, 'input.txt'))
    expected_lines = [l for l in infd]
    def func(l):
        assert l in expected_lines
        if int(l.strip()[-1]) % 2 == 0:
            raise Exception

    infd = open(os.path.join(_data_path, 'input.txt'))
    actual = pool.run(func, infd)
    assert len(expected_lines) / 2 == actual
    assert_errno(partial(os.waitpid, 0, os.WNOHANG), errno.ECHILD)
    assert_eof(infd)

def testPoolRunWithForkFailure():
    pool = pydoop.Pool(4)
    infd = open(os.path.join(_data_path, 'input.txt'))
    expected_lines = [l for l in infd]
    def func(l):
        assert l in expected_lines

    infd = open(os.path.join(_data_path, 'input.txt'))
    fork_cnt = [0]
    def test_fork(real_fork):
        fork_cnt[0] += 1
        if fork_cnt[0] % 2 == 0:
            raise OSError
        else:
            return real_fork()
        
    old_fork = os.fork
    try:
        os.fork = partial(test_fork, old_fork)
        actual = pool.run(func, infd)
    finally:
        os.fork = old_fork

    assert len(expected_lines) == actual
    assert_errno(partial(os.waitpid, 0, os.WNOHANG), errno.ECHILD)
    assert_eof(infd)

def testPoolRunWithLogPrefix():
    infd = open(os.path.join(_data_path, 'input.txt'))
    tmp_work_dir = tempfile.mkdtemp(dir=os.getcwd())
    assert os.path.isdir(tmp_work_dir)
    
    try:
        pool = pydoop.Pool(4)
        expected_lines = [l for l in infd]
        def func(l):
            assert os.path.isdir(tmp_work_dir)
            assert l in expected_lines
            print l
    
        infd = open(os.path.join(_data_path, 'input.txt'))
        file_prefix = 'test_child_'
        log_file_prefix = os.path.join(tmp_work_dir, file_prefix)

        actual = pool.run(func, infd, log_file_prefix)
        assert len(expected_lines) == actual, '%d != %d' % (len(expected_lines), actual)
        assert_eof(infd)
        
        # assert the log file is created.
        log_file_count = 0
        for f in os.listdir(tmp_work_dir):
            if f.startswith(file_prefix):
                log_file_count += 1
        assert log_file_count == 4
    finally:
        shutil.rmtree(tmp_work_dir)

def testPoolRunWithTimeout():
    pool = pydoop.Pool(4, timeout=1)
    infd = open(os.path.join(_data_path, 'input.txt'))
    expected_lines = [l for l in infd]
    def func(l):
        assert l in expected_lines
        if int(l.strip()[-1]) % 2 == 0:
            time.sleep(2)

    infd = open(os.path.join(_data_path, 'input.txt'))
    actual = pool.run(func, infd)
    assert len(expected_lines) / 2 == actual, actual
    assert_errno(partial(os.waitpid, 0, os.WNOHANG), errno.ECHILD)
    assert_eof(infd)
    
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    testPoolRun()
    testPoolRunWithLogPrefix()
    testPoolRunWithWorkerFailure()
    testPoolRunWithForkFailure()
    testPoolRunWithTimeout()
    unittest.main()

