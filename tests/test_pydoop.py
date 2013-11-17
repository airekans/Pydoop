import unittest
import pydoop
import os
from functools import partial


class Test(unittest.TestCase):

    def setUp(self):
        self.__epoll_loop = pydoop.EpollLoop()
        self.__select_loop = pydoop.SelectLoop()
 
    def tearDown(self):
        pass
    

    def testEventloopCtor(self):
        cls_obj = pydoop.EventLoop()
        self.assertTrue(cls_obj)


    def testEpollloopCtor(self):
        cls_obj = pydoop.EpollLoop()
        self.assertTrue(cls_obj)


    def testEpollloopAddEvent(self):
        rfd, wfd = os.pipe()
        pydoop.set_nonblocking(rfd)
        on_read = lambda fd, ev, ev_loop: None
        self.__epoll_loop.add_event(rfd, pydoop.EventLoop.EV_IN, on_read)
        #self.assertTrue(self.EpollLoop.add_event('fd', 'event', 'cb'))

    def testEpollloopAddEventWithDifferentFds(self):
        rfd, wfd = os.pipe()
        pydoop.set_nonblocking(rfd)
        on_read = lambda fd, ev, ev_loop: None
        on_write = lambda fd, ev, ev_loop: None
        self.__epoll_loop.add_event(rfd, pydoop.EventLoop.EV_IN, on_read)
        self.assertRaises(IOError, partial(self.__epoll_loop.add_event,
                                           rfd, pydoop.EventLoop.EV_OUT, on_write))

    def testEpollloopDispatchRead(self):
        rfd, wfd = os.pipe()
        pydoop.set_nonblocking(rfd)
        
        is_read_call = [False]
        def on_read(fd, event, ev_loop):
            self.assertEqual(pydoop.EventLoop.EV_IN, event)
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
            self.assertEqual(pydoop.EventLoop.EV_OUT, event)
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
        rfd, wfd = os.pipe()
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


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()

