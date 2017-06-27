"""Tests for streams.py."""

import os
import queue
import socket
import sys
import threading
import unittest
from unittest import mock
try:
    import ssl
except ImportError:
    ssl = None

import asyncio
from asyncio import test_utils


DATA = b'line1\nline2\nline3\n'


def test_invalid_limit(self):
    with self.assertRaisesRegex(ValueError, 'imit'):
        asyncio.StreamReader(limit=0, loop=self.loop)

    with self.assertRaisesRegex(ValueError, 'imit'):
        asyncio.StreamReader(limit=-1, loop=self.loop)

def test_readuntil_limit_found_sep(self):
    stream = asyncio.StreamReader(loop=self.loop, limit=3)
    stream.feed_data(b'some dataAA')

    with self.assertRaisesRegex(asyncio.LimitOverrunError,
                                'not found') as cm:
        self.loop.run_until_complete(stream.readuntil(b'AAA'))

    self.assertEqual(b'some dataAA', stream._buffer)

    stream.feed_data(b'A')
    with self.assertRaisesRegex(asyncio.LimitOverrunError,
                                'is found') as cm:
        self.loop.run_until_complete(stream.readuntil(b'AAA'))

    self.assertEqual(b'some dataAAA', stream._buffer)

def test_readexactly_zero_or_less(self):
    # Read exact number of bytes (zero or less).
    stream = asyncio.StreamReader(loop=self.loop)
    stream.feed_data(self.DATA)

    data = self.loop.run_until_complete(stream.readexactly(0))
    self.assertEqual(b'', data)
    self.assertEqual(self.DATA, stream._buffer)

    with self.assertRaisesRegex(ValueError, 'less than zero'):
        self.loop.run_until_complete(stream.readexactly(-1))
    self.assertEqual(self.DATA, stream._buffer)

def test_readexactly(self):
    # Read exact number of bytes.
    stream = asyncio.StreamReader(loop=self.loop)

    n = 2 * len(self.DATA)
    read_task = asyncio.Task(stream.readexactly(n), loop=self.loop)

    def cb():
        stream.feed_data(self.DATA)
        stream.feed_data(self.DATA)
        stream.feed_data(self.DATA)
    self.loop.call_soon(cb)

    data = self.loop.run_until_complete(read_task)
    self.assertEqual(self.DATA + self.DATA, data)
    self.assertEqual(self.DATA, stream._buffer)

def test_readexactly_limit(self):
    stream = asyncio.StreamReader(limit=3, loop=self.loop)
    stream.feed_data(b'chunk')
    data = self.loop.run_until_complete(stream.readexactly(5))
    self.assertEqual(b'chunk', data)
    self.assertEqual(b'', stream._buffer)

def test_readexactly_eof(self):
    # Read exact number of bytes (eof).
    stream = asyncio.StreamReader(loop=self.loop)
    n = 2 * len(self.DATA)
    read_task = asyncio.Task(stream.readexactly(n), loop=self.loop)

    def cb():
        stream.feed_data(self.DATA)
        stream.feed_eof()
    self.loop.call_soon(cb)

    with self.assertRaises(asyncio.IncompleteReadError) as cm:
        self.loop.run_until_complete(read_task)
    self.assertEqual(cm.exception.partial, self.DATA)
    self.assertEqual(cm.exception.expected, n)
    self.assertEqual(str(cm.exception),
                     '18 bytes read on a total of 36 expected bytes')
    self.assertEqual(b'', stream._buffer)

def test_readexactly_exception(self):
    stream = asyncio.StreamReader(loop=self.loop)
    stream.feed_data(b'line\n')

    data = self.loop.run_until_complete(stream.readexactly(2))
    self.assertEqual(b'li', data)

    stream.set_exception(ValueError())
    self.assertRaises(
        ValueError, self.loop.run_until_complete, stream.readexactly(2))

def test_exception(self):
    stream = asyncio.StreamReader(loop=self.loop)
    self.assertIsNone(stream.exception())

    exc = ValueError()
    stream.set_exception(exc)
    self.assertIs(stream.exception(), exc)

def test_exception_waiter(self):
    stream = asyncio.StreamReader(loop=self.loop)

    @asyncio.coroutine
    def set_err():
        stream.set_exception(ValueError())

    t1 = asyncio.Task(stream.readline(), loop=self.loop)
    t2 = asyncio.Task(set_err(), loop=self.loop)

    self.loop.run_until_complete(asyncio.wait([t1, t2], loop=self.loop))

    self.assertRaises(ValueError, t1.result)

def test_exception_cancel(self):
    stream = asyncio.StreamReader(loop=self.loop)

    t = asyncio.Task(stream.readline(), loop=self.loop)
    test_utils.run_briefly(self.loop)
    t.cancel()
    test_utils.run_briefly(self.loop)
    # The following line fails if set_exception() isn't careful.
    stream.set_exception(RuntimeError('message'))
    test_utils.run_briefly(self.loop)
    self.assertIs(stream._waiter, None)

def test_start_server(self):

    class MyServer:

        def __init__(self, loop):
            self.server = None
            self.loop = loop

        @asyncio.coroutine
        def handle_client(self, client_reader, client_writer):
            data = yield from client_reader.readline()
            client_writer.write(data)
            yield from client_writer.drain()
            client_writer.close()

        def start(self):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            self.server = self.loop.run_until_complete(
                asyncio.start_server(self.handle_client,
                                     sock=sock,
                                     loop=self.loop))
            return sock.getsockname()

        def handle_client_callback(self, client_reader, client_writer):
            self.loop.create_task(self.handle_client(client_reader,
                                                     client_writer))

        def start_callback(self):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            addr = sock.getsockname()
            sock.close()
            self.server = self.loop.run_until_complete(
                asyncio.start_server(self.handle_client_callback,
                                     host=addr[0], port=addr[1],
                                     loop=self.loop))
            return addr

        def stop(self):
            if self.server is not None:
                self.server.close()
                self.loop.run_until_complete(self.server.wait_closed())
                self.server = None

    @asyncio.coroutine
    def client(addr):
        reader, writer = yield from asyncio.open_connection(
            *addr, loop=self.loop)
        # send a line
        writer.write(b"hello world!\n")
        # read it back
        msgback = yield from reader.readline()
        writer.close()
        return msgback

    # test the server variant with a coroutine as client handler
    server = MyServer(self.loop)
    addr = server.start()
    msg = self.loop.run_until_complete(asyncio.Task(client(addr),
                                                    loop=self.loop))
    server.stop()
    self.assertEqual(msg, b"hello world!\n")

    # test the server variant with a callback as client handler
    server = MyServer(self.loop)
    addr = server.start_callback()
    msg = self.loop.run_until_complete(asyncio.Task(client(addr),
                                                    loop=self.loop))
    server.stop()
    self.assertEqual(msg, b"hello world!\n")

@unittest.skipUnless(hasattr(socket, 'AF_UNIX'), 'No UNIX Sockets')
def test_start_unix_server(self):

    class MyServer:

        def __init__(self, loop, path):
            self.server = None
            self.loop = loop
            self.path = path

        @asyncio.coroutine
        def handle_client(self, client_reader, client_writer):
            data = yield from client_reader.readline()
            client_writer.write(data)
            yield from client_writer.drain()
            client_writer.close()

        def start(self):
            self.server = self.loop.run_until_complete(
                asyncio.start_unix_server(self.handle_client,
                                          path=self.path,
                                          loop=self.loop))

        def handle_client_callback(self, client_reader, client_writer):
            self.loop.create_task(self.handle_client(client_reader,
                                                     client_writer))

        def start_callback(self):
            start = asyncio.start_unix_server(self.handle_client_callback,
                                              path=self.path,
                                              loop=self.loop)
            self.server = self.loop.run_until_complete(start)

        def stop(self):
            if self.server is not None:
                self.server.close()
                self.loop.run_until_complete(self.server.wait_closed())
                self.server = None

    @asyncio.coroutine
    def client(path):
        reader, writer = yield from asyncio.open_unix_connection(
            path, loop=self.loop)
        # send a line
        writer.write(b"hello world!\n")
        # read it back
        msgback = yield from reader.readline()
        writer.close()
        return msgback

    # test the server variant with a coroutine as client handler
    with test_utils.unix_socket_path() as path:
        server = MyServer(self.loop, path)
        server.start()
        msg = self.loop.run_until_complete(asyncio.Task(client(path),
                                                        loop=self.loop))
        server.stop()
        self.assertEqual(msg, b"hello world!\n")

    # test the server variant with a callback as client handler
    with test_utils.unix_socket_path() as path:
        server = MyServer(self.loop, path)
        server.start_callback()
        msg = self.loop.run_until_complete(asyncio.Task(client(path),
                                                        loop=self.loop))
        server.stop()
        self.assertEqual(msg, b"hello world!\n")

@unittest.skipIf(sys.platform == 'win32', "Don't have pipes")
def test_read_all_from_pipe_reader(self):
    # See asyncio issue 168.  This test is derived from the example
    # subprocess_attach_read_pipe.py, but we configure the
    # StreamReader's limit so that twice it is less than the size
    # of the data writter.  Also we must explicitly attach a child
    # watcher to the event loop.

    code = """\
import os, sys
fd = int(sys.argv[1])
os.write(fd, b'data')
os.close(fd)
"""
    rfd, wfd = os.pipe()
    args = [sys.executable, '-c', code, str(wfd)]

    pipe = open(rfd, 'rb', 0)
    reader = asyncio.StreamReader(loop=self.loop, limit=1)
    protocol = asyncio.StreamReaderProtocol(reader, loop=self.loop)
    transport, _ = self.loop.run_until_complete(
        self.loop.connect_read_pipe(lambda: protocol, pipe))

    watcher = asyncio.SafeChildWatcher()
    watcher.attach_loop(self.loop)
    try:
        asyncio.set_child_watcher(watcher)
        create = asyncio.create_subprocess_exec(*args,
                                                pass_fds={wfd},
                                                loop=self.loop)
        proc = self.loop.run_until_complete(create)
        self.loop.run_until_complete(proc.wait())
    finally:
        asyncio.set_child_watcher(None)

    os.close(wfd)
    data = self.loop.run_until_complete(reader.read(-1))
    self.assertEqual(data, b'data')

def test_streamreader_constructor(self):
    self.addCleanup(asyncio.set_event_loop, None)
    asyncio.set_event_loop(self.loop)

    # asyncio issue #184: Ensure that StreamReaderProtocol constructor
    # retrieves the current loop if the loop parameter is not set
    reader = asyncio.StreamReader()
    self.assertIs(reader._loop, self.loop)

def test_streamreaderprotocol_constructor(self):
    self.addCleanup(asyncio.set_event_loop, None)
    asyncio.set_event_loop(self.loop)

    # asyncio issue #184: Ensure that StreamReaderProtocol constructor
    # retrieves the current loop if the loop parameter is not set
    reader = mock.Mock()
    protocol = asyncio.StreamReaderProtocol(reader)
    self.assertIs(protocol._loop, self.loop)

def test_drain_raises(self):
    # See http://bugs.python.org/issue25441

    # This test should not use asyncio for the mock server; the
    # whole point of the test is to test for a bug in drain()
    # where it never gives up the event loop but the socket is
    # closed on the  server side.

    q = queue.Queue()

    def server():
        # Runs in a separate thread.
        sock = socket.socket()
        with sock:
            sock.bind(('localhost', 0))
            sock.listen(1)
            addr = sock.getsockname()
            q.put(addr)
            clt, _ = sock.accept()
            clt.close()

    @asyncio.coroutine
    def client(host, port):
        reader, writer = yield from asyncio.open_connection(
            host, port, loop=self.loop)

        while True:
            writer.write(b"foo\n")
            yield from writer.drain()

    # Start the server thread and wait for it to be listening.
    thread = threading.Thread(target=server)
    thread.setDaemon(True)
    thread.start()
    addr = q.get()

    # Should not be stuck in an infinite loop.
    with self.assertRaises((ConnectionResetError, BrokenPipeError)):
        self.loop.run_until_complete(client(*addr))

    # Clean up the thread.  (Only on success; on failure, it may
    # be stuck in accept().)
    thread.join()

def test___repr__(self):
    stream = asyncio.StreamReader(loop=self.loop)
    self.assertEqual("<StreamReader>", repr(stream))

def test___repr__nondefault_limit(self):
    stream = asyncio.StreamReader(loop=self.loop, limit=123)
    self.assertEqual("<StreamReader l=123>", repr(stream))

def test___repr__eof(self):
    stream = asyncio.StreamReader(loop=self.loop)
    stream.feed_eof()
    self.assertEqual("<StreamReader eof>", repr(stream))

def test___repr__data(self):
    stream = asyncio.StreamReader(loop=self.loop)
    stream.feed_data(b'data')
    self.assertEqual("<StreamReader 4 bytes>", repr(stream))

def test___repr__exception(self):
    stream = asyncio.StreamReader(loop=self.loop)
    exc = RuntimeError()
    stream.set_exception(exc)
    self.assertEqual("<StreamReader e=RuntimeError()>", repr(stream))

def test___repr__waiter(self):
    stream = asyncio.StreamReader(loop=self.loop)
    stream._waiter = asyncio.Future(loop=self.loop)
    self.assertRegex(
        repr(stream),
        r"<StreamReader w=<Future pending[\S ]*>>")
    stream._waiter.set_result(None)
    self.loop.run_until_complete(stream._waiter)
    stream._waiter = None
    self.assertEqual("<StreamReader>", repr(stream))

def test___repr__transport(self):
    stream = asyncio.StreamReader(loop=self.loop)
    stream._transport = mock.Mock()
    stream._transport.__repr__ = mock.Mock()
    stream._transport.__repr__.return_value = "<Transport>"
    self.assertEqual("<StreamReader t=<Transport>>", repr(stream))
