"""Tests for streams.py."""

import asyncio
import os
import queue
import socket
import sys
import threading
from unittest import mock
try:
    import ssl
except ImportError:
    ssl = None

import pytest


from async_streams import open_connection
from aiohttp import web


DATA = b'line1\nline2\nline3\n'


async def _basetest_open_connection(stream):
    await stream.write(b'GET / HTTP/1.0\r\n\r\n')
    data = await stream.readline()
    assert data == b'HTTP/1.0 200 OK\r\n'
    data = await stream.read()
    assert data.endswith(b'\r\n\r\nTest message')
    await stream.close()


async def test_open_connection(test_server):
    async def handler(request):
        return web.Response(text="Test message")
    app = web.Application()
    app.router.add_get('/', handler)
    server = await test_server(app)
    url = server.make_url('/')
    stream = await open_connection(url.host, url.port)
    await _basetest_open_connection(stream)


#@unittest.skipUnless(hasattr(socket, 'AF_UNIX'), 'No UNIX Sockets')
def xtest_open_unix_connection(self):
    with test_utils.run_test_unix_server() as httpd:
        conn_fut = asyncio.open_unix_connection(httpd.address,
                                                loop=self.loop)
        self._basetest_open_connection(conn_fut)


@pytest.mark.skipif(ssl is None, reason='No ssl module')
async def xtest_open_connection_ssl(test_server):
    async def handler(request):
        return web.Response(text="Test message")
    app = web.Application()
    app.router.add_get('/', handler)
    server = await test_server(app, ssl=True)
    url = server.make_url('/')
    stream = await open_connection(url.host, url.port, ssl=True)
    await _basetest_open_connection(stream)

#@unittest.skipIf(ssl is None, 'No ssl module')
#@unittest.skipUnless(hasattr(socket, 'AF_UNIX'), 'No UNIX Sockets')
def xtest_open_unix_connection_no_loop_ssl(self):
    with test_utils.run_test_unix_server(use_ssl=True) as httpd:
        conn_fut = asyncio.open_unix_connection(
            httpd.address,
            ssl=test_utils.dummy_ssl_context(),
            server_hostname='',
            loop=self.loop)

        self._basetest_open_connection_no_loop_ssl(conn_fut)

def _basetest_open_connection_error(self, open_connection_fut):
    reader, writer = self.loop.run_until_complete(open_connection_fut)
    writer._protocol.connection_lost(ZeroDivisionError())
    f = reader.read()
    with self.assertRaises(ZeroDivisionError):
        self.loop.run_until_complete(f)
    writer.close()
    test_utils.run_briefly(self.loop)

def xtest_open_connection_error():
    with test_utils.run_test_server() as httpd:
        conn_fut = asyncio.open_connection(*httpd.address,
                                           loop=self.loop)
        self._basetest_open_connection_error(conn_fut)

#@unittest.skipUnless(hasattr(socket, 'AF_UNIX'), 'No UNIX Sockets')
def xtest_open_unix_connection_error(self):
    with test_utils.run_test_unix_server() as httpd:
        conn_fut = asyncio.open_unix_connection(httpd.address,
                                                loop=self.loop)
        self._basetest_open_connection_error(conn_fut)
