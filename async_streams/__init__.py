import asyncio
import functools
import logging
import socket

from collections import namedtuple


__version__ = '0.0.1'


Limits = namedtuple('Limits', 'read_high read_low write_high write_low')

logger = logging.getLogger(__package__)


_DEFAULT_LIMITS = Limits(2 ** 16, 2 ** 14, 2 ** 16, 2 ** 14)


async def open_connection(host=None, port=None, *,
                          limits=_DEFAULT_LIMITS, **kwds):
    """A wrapper for create_connection() returning a (reader, writer) pair.

    The reader returned is a StreamReader instance; the writer is a
    StreamWriter instance.

    The arguments are all the usual arguments to create_connection()
    except protocol_factory; most common are positional host and port,
    with various optional keyword arguments following.

    Additional optional keyword arguments are loop (to set the event loop
    instance to use) and limit (to set the buffer limit passed to the
    StreamReader).

    (If you want to customize the StreamReader and/or
    StreamReaderProtocol classes, just copy the code -- there's
    really nothing special here except some convenience.)
    """
    loop = asyncio.get_event_loop()
    stream = Stream(limits=limits, loop=loop)
    protocol = _Protocol(stream, loop=loop)
    transport, _ = await loop.create_connection(
        lambda: protocol, host, port, **kwds)
    return stream


async def start_server(client_connected_cb, host=None, port=None, *,
                       limits=_DEFAULT_LIMITS, **kwds):
    """Start a socket server, call back for each client connected.

    The first parameter, `client_connected_cb`, takes two parameters:
    client_reader, client_writer.  client_reader is a StreamReader
    object, while client_writer is a StreamWriter object.  This
    parameter can either be a plain callback function or a coroutine;
    if it is a coroutine, it will be automatically converted into a
    Task.

    The rest of the arguments are all the usual arguments to
    loop.create_server() except protocol_factory; most common are
    positional host and port, with various optional keyword arguments
    following.  The return value is the same as loop.create_server().

    Additional optional keyword arguments are loop (to set the event loop
    instance to use) and limit (to set the buffer limit passed to the
    StreamReader).

    The return value is the same as loop.create_server(), i.e. a
    Server object which can be used to stop the service.
    """
    loop = asyncio.get_event_loop()

    # TODO: make sure client_connected_cb is a coroutine

    def factory():
        stream = Stream(limits=limits, loop=loop)
        protocol = _Protocol(stream, client_connected_cb, loop=loop)
        return protocol

    return await loop.create_server(factory, host, port, **kwds)


if hasattr(socket, 'AF_UNIX'):
    # UNIX Domain Sockets are supported on this platform

    async def open_unix_connection(path=None, *,
                                   limits=_DEFAULT_LIMITS, **kwds):
        """Similar to `open_connection` but works with UNIX Domain Sockets."""
        loop = asyncio.get_event_loop()
        stream = Stream(limits=limits, loop=loop)
        protocol = _Protocol(stream, loop=loop)
        transport, _ = await loop.create_unix_connection(
            lambda: protocol, path, **kwds)
        return stream

    async def start_unix_server(client_connected_cb, path=None, *,
                                limits=_DEFAULT_LIMITS, **kwds):
        """Similar to `start_server` but works with UNIX Domain Sockets."""
        loop = asyncio.get_event_loop()

        # TODO: make sure client_connected_cb is a coroutine

        def factory():
            stream = Stream(limits=limits, loop=loop)
            protocol = _Protocol(stream, client_connected_cb,
                                 loop=loop)
            return protocol

        return await loop.create_unix_server(factory, path, **kwds)


class IncompleteReadError(EOFError):
    """
    Incomplete read error. Attributes:

    - partial: read bytes string before the end of stream was reached
    - expected: total number of expected bytes (or None if unknown)
    """
    def __init__(self, partial, expected):
        super().__init__("%d bytes read on a total of %r expected bytes"
                         % (len(partial), expected))
        self.partial = partial
        self.expected = expected


class LimitOverrunError(Exception):
    """Reached the buffer limit while looking for a separator.

    Attributes:
    - consumed: total number of to be consumed bytes.
    """
    def __init__(self, message, consumed):
        super().__init__(message)
        self.consumed = consumed


class Stream:
    def __init__(self, *, limits=_DEFAULT_LIMITS, loop):
        self._limits = limits
        self._loop = loop
        self._close_waiter = loop.create_future()
        self._transport = None
        self._protocol = None
        self._buffer = bytearray()
        self._eof = False    # Whether we're done.
        self._read_waiter = None  # A future used by _wait_for_data()
        self._write_waiter = None  # A future used by _drain()
        self._exception = None
        self._read_paused = False
        self._write_paused = False

    def __repr__(self):
        info = [self.__class__.__name__]
        if self._buffer:
            info.append('%d bytes' % len(self._buffer))
        if self._eof:
            info.append('eof')
        if self._limits != _DEFAULT_LIMITS:
            info.append('l=%d' % self._limits)
        if self._read_waiter:
            info.append('w=%r' % self._read_waiter)
        if self._exception:
            info.append('e=%r' % self._exception)
        if self._transport:
            info.append('t=%r' % self._transport)
        if self._read_paused:
            info.append('read_paused')
        if self._write_paused:
            info.append('write_paused')
        return '<%s>' % ' '.join(info)

    @property
    def transport(self):
        return self._transport

    def get_extra_info(self, name, default=None):
        return self._transport.get_extra_info(name, default)

    async def write(self, data):
        await self._drain()
        self._transport.write(data)

    async def writelines(self, data):
        await self._drain()
        self._transport.writelines(data)

    async def write_eof(self):
        await self._drain()
        return self._transport.write_eof()

    def can_write_eof(self):
        return self._transport.can_write_eof()

    async def close(self):
        self._transport.close()
        await self._close_waiter

    def exception(self):
        return self._exception

    def _set_exception(self, exc):
        self._exception = exc

        waiter = self._read_waiter
        if waiter is not None:
            self._read_waiter = None
            if not waiter.cancelled():
                waiter.set_exception(exc)

        waiter = self._write_waiter
        if waiter is not None:
            self._write_waiter = None
            if not waiter.cancelled():
                waiter.set_exception(exc)

    def _wakeup_readers(self):
        """Wakeup read*() functions waiting for data or EOF."""
        waiter = self._read_waiter
        if waiter is not None:
            self._read_waiter = None
            if not waiter.cancelled():
                waiter.set_result(None)

    def _wakeup_writers(self):
        """Wakeup write*() functions waiting for data or EOF."""
        waiter = self._write_waiter
        if waiter is not None:
            self._write_waiter = None
            if not waiter.cancelled():
                if self._eof:
                    waiter.set_exception(
                        ConnectionResetError('Connection lost'))
                else:
                    waiter.set_result(None)

    def _setup(self, transport, protocol):
        # INTERNAL API
        assert self._transport is None, 'Transport already set'
        self._transport = transport
        transport.set_write_buffer_limits(self._limits.write_high,
                                          self._limits.write_low)
        assert self._protocol is None, 'Protocol already set'
        self._protocol = protocol

    def _maybe_resume_transport(self):
        if self._read_paused and len(self._buffer) <= self._limits.read_low:
            self._read_paused = False
            self._transport.resume_reading()

    def _feed_eof(self):
        # INTERNAL API
        self._eof = True
        self._wakeup_readers()
        self._wakeup_writers()
        # TODO: cancel writer

    def at_eof(self):
        """Return True if the buffer is empty and 'feed_eof' was called."""
        return self._eof and not self._buffer

    def _feed_data(self, data):
        # INTERNAL API
        assert not self._eof, 'feed_data after feed_eof'

        if not data:
            return

        self._buffer.extend(data)
        self._wakeup_readers()

        if (self._transport is not None and
                not self._read_paused and
                len(self._buffer) > self._limits.read_high):
            try:
                self._transport.pause_reading()
            except NotImplementedError:
                self._set_exception(
                    RuntimeError("Underlying transport "
                                 "doesn't support .pause_reading()"))
            else:
                self._read_paused = True

    async def _wait_for_data(self, func_name):
        """Wait until feed_data() or feed_eof() is called.

        If stream was paused, automatically resume it.
        """
        # StreamReader uses a future to link the protocol feed_data() method
        # to a read coroutine. Running two read coroutines at the same time
        # would have an unexpected behaviour. It would not possible to know
        # which coroutine would get the next data.
        if self._read_waiter is not None:
            raise RuntimeError("%s() called while another coroutine is "
                               "already waiting for incoming data" % func_name)

        if self._eof:
            raise ValueError("I/O operation on closed stream.")

        # Waiting for data while paused will make deadlock, so prevent it.
        # This is essential for readexactly(n) for case when n > self._limit.
        if self._read_paused:
            self._read_paused = False
            self._transport.resume_reading()

        self._read_waiter = self._loop.create_future()
        try:
            await self._read_waiter
        finally:
            self._read_waiter = None

    async def readline(self):
        """Read chunk of data from the stream until newline (b'\n') is found.

        On success, return chunk that ends with newline. If only partial
        line can be read due to EOF, return incomplete line without
        terminating newline. When EOF was reached while no bytes read, empty
        bytes object is returned.

        If limit is reached, ValueError will be raised. In that case, if
        newline was found, complete line including newline will be removed
        from internal buffer. Else, internal buffer will be cleared. Limit is
        compared against part of the line without newline.

        If stream was paused, this function will automatically resume it if
        needed.
        """
        sep = b'\n'
        seplen = len(sep)
        try:
            line = await self.readuntil(sep)
        except IncompleteReadError as e:
            return e.partial
        except LimitOverrunError as e:
            if self._buffer.startswith(sep, e.consumed):
                del self._buffer[:e.consumed + seplen]
            else:
                self._buffer.clear()
            self._maybe_resume_transport()
            raise ValueError(e.args[0])
        return line

    async def readuntil(self, separator=b'\n'):
        """Read data from the stream until ``separator`` is found.

        On success, the data and separator will be removed from the
        internal buffer (consumed). Returned data will include the
        separator at the end.

        Configured stream limit is used to check result. Limit sets the
        maximal length of data that can be returned, not counting the
        separator.

        If an EOF occurs and the complete separator is still not found,
        an IncompleteReadError exception will be raised, and the internal
        buffer will be reset.  The IncompleteReadError.partial attribute
        may contain the separator partially.

        If the data cannot be read because of over limit, a
        LimitOverrunError exception  will be raised, and the data
        will be left in the internal buffer, so it can be read again.
        """
        seplen = len(separator)
        if seplen == 0:
            raise ValueError('Separator should be at least one-byte string')

        if self._exception is not None:
            raise self._exception

        # Consume whole buffer except last bytes, which length is
        # one less than seplen. Let's check corner cases with
        # separator='SEPARATOR':
        # * we have received almost complete separator (without last
        #   byte). i.e buffer='some textSEPARATO'. In this case we
        #   can safely consume len(separator) - 1 bytes.
        # * last byte of buffer is first byte of separator, i.e.
        #   buffer='abcdefghijklmnopqrS'. We may safely consume
        #   everything except that last byte, but this require to
        #   analyze bytes of buffer that match partial separator.
        #   This is slow and/or require FSM. For this case our
        #   implementation is not optimal, since require rescanning
        #   of data that is known to not belong to separator. In
        #   real world, separator will not be so long to notice
        #   performance problems. Even when reading MIME-encoded
        #   messages :)

        # `offset` is the number of bytes from the beginning of the buffer
        # where there is no occurrence of `separator`.
        offset = 0

        # Loop until we find `separator` in the buffer, exceed the buffer size,
        # or an EOF has happened.
        while True:
            buflen = len(self._buffer)

            # Check if we now have enough data in the buffer for `separator` to
            # fit.
            if buflen - offset >= seplen:
                isep = self._buffer.find(separator, offset)

                if isep != -1:
                    # `separator` is in the buffer. `isep` will be used later
                    # to retrieve the data.
                    break

                # see upper comment for explanation.
                offset = buflen + 1 - seplen
                if offset > self._limits.read_high:
                    raise LimitOverrunError(
                        'Separator is not found, and chunk exceed the limit',
                        offset)

            # Complete message (with full separator) may be present in buffer
            # even when EOF flag is set. This may happen when the last chunk
            # adds data which makes separator be found. That's why we check for
            # EOF *ater* inspecting the buffer.
            if self._eof:
                chunk = bytes(self._buffer)
                self._buffer.clear()
                raise IncompleteReadError(chunk, None)

            # _wait_for_data() will resume reading if stream was paused.
            await self._wait_for_data('readuntil')

        if isep > self._limits.read_high:
            raise LimitOverrunError(
                'Separator is found, but chunk is longer than limit', isep)

        chunk = self._buffer[:isep + seplen]
        del self._buffer[:isep + seplen]
        self._maybe_resume_transport()
        return bytes(chunk)

    async def read(self, n=-1):
        """Read up to `n` bytes from the stream.

        If n is not provided, or set to -1, read until EOF and return all read
        bytes. If the EOF was received and the internal buffer is empty, return
        an empty bytes object.

        If n is zero, return empty bytes object immediately.

        If n is positive, this function try to read `n` bytes, and may return
        less or equal bytes than requested, but at least one byte. If EOF was
        received before any byte is read, this function returns empty byte
        object.

        Returned value is not limited with limit, configured at stream
        creation.

        If stream was paused, this function will automatically resume it if
        needed.
        """

        if self._exception is not None:
            raise self._exception

        if n == 0:
            return b''

        if n < 0:
            # This used to just loop creating a new waiter hoping to
            # collect everything in self._buffer, but that would
            # deadlock if the subprocess sends more than self.limit
            # bytes.  So just call self.read(self._limit) until EOF.
            blocks = []
            while True:
                block = await self.read(self._limits.read_high)
                if not block:
                    break
                blocks.append(block)
            return b''.join(blocks)

        if not self._buffer and not self._eof:
            await self._wait_for_data('read')

        # This will work right even if buffer is less than n bytes
        data = bytes(self._buffer[:n])
        del self._buffer[:n]

        self._maybe_resume_transport()
        return data

    async def readexactly(self, n):
        """Read exactly `n` bytes.

        Raise an IncompleteReadError if EOF is reached before `n` bytes can be
        read. The IncompleteReadError.partial attribute of the exception will
        contain the partial read bytes.

        if n is zero, return empty bytes object.

        Returned value is not limited with limit, configured at stream
        creation.

        If stream was paused, this function will automatically resume it if
        needed.
        """
        if n < 0:
            raise ValueError('readexactly size can not be less than zero')

        if self._exception is not None:
            raise self._exception

        if n == 0:
            return b''

        while len(self._buffer) < n:
            if self._eof:
                incomplete = bytes(self._buffer)
                self._buffer.clear()
                raise IncompleteReadError(incomplete, n)

            await self._wait_for_data('readexactly')

        if len(self._buffer) == n:
            data = bytes(self._buffer)
            self._buffer.clear()
        else:
            data = bytes(self._buffer[:n])
            del self._buffer[:n]
        self._maybe_resume_transport()
        return data

    def __aiter__(self):
        return self

    async def __anext__(self):
        val = await self.readline()
        if val == b'':
            raise StopAsyncIteration
        return val

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def _drain(self):
        exc = self._exception
        if exc is not None:
            raise exc
        assert self._transport is not None
        if self._transport.is_closing():
            raise ConnectionResetError('Connection lost')
        if not self._write_paused:
            return
        waiter = self._write_waiter
        if waiter is None or waiter.cancelled():
            waiter = self._loop.create_future()
            self._write_waiter = waiter
            await waiter
        else:
            new_waiter = self._loop.create_future()
            cb = functools.partial(_chain, new_waiter)
            waiter.add_done_callback(cb)
            await new_waiter


def _chain(new_waiter, real_waiter):
    if real_waiter.cancelled():
        new_waiter.cancel()
    elif real_waiter.exception() is not None:
        new_waiter.set_exception(real_waiter.exception())
    else:
        new_waiter.set_result(real_waiter.result())


class _Protocol(asyncio.Protocol):

    def __init__(self, stream, client_connected_cb=None, *, loop):
        self._loop = loop
        self._stream = stream
        self._client_connected_cb = client_connected_cb
        self._over_ssl = False

    def pause_writing(self):
        stream = self._stream
        assert not stream._write_paused
        stream._write_paused = True
        if self._loop.get_debug():
            logger.debug("%r pauses writing", stream)

    def resume_writing(self):
        stream = self._stream
        assert stream._write_paused
        stream._write_paused = False
        if self._loop.get_debug():
            logger.debug("%r resumes writing", stream)
        stream._wakeup_writers()

    def connection_made(self, transport):
        self._stream._setup(transport, self)
        self._over_ssl = transport.get_extra_info('sslcontext') is not None
        if self._client_connected_cb is not None:
            coro = self._client_connected_cb(self._stream),
            # TODO: await the task somewhere
            self._loop.create_task(coro)

    def connection_lost(self, exc):
        if exc is None:
            self._stream._feed_eof()
        else:
            self._stream._set_exception(exc)
        self._stream._close_waiter.set_result(None)
        self._stream = None

    def data_received(self, data):
        self._stream._feed_data(data)

    def eof_received(self):
        self._stream._feed_eof()
        if self._over_ssl:
            # Prevent a warning in SSLProtocol.eof_received:
            # "returning true from eof_received()
            # has no effect when using ssl"
            return False
        return True
