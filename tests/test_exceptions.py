import asyncio
from unittest import mock

import pytest

from async_streams import Stream, Limits


def test_exception(loop):
    stream = Stream(loop=loop)
    assert stream.exception() is None

    exc = ValueError()
    stream._set_exception(exc)
    assert stream.exception() is exc


async def test_exception_read_waiter(loop):
    stream = Stream(loop=loop)

    async def set_err():
        stream._set_exception(ValueError())

    t1 = loop.create_task(stream.readline())
    t2 = loop.create_task(set_err())

    await asyncio.wait([t1, t2], loop=loop)

    with pytest.raises(ValueError):
        t1.result()


async def test_exception_write_waiter(loop):
    limits = Limits(read_high=5, read_low=1, write_high=1, write_low=0)
    stream = Stream(limits=limits, loop=loop)
    stream._transport = mock.Mock()
    stream._transport.is_closing.return_value = False

    async def set_err():
        stream._set_exception(ValueError())

    t1 = loop.create_task(stream.write(b'123456'))
    t2 = loop.create_task(set_err())

    await asyncio.wait([t1, t2], loop=loop)

    with pytest.raises(ValueError):
        t1.result()


async def test_exception_cancel_readers(loop):
    stream = Stream(loop=loop)

    t = loop.create_task(stream.readline())
    await asyncio.sleep(0, loop=loop)
    assert stream._read_waiter is not None
    t.cancel()
    await asyncio.sleep(0, loop=loop)

    # The following line fails if set_exception() isn't careful.
    stream._set_exception(RuntimeError('message'))
    await asyncio.sleep(0, loop=loop)

    assert stream._read_waiter is None


async def test_exception_cancel_writers(loop):
    limits = Limits(read_high=5, read_low=1, write_high=1, write_low=0)
    stream = Stream(loop=loop, limits=limits)

    t = loop.create_task(stream.write(b'123456789'))
    await asyncio.sleep(0, loop=loop)
    assert stream._write_waiter is not None
    t.cancel()
    await asyncio.sleep(0, loop=loop)

    # The following line fails if set_exception() isn't careful.
    stream._set_exception(RuntimeError('message'))
    await asyncio.sleep(0, loop=loop)

    assert stream._write_waiter is None
