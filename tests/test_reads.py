import pytest

from async_streams import Limits, Stream, IncompleteReadError


DATA = b'line1\nline2\nline3\n'


def test_feed_empty_data(loop):
    stream = Stream(loop=loop)

    stream._feed_data(b'')
    assert b'' == stream._buffer


def test_feed_nonempty_data(loop):
    stream = Stream(loop=loop)

    stream._feed_data(DATA)
    assert DATA == stream._buffer


async def test_read_zero(loop):
    # Read zero bytes.
    stream = Stream(loop=loop)
    stream._feed_data(DATA)

    data = await stream.read(0)
    assert b'' == data
    assert DATA == stream._buffer


async def test_read(loop):
    # Read bytes.
    stream = Stream(loop=loop)

    def cb():
        stream._feed_data(DATA)
    loop.call_soon(cb)

    data = await stream.read(30)
    assert DATA == data
    assert b'' == stream._buffer


async def test_read_line_breaks(loop):
    # Read bytes without line breaks.
    stream = Stream(loop=loop)
    stream._feed_data(b'line1')
    stream._feed_data(b'line2')

    data = await stream.read(5)

    assert b'line1' == data
    assert b'line2' == stream._buffer


async def test_read_eof(loop):
    # Read bytes, stop at eof.
    stream = Stream(loop=loop)

    def cb():
        stream._feed_eof()
    loop.call_soon(cb)

    data = await stream.read(1024)
    assert b'' == data
    assert b'' == stream._buffer


async def test_read_until_eof(loop):
    # Read all bytes until eof.
    stream = Stream(loop=loop)

    def cb():
        stream._feed_data(b'chunk1\n')
        stream._feed_data(b'chunk2')
        stream._feed_eof()
    loop.call_soon(cb)

    data = await stream.read(-1)

    assert b'chunk1\nchunk2' == data
    assert b'' == stream._buffer


async def test_read_exception(loop):
    stream = Stream(loop=loop)
    stream._feed_data(b'line\n')

    data = await stream.read(2)
    assert b'li' == data

    stream._set_exception(ValueError())

    with pytest.raises(ValueError):
        await stream.read(2)


async def test_read_limit(loop):
    limits = Limits(read_high=3, read_low=1, write_high=10, write_low=1)
    stream = Stream(limits=limits, loop=loop)
    stream._feed_data(b'chunk')
    data = await stream.read(5)
    assert b'chunk' == data
    assert b'' == stream._buffer


async def test_readline(loop):
    # Read one line. 'readline' will need to wait for the data
    # to come from 'cb'
    stream = Stream(loop=loop)
    stream._feed_data(b'chunk1 ')

    def cb():
        stream._feed_data(b'chunk2 ')
        stream._feed_data(b'chunk3 ')
        stream._feed_data(b'\n chunk4')
    loop.call_soon(cb)

    line = await stream.readline()
    assert b'chunk1 chunk2 chunk3 \n' == line
    assert b' chunk4' == stream._buffer


async def test_readline_limit_with_existing_data(loop):
    # Read one line. The data is in StreamReader's buffer
    # before the event loop is run.

    limits = Limits(read_high=3, read_low=1, write_high=10, write_low=1)
    stream = Stream(limits=limits, loop=loop)
    stream._feed_data(b'li')
    stream._feed_data(b'ne1\nline2\n')

    with pytest.raises(ValueError):
        await stream.readline()
    # The buffer should contain the remaining data after exception
    assert b'line2\n' == stream._buffer

    stream = Stream(limits=limits, loop=loop)
    stream._feed_data(b'li')
    stream._feed_data(b'ne1')
    stream._feed_data(b'li')

    with pytest.raises(ValueError):
        await stream.readline()
    # No b'\n' at the end. The 'limit' is set to 3. So before
    # waiting for the new data in buffer, 'readline' will consume
    # the entire buffer, and since the length of the consumed data
    # is more than 3, it will raise a ValueError. The buffer is
    # expected to be empty now.
    assert b'' == stream._buffer


async def test_at_eof(loop):
    stream = Stream(loop=loop)
    assert not stream.at_eof()

    stream._feed_data(b'some data\n')
    assert not stream.at_eof()

    await stream.readline()
    assert not stream.at_eof()

    stream._feed_data(b'some data\n')
    stream._feed_eof()
    await stream.readline()
    assert stream.at_eof()


async def test_readline_limit(loop):
    # Read one line. StreamReaders are fed with data after
    # their 'readline' methods are called.

    limits = Limits(read_high=7, read_low=1, write_high=10, write_low=1)

    stream = Stream(limits=limits, loop=loop)

    def cb():
        stream._feed_data(b'chunk1')
        stream._feed_data(b'chunk2')
        stream._feed_data(b'chunk3\n')
        stream._feed_eof()
    loop.call_soon(cb)

    with pytest.raises(ValueError):
        await stream.readline()
    # The buffer had just one line of data, and after raising
    # a ValueError it should be empty.
    assert b'' == stream._buffer

    stream = Stream(limits=limits, loop=loop)

    def cb():
        stream._feed_data(b'chunk1')
        stream._feed_data(b'chunk2\n')
        stream._feed_data(b'chunk3\n')
        stream._feed_eof()
    loop.call_soon(cb)

    with pytest.raises(ValueError):
        await stream.readline()
    assert b'chunk3\n' == stream._buffer

    # check strictness of the limit
    stream = Stream(limits=limits, loop=loop)
    stream._feed_data(b'1234567\n')
    line = await stream.readline()
    assert b'1234567\n' == line
    assert b'' == stream._buffer

    stream._feed_data(b'12345678\n')
    with pytest.raises(ValueError):
        await stream.readline()
    assert b'' == stream._buffer

    stream._feed_data(b'12345678')
    with pytest.raises(ValueError):
        await stream.readline()
    assert b'' == stream._buffer


async def test_readline_nolimit_nowait(loop):
    # All needed data for the first 'readline' call will be
    # in the buffer.
    stream = Stream(loop=loop)
    stream._feed_data(DATA[:6])
    stream._feed_data(DATA[6:])

    line = await stream.readline()

    assert b'line1\n' == line
    assert b'line2\nline3\n' == stream._buffer


async def test_readline_eof(loop):
    stream = Stream(loop=loop)
    stream._feed_data(b'some data')
    stream._feed_eof()

    line = await stream.readline()
    assert b'some data' == line


async def test_readline_empty_eof(loop):
    stream = Stream(loop=loop)
    stream._feed_eof()

    line = await stream.readline()
    assert b'' == line


async def test_readline_read_byte_count(loop):
    stream = Stream(loop=loop)
    stream._feed_data(DATA)

    await stream.readline()

    data = await stream.read(7)

    assert b'line2\nl' == data
    assert b'ine3\n' == stream._buffer


async def test_readline_exception(loop):
    stream = Stream(loop=loop)
    stream._feed_data(b'line\n')

    data = await stream.readline()
    assert b'line\n' == data

    stream._set_exception(ValueError())
    with pytest.raises(ValueError):
        await stream.readline()
    assert b'' == stream._buffer


async def test_readuntil_separator(loop):
    stream = Stream(loop=loop)
    with pytest.raises(ValueError) as cm:
        await stream.readuntil(separator=b'')
    assert 'Separator should be' in str(cm.value)


async def test_readuntil_multi_chunks(loop):
    stream = Stream(loop=loop)

    stream._feed_data(b'lineAAA')
    data = await stream.readuntil(separator=b'AAA')
    assert b'lineAAA' == data
    assert b'' == stream._buffer

    stream._feed_data(b'lineAAA')
    data = await stream.readuntil(b'AAA')
    assert b'lineAAA' == data
    assert b'' == stream._buffer

    stream._feed_data(b'lineAAAxxx')
    data = await stream.readuntil(b'AAA')
    assert b'lineAAA' == data
    assert b'xxx' == stream._buffer


async def test_readuntil_multi_chunks_1(loop):
    stream = Stream(loop=loop)

    stream._feed_data(b'QWEaa')
    stream._feed_data(b'XYaa')
    stream._feed_data(b'a')
    data = await stream.readuntil(b'aaa')
    assert b'QWEaaXYaaa' == data
    assert b'' == stream._buffer

    stream._feed_data(b'QWEaa')
    stream._feed_data(b'XYa')
    stream._feed_data(b'aa')
    data = await stream.readuntil(b'aaa')
    assert b'QWEaaXYaaa' == data
    assert b'' == stream._buffer

    stream._feed_data(b'aaa')
    data = await stream.readuntil(b'aaa')
    assert b'aaa' == data
    assert b'' == stream._buffer

    stream._feed_data(b'Xaaa')
    data = await stream.readuntil(b'aaa')
    assert b'Xaaa' == data
    assert b'' == stream._buffer

    stream._feed_data(b'XXX')
    stream._feed_data(b'a')
    stream._feed_data(b'a')
    stream._feed_data(b'a')
    data = await stream.readuntil(b'aaa')
    assert b'XXXaaa' == data
    assert b'' == stream._buffer


async def test_readuntil_eof(loop):
    stream = Stream(loop=loop)
    stream._feed_data(b'some dataAA')
    stream._feed_eof()

    with pytest.raises(IncompleteReadError) as cm:
        await stream.readuntil(b'AAA')
    assert cm.value.partial == b'some dataAA'
    assert cm.value.expected is None
    assert b'' == stream._buffer
