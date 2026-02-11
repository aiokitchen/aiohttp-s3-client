import asyncio

import pytest

from aiohttp_s3_client.retry import Retry


async def test_succeeds_on_first_try():
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        return "ok"

    result = await Retry(max_tries=3, exceptions=(ValueError,))(func)()
    assert result == "ok"
    assert call_count == 1


async def test_succeeds_after_transient_failure():
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("transient")
        return "ok"

    result = await Retry(max_tries=3, exceptions=(ValueError,))(func)()
    assert result == "ok"
    assert call_count == 3


async def test_exhausts_max_tries():
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        raise ValueError("always fails")

    with pytest.raises(ValueError, match="always fails"):
        await Retry(max_tries=3, exceptions=(ValueError,))(func)()
    assert call_count == 3


async def test_unmatched_exception_propagates_immediately():
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        raise TypeError("wrong type")

    with pytest.raises(TypeError, match="wrong type"):
        await Retry(max_tries=3, exceptions=(ValueError,))(func)()
    assert call_count == 1


async def test_timeout_error_always_retried():
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise asyncio.TimeoutError()
        return "ok"

    result = await Retry(max_tries=3, exceptions=())(func)()
    assert result == "ok"
    assert call_count == 2


async def test_cancelled_error_always_propagates():
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await Retry(max_tries=3, exceptions=(Exception,))(func)()
    assert call_count == 1


async def test_pause_delays_between_retries():
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("transient")
        return "ok"

    loop = asyncio.get_event_loop()
    t0 = loop.time()
    result = await Retry(
        max_tries=3,
        exceptions=(ValueError,),
        pause=0.1,
    )(func)()
    elapsed = loop.time() - t0

    assert result == "ok"
    assert call_count == 2
    assert elapsed >= 0.09


async def test_invalid_max_tries_raises():
    with pytest.raises(ValueError, match="max_tries must be >= 1"):
        Retry(max_tries=0, exceptions=())


async def test_invalid_pause_raises():
    with pytest.raises(ValueError, match="pause must be >= 0"):
        Retry(max_tries=1, exceptions=(), pause=-1)


async def test_max_tries_accepts_range():
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("transient")
        return "ok"

    result = await Retry(
        max_tries=range(3),
        exceptions=(ValueError,),
    )(func)()
    assert result == "ok"
    assert call_count == 3


async def test_wraps_preserves_function_name():
    async def my_function():
        return "ok"

    wrapped = Retry(max_tries=1, exceptions=())(my_function)
    assert wrapped.__name__ == "my_function"
