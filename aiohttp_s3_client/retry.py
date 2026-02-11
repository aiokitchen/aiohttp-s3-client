import asyncio
import functools
import logging
from collections.abc import Callable, Coroutine
from typing import Any, ParamSpec, TypeVar


log = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


class Retry:
    """Fixed-delay retry decorator for coroutine functions."""

    __slots__ = ("_max_tries", "_exceptions", "_pause")

    def __init__(
        self,
        *,
        max_tries: int | range,
        exceptions: tuple[type[BaseException], ...] = (),
        pause: float = 0,
    ) -> None:
        if isinstance(max_tries, range):
            max_tries = len(max_tries)
        if max_tries < 1:
            raise ValueError("max_tries must be >= 1")
        if pause < 0:
            raise ValueError("pause must be >= 0")
        self._max_tries = max_tries
        self._exceptions = exceptions
        self._pause = pause

    def __call__(
        self, func: Callable[P, Coroutine[Any, Any, T]],
    ) -> Callable[P, Coroutine[Any, Any, T]]:
        max_tries = self._max_tries
        exceptions = self._exceptions
        pause = self._pause

        @functools.wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_exc: BaseException | None = None
            for attempt in range(1, max_tries + 1):
                try:
                    return await func(*args, **kwargs)
                except asyncio.CancelledError:
                    raise
                except (*exceptions, asyncio.TimeoutError) as exc:
                    last_exc = exc
                    if attempt < max_tries:
                        log.debug(
                            "Retry %d/%d for %s after %r",
                            attempt, max_tries, func.__name__, exc,
                        )
                        if pause > 0:
                            await asyncio.sleep(pause)
                    else:
                        raise
            raise last_exc  # pragma: no cover

        return wrapper
