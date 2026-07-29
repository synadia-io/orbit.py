"""Extensions for Core NATS.

The request-many pattern sends one request and yields any number of replies.
Iteration can end after a response count, an overall timeout, an idle timeout,
a caller-defined sentinel, a no-responders status, or subscription closure.

Example::

    from nats.extra import request_many

    responses = await request_many(
        client,
        "services.ping",
        b"ping",
        stall_wait=0.25,
    )
    async for response in responses:
        print(response.data)
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from enum import Enum
from typing import TYPE_CHECKING, Protocol, Self

from nats.client.message import Message

if TYPE_CHECKING:
    from types import TracebackType

try:
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version("nats-extra")
except (ImportError, PackageNotFoundError):  # pragma: no cover
    __version__ = "unknown"

__all__ = [
    "RequestMany",
    "Responses",
    "TerminationReason",
    "request_many",
]

_DEFAULT_MAX_WAIT = 2.0
_NO_RESPONDERS = "503"

Sentinel = Callable[[Message], bool]


class _Subscription(Protocol):
    async def next(self, timeout: float | None = None) -> Message: ...

    async def unsubscribe(self) -> None: ...


class _Client(Protocol):
    def new_inbox(self) -> str: ...

    async def subscribe(self, subject: str) -> _Subscription: ...

    async def publish(self, subject: str, payload: bytes, *, reply: str | None = None) -> None: ...


class TerminationReason(str, Enum):
    """Why a :class:`Responses` iterator stopped."""

    MAX_MESSAGES = "max_messages"
    MAX_WAIT = "max_wait"
    STALL_WAIT = "stall_wait"
    SENTINEL = "sentinel"
    NO_RESPONDERS = "no_responders"
    SUBSCRIPTION_CLOSED = "subscription_closed"


def _validate_duration(name: str, value: float | None) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{name} must not be negative")


def _validate_max_messages(value: int | None) -> None:
    if value is not None and value < 0:
        raise ValueError("max_messages must not be negative")


class RequestMany:
    """Builder for a streaming request/reply operation."""

    def __init__(self, client: _Client, *, max_wait: float | None = _DEFAULT_MAX_WAIT) -> None:
        _validate_duration("max_wait", max_wait)
        self._client = client
        self._sentinel: Sentinel | None = None
        self._max_wait = max_wait
        self._stall_wait: float | None = None
        self._max_messages: int | None = None

    def sentinel(self, predicate: Sentinel) -> Self:
        """Stop when ``predicate`` returns true; the sentinel is not yielded."""
        self._sentinel = predicate
        return self

    def stall_wait(self, seconds: float) -> Self:
        """Stop after ``seconds`` pass without a response."""
        _validate_duration("stall_wait", seconds)
        self._stall_wait = seconds
        return self

    def max_messages(self, count: int) -> Self:
        """Stop after yielding ``count`` responses."""
        _validate_max_messages(count)
        self._max_messages = count
        return self

    def max_wait(self, seconds: float | None) -> Self:
        """Set the overall wait, or disable it with ``None``."""
        _validate_duration("max_wait", seconds)
        self._max_wait = seconds
        return self

    async def send(self, subject: str, payload: bytes = b"") -> Responses:
        """Send the request and return its response iterator."""
        inbox = self._client.new_inbox()
        subscription = await self._client.subscribe(inbox)
        try:
            await self._client.publish(subject, payload, reply=inbox)
        except BaseException:
            await subscription.unsubscribe()
            raise

        return Responses(
            subscription,
            sentinel=self._sentinel,
            max_wait=self._max_wait,
            stall_wait=self._stall_wait,
            max_messages=self._max_messages,
        )


class Responses:
    """Async iterator of replies produced by :class:`RequestMany`.

    Natural termination unsubscribes automatically. If iteration is abandoned
    early, call :meth:`aclose` or use ``async with`` to release the inbox
    subscription immediately.
    """

    def __init__(
        self,
        subscription: _Subscription,
        *,
        sentinel: Sentinel | None,
        max_wait: float | None,
        stall_wait: float | None,
        max_messages: int | None,
    ) -> None:
        self._subscription = subscription
        self._sentinel = sentinel
        self._stall_wait = stall_wait
        self._max_messages = max_messages
        self._messages_received = 0
        self._reason: TerminationReason | None = None
        self._stall_deadline: float | None = None

        loop = asyncio.get_running_loop()
        self._max_deadline = loop.time() + max_wait if max_wait is not None else None

    @property
    def termination_reason(self) -> TerminationReason | None:
        """The reason iteration ended, or ``None`` while it is active."""
        return self._reason

    @property
    def messages_received(self) -> int:
        """Number of replies received, including a terminating sentinel."""
        return self._messages_received

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Message:
        if self._reason is not None:
            raise StopAsyncIteration

        if self._max_messages is not None and self._messages_received >= self._max_messages:
            await self._finish(TerminationReason.MAX_MESSAGES)
            raise StopAsyncIteration

        timeout, timeout_reason = self._next_timeout()
        try:
            message = await self._subscription.next(timeout)
        except TimeoutError:
            assert timeout_reason is not None
            await self._finish(timeout_reason)
            raise StopAsyncIteration from None
        except RuntimeError:
            await self._finish(TerminationReason.SUBSCRIPTION_CLOSED)
            raise StopAsyncIteration from None
        except asyncio.CancelledError:
            await self._finish(TerminationReason.SUBSCRIPTION_CLOSED)
            raise

        if message.status is not None and message.status.code == _NO_RESPONDERS:
            await self._finish(TerminationReason.NO_RESPONDERS)
            raise StopAsyncIteration

        self._messages_received += 1
        if self._stall_wait is not None:
            self._stall_deadline = asyncio.get_running_loop().time() + self._stall_wait

        if self._sentinel is not None:
            try:
                is_sentinel = self._sentinel(message)
            except BaseException:
                await self._finish(TerminationReason.SUBSCRIPTION_CLOSED)
                raise
            if is_sentinel:
                await self._finish(TerminationReason.SENTINEL)
                raise StopAsyncIteration

        return message

    def _next_timeout(self) -> tuple[float | None, TerminationReason | None]:
        loop = asyncio.get_running_loop()
        now = loop.time()
        deadlines: list[tuple[float, TerminationReason]] = []

        if self._max_deadline is not None:
            deadlines.append((self._max_deadline, TerminationReason.MAX_WAIT))

        if self._stall_wait is not None:
            if self._stall_deadline is None:
                self._stall_deadline = now + self._stall_wait
            deadlines.append((self._stall_deadline, TerminationReason.STALL_WAIT))

        if not deadlines:
            return None, None

        deadline, reason = min(deadlines, key=lambda item: item[0])
        return max(0.0, deadline - now), reason

    async def _finish(self, reason: TerminationReason) -> None:
        if self._reason is None:
            self._reason = reason
            await self._subscription.unsubscribe()

    async def aclose(self) -> None:
        """Stop receiving and unsubscribe from the response inbox."""
        await self._finish(TerminationReason.SUBSCRIPTION_CLOSED)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()


async def request_many(
    client: _Client,
    subject: str,
    payload: bytes = b"",
    *,
    sentinel: Sentinel | None = None,
    max_wait: float | None = _DEFAULT_MAX_WAIT,
    stall_wait: float | None = None,
    max_messages: int | None = None,
) -> Responses:
    """Send one request and asynchronously iterate over multiple replies."""
    _validate_duration("max_wait", max_wait)
    _validate_duration("stall_wait", stall_wait)
    _validate_max_messages(max_messages)

    request = RequestMany(client, max_wait=max_wait)
    if sentinel is not None:
        request.sentinel(sentinel)
    if stall_wait is not None:
        request.stall_wait(stall_wait)
    if max_messages is not None:
        request.max_messages(max_messages)
    return await request.send(subject, payload)
