"""Distributed counters built on NATS JetStream streams.

Wraps a JetStream stream configured with ``allow_msg_counter`` (ADR-49, requires
nats-server 2.12+) so that each subject in the stream behaves as an independent,
arbitrary-precision counter.

Increments are published with a ``Nats-Incr`` header; the server applies them
atomically and returns the new total. Values are read back from the last stored
message for a subject, optionally with cross-stream source tracking via the
``Nats-Counter-Sources`` header.

Example::

    from nats.client import connect
    from nats.jetstream import new as jetstream
    from orbit import counters

    nc = await connect("nats://localhost:4222")
    js = jetstream(nc)
    stream = await js.create_stream(
        name="COUNTERS", subjects=["events.>"],
        allow_msg_counter=True, allow_direct=True,
    )
    counter = counters.from_stream(js, stream)

    total = await counter.add("events.orders", 1)
    current = await counter.load("events.orders")
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

import nats.jetstream_extra as jetstream_extra
from nats.jetstream.errors import MessageNotFoundError, StreamNotFoundError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from nats.client.message import Headers
    from nats.jetstream import JetStream
    from nats.jetstream.stream import Stream, StreamMessage

try:
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version("orbit-counters")
except (ImportError, PackageNotFoundError):  # pragma: no cover
    __version__ = "unknown"

__all__ = [
    "COUNTER_INCREMENT_HEADER",
    "COUNTER_SOURCES_HEADER",
    "Counter",
    "CounterError",
    "CounterNotEnabledError",
    "CounterNotFoundError",
    "CounterSources",
    "DirectAccessRequiredError",
    "Entry",
    "InvalidCounterValueError",
    "NoCounterForSubjectError",
    "from_stream",
    "get_counter",
]

# Header that requests an atomic counter increment (ADR-49).
COUNTER_INCREMENT_HEADER = "Nats-Incr"
# Header that stores per-source contributions on counter messages.
COUNTER_SOURCES_HEADER = "Nats-Counter-Sources"

# Maps a source identifier to its subject -> value contributions.
CounterSources = dict[str, dict[str, int]]


class CounterError(Exception):
    """Base class for all counter errors."""


class CounterNotEnabledError(CounterError):
    """The stream is not configured for counters (``allow_msg_counter`` must be true)."""


class DirectAccessRequiredError(CounterError):
    """The stream is not configured for direct access (``allow_direct`` must be true)."""


class InvalidCounterValueError(CounterError):
    """A counter value or payload is invalid."""


class CounterNotFoundError(CounterError):
    """The counter (i.e. the backing stream) does not exist."""


class NoCounterForSubjectError(CounterError):
    """The counter has not been initialized for the given subject."""


@dataclass(slots=True)
class Entry:
    """A counter's current state with optional source history."""

    subject: str
    value: int
    sources: CounterSources | None = None
    incr: int | None = None


def _parse_counter_value(data: bytes) -> int:
    if not data:
        raise InvalidCounterValueError("empty counter value")
    try:
        payload = json.loads(data)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidCounterValueError(f"failed to decode counter payload: {exc}") from exc
    if not isinstance(payload, dict) or "val" not in payload:
        raise InvalidCounterValueError("counter payload missing 'val' field")
    try:
        return int(payload["val"])
    except (TypeError, ValueError) as exc:
        raise InvalidCounterValueError(f"invalid counter value: {payload['val']!r}") from exc


def _parse_incr(headers: Headers | None) -> int | None:
    raw = headers.get(COUNTER_INCREMENT_HEADER) if headers is not None else None
    if not raw:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise InvalidCounterValueError(f"invalid counter increment value: {raw!r}") from exc


def _entry_from_message(subject: str, data: bytes, headers: Headers | None) -> Entry:
    return Entry(
        subject=subject,
        value=_parse_counter_value(data),
        sources=_parse_sources(headers),
        incr=_parse_incr(headers),
    )


def _parse_sources(headers: Headers | None) -> CounterSources | None:
    raw = headers.get(COUNTER_SOURCES_HEADER) if headers is not None else None
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise InvalidCounterValueError(f"failed to parse sources: {exc}") from exc
    result: CounterSources = {}
    for source_id, subjects in parsed.items():
        result[source_id] = {}
        for subject, value in subjects.items():
            try:
                result[source_id][subject] = int(value)
            except (TypeError, ValueError) as exc:
                raise InvalidCounterValueError(f"invalid counter value for subject {subject}: {value!r}") from exc
    return result


class Counter:
    """Operations on a JetStream stream configured for distributed counters.

    Each subject in the stream is a separate counter. Construct via
    :func:`from_stream` or :func:`get_counter` rather than directly.
    """

    def __init__(self, js: JetStream, stream: Stream) -> None:
        self._js = js
        self._stream = stream

    async def add(self, subject: str, value: int) -> int:
        """Increment the counter for ``subject`` and return the new total."""
        ack = await self._js.publish(subject, b"", headers={COUNTER_INCREMENT_HEADER: str(value)})
        if not ack.value:
            raise InvalidCounterValueError("counter increment response missing value")
        try:
            return int(ack.value)
        except (TypeError, ValueError) as exc:
            raise InvalidCounterValueError(f"invalid counter value in response: {ack.value!r}") from exc

    async def load(self, subject: str) -> int:
        """Return the current value of the counter for ``subject``."""
        msg = await self._get_last(subject)
        return _parse_counter_value(msg.data)

    async def get(self, subject: str) -> Entry:
        """Return the full entry (value, sources, last increment) for ``subject``."""
        msg = await self._get_last(subject)
        return _entry_from_message(subject, msg.data, msg.headers)

    async def get_multiple(self, subjects: list[str]) -> AsyncIterator[Entry]:
        """Iterate counter entries for ``subjects`` (wildcards supported).

        Fetches the latest message for each matching subject with a single batch
        direct get (``multi_last``). Subjects that have no stored counter are
        omitted from the results rather than reported.

        Raises:
            SubjectRequiredError: if ``subjects`` is empty.
            NoMessagesError: if none of the subjects have a stored counter.
            InvalidCounterValueError: if a matched message is malformed; this
                ends iteration (unlike per-subject omission of misses).
        """
        async for msg in jetstream_extra.get_last_msgs_for(self._js, self._stream.name, subjects):
            yield _entry_from_message(msg.subject, msg.data, msg.headers)

    async def _get_last(self, subject: str) -> StreamMessage:
        try:
            return await self._stream.get_last_message_for_subject(subject)
        except MessageNotFoundError as exc:
            raise NoCounterForSubjectError(f"counter not initialized for subject: {subject}") from exc


def from_stream(js: JetStream, stream: Stream) -> Counter:
    """Wrap an existing counter-enabled stream as a :class:`Counter`.

    Raises:
        CounterNotEnabledError: if the stream lacks ``allow_msg_counter``.
        DirectAccessRequiredError: if the stream lacks ``allow_direct``.
    """
    info = stream.info
    if info is None:
        raise CounterError("stream info is not available; fetch the stream first")
    if not info.config.allow_msg_counter:
        raise CounterNotEnabledError("stream is not configured for counters (allow_msg_counter must be true)")
    if not info.config.allow_direct:
        raise DirectAccessRequiredError("stream must be configured for direct access (allow_direct must be true)")
    return Counter(js, stream)


async def get_counter(js: JetStream, name: str) -> Counter:
    """Fetch a stream by ``name`` and wrap it as a counter.

    Raises:
        CounterNotFoundError: if the stream does not exist.
    """
    try:
        stream = await js.get_stream(name)
    except StreamNotFoundError as exc:
        raise CounterNotFoundError(f"counter not found: {name}") from exc
    return from_stream(js, stream)
