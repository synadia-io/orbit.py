"""JetStream batch retrieval and publishing extensions.

Batch direct get lets a client fetch several stored messages with a single
request instead of one round-trip per message. The server streams the matching
messages back on a reply inbox and terminates the stream with an end-of-batch
(EOB) sentinel.

Two entry points are provided, mirroring orbit.go's ``jetstreamext``:

* :func:`get_batch` — a batch of messages from a starting point (sequence or
  time), optionally filtered by subject.
* :func:`get_last_msgs_for` — the last message for each of a list of subjects
  (wildcards allowed; the server matches at most 1024 subjects).

Both require the stream to be configured with ``allow_direct`` and a
nats-server new enough to support batch direct get (2.11+).

Fast-ingest publishing is also available through :func:`fast_publish`. It is
non-atomic: each message is stored as it arrives, with a persistent reply
inbox and server-driven flow control. It requires ``allow_batched`` on the
stream and nats-server 2.14+.

Example::

    from nats.client import connect
    from nats.jetstream import new as jetstream
    from orbit import jetstreamext

    nc = await connect("nats://localhost:4222")
    js = jetstream(nc)

    async for msg in jetstreamext.get_last_msgs_for(js, "EVENTS", ["events.a", "events.b"]):
        print(msg.subject, msg.sequence, msg.data)
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping

    from nats.client.message import Headers, Message
    from nats.jetstream import JetStream

try:
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version("orbit-jetstreamext")
except (ImportError, PackageNotFoundError):  # pragma: no cover
    __version__ = "unknown"

__all__ = [
    "BatchUnsupportedError",
    "FastPubAck",
    "FastPublishClosedError",
    "FastPublishConfigError",
    "FastPublishEmptyBatchError",
    "FastPublishError",
    "FastPublishFlowError",
    "FastPublishGapError",
    "FastPublishInvalidBatchIdError",
    "FastPublishInvalidPatternError",
    "FastPublishNotEnabledError",
    "FastPublishPublishError",
    "FastPublishResponseError",
    "FastPublishSubscribeError",
    "FastPublishTimeoutError",
    "FastPublishTooManyInflightError",
    "FastPublishUnknownBatchIdError",
    "FastPublisher",
    "GapMode",
    "InvalidOptionError",
    "InvalidResponseError",
    "JetStreamExtError",
    "NoMessagesError",
    "RawStreamMsg",
    "SubjectRequiredError",
    "fast_publish",
    "get_batch",
    "get_last_msgs_for",
]

# Control-message markers used to frame a batch direct get response.
_STATUS_NO_CONTENT = "204"
_STATUS_NO_MESSAGES = "404"
_DESCRIPTION_EOB = "EOB"

# Per-message headers set by the server on direct get responses.
_HEADER_STREAM = "Nats-Stream"
_HEADER_SUBJECT = "Nats-Subject"
_HEADER_SEQUENCE = "Nats-Sequence"
_HEADER_TIMESTAMP = "Nats-Time-Stamp"
_HEADER_NUM_PENDING = "Nats-Num-Pending"
_HEADER_LAST_SEQUENCE = "Nats-Last-Sequence"

# The server matches at most this many subjects for a multi_last request.
_MAX_SUBJECTS = 1024


class JetStreamExtError(Exception):
    """Base class for all jetstreamext errors."""


class InvalidOptionError(JetStreamExtError):
    """An invalid or conflicting option was provided."""


class SubjectRequiredError(JetStreamExtError):
    """No subjects were provided to :func:`get_last_msgs_for`."""


class NoMessagesError(JetStreamExtError):
    """No messages matched the request."""


class BatchUnsupportedError(JetStreamExtError):
    """The server does not support batch direct get (requires nats-server 2.11+)."""


class InvalidResponseError(JetStreamExtError):
    """The server returned a response that could not be understood."""


@dataclass(slots=True)
class RawStreamMsg:
    """A stored message returned by a batch direct get."""

    subject: str
    sequence: int
    data: bytes
    time: datetime
    headers: Headers | None = None
    num_pending: int | None = None
    last_sequence: int | None = None


def _parse_timestamp(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    # datetime supports at most microsecond precision; the server may send
    # nanoseconds (RFC3339Nano), so truncate any extra fractional digits.
    text = re.sub(r"(\.\d{6})\d+", r"\1", text)
    return datetime.fromisoformat(text)


def _is_eob(msg: Message) -> bool:
    status = msg.status
    return (
        not msg.data
        and status is not None
        and status.code == _STATUS_NO_CONTENT
        and status.description == _DESCRIPTION_EOB
    )


def _int_header(headers: Headers, key: str) -> int:
    raw = headers.get(key)
    if not raw:
        raise InvalidResponseError(f"missing {key} header")
    try:
        return int(raw)
    except ValueError as exc:
        raise InvalidResponseError(f"invalid {key} header: {raw!r}") from exc


def _convert(msg: Message) -> RawStreamMsg:
    if not msg.data and msg.status is not None and msg.status.code == _STATUS_NO_MESSAGES:
        raise NoMessagesError("no messages")

    headers = msg.headers
    if headers is None:
        raise InvalidResponseError("response should have headers")

    # A missing Nats-Num-Pending header means the server predates batch direct
    # get and answered a plain direct get instead.
    if headers.get(_HEADER_NUM_PENDING) is None:
        raise BatchUnsupportedError("batch get not supported by server")

    if not headers.get(_HEADER_STREAM):
        raise InvalidResponseError("missing Nats-Stream header")
    subject = headers.get(_HEADER_SUBJECT)
    if not subject:
        raise InvalidResponseError("missing Nats-Subject header")
    timestamp = headers.get(_HEADER_TIMESTAMP)
    if not timestamp:
        raise InvalidResponseError("missing Nats-Time-Stamp header")

    last_raw = headers.get(_HEADER_LAST_SEQUENCE)
    return RawStreamMsg(
        subject=subject,
        sequence=_int_header(headers, _HEADER_SEQUENCE),
        data=msg.data,
        time=_parse_timestamp(timestamp),
        headers=headers,
        num_pending=_int_header(headers, _HEADER_NUM_PENDING),
        last_sequence=int(last_raw) if last_raw else None,
    )


async def _get_direct(
    js: JetStream, stream: str, request: Mapping[str, object], timeout: float
) -> AsyncIterator[RawStreamMsg]:
    client = js.client
    subject = f"{js.api_prefix}.DIRECT.GET.{stream}"
    inbox = client.new_inbox()
    subscription = await client.subscribe(inbox)
    try:
        await client.publish(subject, json.dumps(request).encode(), reply=inbox)
        while True:
            msg = await subscription.next(timeout)
            if _is_eob(msg):
                return
            yield _convert(msg)
    finally:
        await subscription.unsubscribe()


def _isoformat(value: datetime) -> str:
    return value.isoformat()


def get_batch(
    js: JetStream,
    stream: str,
    batch: int,
    *,
    seq: int | None = None,
    next_by_subject: str | None = None,
    start_time: datetime | None = None,
    max_bytes: int | None = None,
    timeout: float = 5.0,
) -> AsyncIterator[RawStreamMsg]:
    """Fetch a batch of up to ``batch`` messages from ``stream``.

    By default fetching starts at the first message. Provide ``seq`` or
    ``start_time`` (but not both) to start elsewhere, ``next_by_subject`` to
    only match a subject (wildcards allowed), and ``max_bytes`` to cap the
    total size the server returns.

    Returns an async iterator over :class:`RawStreamMsg`. Iteration ends at the
    end-of-batch sentinel; a server error surfaces as an exception mid-iteration.

    Raises:
        InvalidOptionError: for a non-positive ``batch``/``seq``/``max_bytes`` or
            if both ``seq`` and ``start_time`` are given.
    """
    if batch <= 0:
        raise InvalidOptionError("batch has to be greater than 0")
    if seq is not None and start_time is not None:
        raise InvalidOptionError("cannot set both seq and start_time")
    if seq is not None and seq <= 0:
        raise InvalidOptionError("seq has to be greater than 0")
    if max_bytes is not None and max_bytes <= 0:
        raise InvalidOptionError("max_bytes has to be greater than 0")

    request: dict[str, object] = {"batch": batch}
    if start_time is not None:
        request["start_time"] = _isoformat(start_time)
    else:
        # Mirror orbit.go: default to the first message when no start is given.
        request["seq"] = seq if seq is not None else 1
    if next_by_subject is not None:
        request["next_by_subj"] = next_by_subject
    if max_bytes is not None:
        request["max_bytes"] = max_bytes

    return _get_direct(js, stream, request, timeout)


def get_last_msgs_for(
    js: JetStream,
    stream: str,
    subjects: list[str],
    *,
    batch: int | None = None,
    up_to_seq: int | None = None,
    up_to_time: datetime | None = None,
    timeout: float = 5.0,
) -> AsyncIterator[RawStreamMsg]:
    """Fetch the last message for each of ``subjects`` from ``stream``.

    Subjects may include wildcards; the server matches at most 1024. Use
    ``up_to_seq`` or ``up_to_time`` (but not both) to fetch the last message at
    or before a point rather than the latest, and ``batch`` to cap how many
    messages are returned.

    Returns an async iterator over :class:`RawStreamMsg`. Iteration ends at the
    end-of-batch sentinel; a server error surfaces as an exception mid-iteration.

    Raises:
        SubjectRequiredError: if ``subjects`` is empty.
        InvalidOptionError: for too many subjects, a non-positive ``batch``, or
            if both ``up_to_seq`` and ``up_to_time`` are given.
    """
    if not subjects:
        raise SubjectRequiredError("at least one subject is required")
    if len(subjects) > _MAX_SUBJECTS:
        raise InvalidOptionError(f"at most {_MAX_SUBJECTS} subjects are allowed")
    if up_to_seq is not None and up_to_time is not None:
        raise InvalidOptionError("cannot set both up_to_seq and up_to_time")
    if batch is not None and batch <= 0:
        raise InvalidOptionError("batch has to be greater than 0")

    request: dict[str, object] = {"multi_last": subjects}
    if batch is not None:
        request["batch"] = batch
    if up_to_seq is not None:
        request["up_to_seq"] = up_to_seq
    if up_to_time is not None:
        request["up_to_time"] = _isoformat(up_to_time)

    return _get_direct(js, stream, request, timeout)


# Imported after JetStreamExtError is defined because the fast publisher's
# typed error hierarchy derives from it.
from orbit.jetstreamext.fast_publish import (  # noqa: E402
    FastPubAck,
    FastPublishClosedError,
    FastPublishConfigError,
    FastPublishEmptyBatchError,
    FastPublisher,
    FastPublishError,
    FastPublishFlowError,
    FastPublishGapError,
    FastPublishInvalidBatchIdError,
    FastPublishInvalidPatternError,
    FastPublishNotEnabledError,
    FastPublishPublishError,
    FastPublishResponseError,
    FastPublishSubscribeError,
    FastPublishTimeoutError,
    FastPublishTooManyInflightError,
    FastPublishUnknownBatchIdError,
    GapMode,
    fast_publish,
)
