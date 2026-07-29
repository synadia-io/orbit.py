"""Atomic JetStream batch publishing.

Atomic batches are held by the server until a final message carrying the
commit marker arrives. Either every message is stored, in order, or none of
them are. The target stream must enable atomic publishing.
"""

from __future__ import annotations

import json
import math
import uuid
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from nats.client.errors import StatusError
from nats.client.message import Headers
from nats.jetstream.headers import NATS_BATCH_COMMIT, NATS_BATCH_COMMIT_FINAL, NATS_BATCH_ID, NATS_BATCH_SEQUENCE

from . import JetStreamExtError

if TYPE_CHECKING:
    from nats.jetstream import JetStream


__all__ = [
    "AtomicPublishDuplicateMessageIDError",
    "AtomicPublishIncompleteError",
    "AtomicPublishInvalidCommitError",
    "AtomicPublishInvalidIDError",
    "AtomicPublishMirrorError",
    "AtomicPublishMissingSequenceError",
    "AtomicPublishNotEnabledError",
    "AtomicPublishTooManyInflightError",
    "AtomicPublishUnsupportedHeaderError",
    "BatchAck",
    "BatchClosedError",
    "BatchMessage",
    "BatchPublishError",
    "BatchPublishRequestError",
    "BatchPublishServerError",
    "BatchPublisher",
    "BatchTooLargeError",
    "EmptyBatchError",
    "InvalidBatchAckError",
    "batch_publish",
    "publish_batch",
]


_MAX_BATCH_SIZE = 1000

_EXPECTED_LAST_SEQUENCE = "Nats-Expected-Last-Sequence"

_MANAGED_HEADERS = frozenset(
    {
        NATS_BATCH_ID.lower(),
        NATS_BATCH_SEQUENCE.lower(),
        NATS_BATCH_COMMIT.lower(),
    }
)
_UNSUPPORTED_HEADERS = frozenset({"nats-expected-last-msg-id"})


class BatchPublishError(JetStreamExtError):
    """Base class for atomic batch publish failures."""


class BatchClosedError(BatchPublishError):
    """The publisher has already been closed or failed during I/O."""


class BatchTooLargeError(BatchPublishError):
    """The batch would exceed the server limit of 1,000 messages."""


class EmptyBatchError(BatchPublishError):
    """The bulk publish input contained no messages."""


class InvalidBatchAckError(BatchPublishError):
    """The server's commit acknowledgement violated batch invariants."""


class BatchPublishRequestError(BatchPublishError):
    """A core publish, acknowledgement request, or status response failed."""


class BatchPublishServerError(BatchPublishError):
    """A JetStream server error returned while publishing a batch."""

    def __init__(
        self,
        description: str,
        *,
        code: int | None = None,
        error_code: int | None = None,
    ) -> None:
        super().__init__(description)
        self.code = code
        self.error_code = error_code
        self.description = description


class AtomicPublishNotEnabledError(BatchPublishServerError):
    """Atomic publishing is not enabled on the target stream (10174)."""


class AtomicPublishMissingSequenceError(BatchPublishServerError):
    """A batch message had no valid sequence header (10175)."""


class AtomicPublishIncompleteError(BatchPublishServerError):
    """The server abandoned an incomplete atomic batch (10176)."""


class AtomicPublishUnsupportedHeaderError(BatchPublishServerError):
    """A message contained a header unsupported by atomic publishing (10177)."""


class AtomicPublishInvalidIDError(BatchPublishServerError):
    """The batch identifier was invalid (10179)."""


class AtomicPublishMirrorError(BatchPublishServerError):
    """Atomic publishing was configured on a mirror stream (10198)."""


class AtomicPublishInvalidCommitError(BatchPublishServerError):
    """The batch commit marker was invalid (10200)."""


class AtomicPublishDuplicateMessageIDError(BatchPublishServerError):
    """The batch contained duplicate message identifiers (10201)."""


class AtomicPublishTooManyInflightError(BatchPublishServerError):
    """The server's inflight atomic-batch limit was reached (10210)."""


_SERVER_ERRORS: dict[int, type[BatchPublishServerError]] = {
    10174: AtomicPublishNotEnabledError,
    10175: AtomicPublishMissingSequenceError,
    10176: AtomicPublishIncompleteError,
    10177: AtomicPublishUnsupportedHeaderError,
    10179: AtomicPublishInvalidIDError,
    10198: AtomicPublishMirrorError,
    10200: AtomicPublishInvalidCommitError,
    10201: AtomicPublishDuplicateMessageIDError,
    10210: AtomicPublishTooManyInflightError,
}


_HeaderValue = str | Sequence[str]


@dataclass(frozen=True, slots=True)
class BatchMessage:
    """An outbound message used by :func:`publish_batch`."""

    subject: str
    data: bytes
    headers: Headers | Mapping[str, _HeaderValue] | None = None


@dataclass(frozen=True, slots=True)
class BatchAck:
    """The validated acknowledgement for a committed atomic batch."""

    stream: str
    sequence: int
    batch_id: str
    batch_size: int
    domain: str | None = None
    value: str | None = None


def _copy_headers(headers: Headers | Mapping[str, _HeaderValue] | None) -> dict[str, str | list[str]]:
    if headers is None:
        return {}
    source = headers.asdict() if isinstance(headers, Headers) else headers
    copied: dict[str, str | list[str]] = {}
    for name, value in source.items():
        if isinstance(value, str):
            copied[name] = value
        else:
            copied[name] = list(value)
    return copied


def _validate_user_headers(headers: Mapping[str, _HeaderValue], prior_sequence: int) -> None:
    names = {name.lower() for name in headers}
    managed = names & _MANAGED_HEADERS
    if managed:
        header = sorted(managed)[0]
        raise AtomicPublishUnsupportedHeaderError(f"header {header!r} is managed by atomic batch publishing")
    unsupported = names & _UNSUPPORTED_HEADERS
    if unsupported:
        header = sorted(unsupported)[0]
        raise AtomicPublishUnsupportedHeaderError(f"header {header!r} is unsupported by atomic batch publishing")
    if prior_sequence >= 1 and _EXPECTED_LAST_SEQUENCE.lower() in names:
        raise AtomicPublishUnsupportedHeaderError(
            f"{_EXPECTED_LAST_SEQUENCE} is allowed only on the first message in a batch"
        )


def _positive_timeout(timeout: float) -> float:
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and greater than 0")
    return timeout


def _parse_json(data: bytes) -> Any:
    try:
        return json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InvalidBatchAckError("server returned an invalid batch acknowledgement") from exc


def _server_error(data: Any) -> BatchPublishError | None:
    if not isinstance(data, dict) or not isinstance(data.get("error"), dict):
        return None

    error = data["error"]
    code = error.get("code")
    error_code = error.get("err_code")
    description = error.get("description")
    code = code if isinstance(code, int) else None
    error_code = error_code if isinstance(error_code, int) else None
    description = description if isinstance(description, str) else "atomic batch publish failed"

    if error_code == 10199:
        return BatchTooLargeError(description)
    error_type = _SERVER_ERRORS.get(error_code, BatchPublishServerError)
    return error_type(description, code=code, error_code=error_code)


class BatchPublisher:
    """A single stateful atomic batch.

    Messages passed to :meth:`add` are sent immediately but remain invisible
    in the stream until :meth:`commit` succeeds. This object is intended for
    use by one asyncio task at a time.
    """

    def __init__(
        self,
        js: JetStream,
        *,
        ack_first: bool = True,
        ack_every: int | None = None,
        timeout: float = 5.0,
    ) -> None:
        if ack_every is not None and (not isinstance(ack_every, int) or isinstance(ack_every, bool) or ack_every <= 0):
            raise ValueError("ack_every must be greater than 0")
        self._js = js
        self._ack_first = ack_first
        self._ack_every = ack_every
        self._timeout = _positive_timeout(timeout)
        self._batch_id = uuid.uuid4().hex
        self._sequence = 0
        self._closed = False

    @property
    def batch_id(self) -> str:
        """The unique identifier placed on every message in this batch."""
        return self._batch_id

    @property
    def size(self) -> int:
        """The number of messages sent so far, excluding a future commit."""
        return self._sequence

    @property
    def is_closed(self) -> bool:
        """Whether the batch was committed, discarded, or failed during I/O."""
        return self._closed

    def _check_open(self) -> None:
        if self._closed:
            raise BatchClosedError("batch publisher is closed")

    def _prepare(
        self,
        headers: Headers | Mapping[str, _HeaderValue] | None,
        *,
        commit: bool,
    ) -> tuple[int, dict[str, str | list[str]]]:
        self._check_open()
        copied = _copy_headers(headers)
        _validate_user_headers(copied, self._sequence)
        if self._sequence >= _MAX_BATCH_SIZE:
            raise BatchTooLargeError("batch exceeds the server limit of 1,000 messages")

        sequence = self._sequence + 1
        copied[NATS_BATCH_ID] = self._batch_id
        copied[NATS_BATCH_SEQUENCE] = str(sequence)
        if commit:
            copied[NATS_BATCH_COMMIT] = NATS_BATCH_COMMIT_FINAL
        return sequence, copied

    def _needs_ack(self, sequence: int) -> bool:
        return (self._ack_first and sequence == 1) or (self._ack_every is not None and sequence % self._ack_every == 0)

    async def _request(self, subject: str, data: bytes, headers: dict[str, str | list[str]]) -> bytes:
        try:
            response = await self._js.client.request(
                subject,
                data,
                headers=headers,
                timeout=self._timeout,
                return_on_error=True,
            )
        except Exception as exc:
            raise BatchPublishRequestError("atomic batch acknowledgement request failed") from exc
        if response.status is not None and response.status.code != "200":
            description = response.status.description or "Unknown error"
            cause = StatusError.from_status(response.status.code, description, subject=subject)
            raise BatchPublishRequestError(f"atomic batch acknowledgement request failed: {cause}") from cause
        return response.data

    async def _publish(self, subject: str, data: bytes, headers: dict[str, str | list[str]]) -> None:
        try:
            await self._js.client.publish(subject, data, headers=headers)
        except Exception as exc:
            raise BatchPublishRequestError("atomic batch message publish failed") from exc

    async def add(
        self,
        subject: str,
        data: bytes,
        *,
        headers: Headers | Mapping[str, _HeaderValue] | None = None,
    ) -> None:
        """Send one non-final message as part of the batch.

        Validation failures leave the publisher usable. Any failure after I/O
        begins closes it because the server-side state is then uncertain.
        """
        sequence, outbound_headers = self._prepare(headers, commit=False)
        self._sequence = sequence
        try:
            if self._needs_ack(sequence):
                response = await self._request(subject, data, outbound_headers)
                if response:
                    parsed = _parse_json(response)
                    if error := _server_error(parsed):
                        raise error
                    raise InvalidBatchAckError("server returned an invalid flow-control acknowledgement")
            else:
                await self._publish(subject, data, outbound_headers)
        except BaseException:
            self._closed = True
            raise

    async def commit(
        self,
        subject: str,
        data: bytes,
        *,
        headers: Headers | Mapping[str, _HeaderValue] | None = None,
    ) -> BatchAck:
        """Send the final message, commit the batch, and return its ack."""
        sequence, outbound_headers = self._prepare(headers, commit=True)
        self._sequence = sequence
        self._closed = True

        response = await self._request(subject, data, outbound_headers)
        parsed = _parse_json(response)
        if error := _server_error(parsed):
            raise error
        return self._parse_ack(parsed)

    def _parse_ack(self, data: Any) -> BatchAck:
        if not isinstance(data, dict):
            raise InvalidBatchAckError("server returned an invalid batch acknowledgement")
        stream = data.get("stream")
        sequence = data.get("seq")
        batch_id = data.get("batch")
        batch_size = data.get("count")
        domain = data.get("domain")
        value = data.get("val")
        if (
            not isinstance(stream, str)
            or not stream
            or not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or batch_id != self._batch_id
            or batch_size != self._sequence
            or not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or (domain is not None and not isinstance(domain, str))
            or (value is not None and not isinstance(value, str))
        ):
            raise InvalidBatchAckError("server returned an invalid batch acknowledgement")
        return BatchAck(
            stream=stream,
            sequence=sequence,
            batch_id=batch_id,
            batch_size=batch_size,
            domain=domain,
            value=value,
        )

    def discard(self) -> None:
        """Close without committing; the server expires pending messages."""
        self._check_open()
        self._closed = True


def batch_publish(
    js: JetStream,
    *,
    ack_first: bool = True,
    ack_every: int | None = None,
    timeout: float = 5.0,
) -> BatchPublisher:
    """Create one stateful atomic batch publisher."""
    return BatchPublisher(js, ack_first=ack_first, ack_every=ack_every, timeout=timeout)


async def _messages(
    messages: Iterable[BatchMessage] | AsyncIterable[BatchMessage],
) -> AsyncIterator[BatchMessage]:
    if isinstance(messages, AsyncIterable):
        async for message in cast("AsyncIterable[BatchMessage]", messages):
            yield message
    else:
        for message in messages:
            yield message


async def publish_batch(
    js: JetStream,
    messages: Iterable[BatchMessage] | AsyncIterable[BatchMessage],
    *,
    ack_first: bool = True,
    ack_every: int | None = None,
    timeout: float = 5.0,
) -> BatchAck:
    """Publish all ``messages`` as one atomic batch.

    Both regular and async iterables are accepted. One message is buffered so
    the last input can carry the commit marker without adding a synthetic
    stored message.
    """
    publisher = batch_publish(js, ack_first=ack_first, ack_every=ack_every, timeout=timeout)
    last: BatchMessage | None = None
    async for message in _messages(messages):
        if not isinstance(message, BatchMessage):
            raise TypeError("messages must contain BatchMessage instances")
        if last is not None:
            await publisher.add(last.subject, last.data, headers=last.headers)
        last = message

    if last is None:
        raise EmptyBatchError("empty batch cannot be committed")
    return await publisher.commit(last.subject, last.data, headers=last.headers)
