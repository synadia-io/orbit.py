"""Fast-ingest, non-atomic JetStream batch publishing.

The wire protocol is ADR-50's fast-ingest protocol, available in nats-server
2.14 and later. Messages are stored as they arrive. A persistent inbox carries
server-selected flow-control acknowledgements, gap notifications, per-message
errors, and the final JetStream publish acknowledgement.

``FastPublisher`` is stateful and must be driven by one asyncio task at a time.
Create one publisher per task when publishing batches concurrently.
"""

from __future__ import annotations

import asyncio
import json
import math
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import TYPE_CHECKING, cast

from nats.client.message import Headers, Message
from nats.jetstream import PublishAck

from orbit.jetstreamext import JetStreamExtError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from typing import TypeVar

    from nats.client.subscription import Subscription
    from nats.jetstream import JetStream

    _T = TypeVar("_T")

__all__ = [
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
    "fast_publish",
]

_DEFAULT_FLOW = 100
_DEFAULT_MAX_OUTSTANDING_ACKS = 2
_DEFAULT_ACK_TIMEOUT = 5.0
_MAX_U16 = 2**16 - 1
_MAX_U64 = 2**64 - 1


class GapMode(StrEnum):
    """How the server handles missing batch sequence numbers."""

    OK = "ok"
    """Continue the batch and report gaps; some message loss is acceptable."""

    FAIL = "fail"
    """Abandon the batch on the first gap. This is the default."""


class _Operation(IntEnum):
    START = 0
    APPEND = 1
    COMMIT = 2
    COMMIT_EOB = 3
    PING = 4


class FastPublishError(JetStreamExtError):
    """Base class for fast-ingest publishing failures."""

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        error_code: int | None = None,
        description: str | None = None,
        batch_sequence: int | None = None,
        publish_ack: PublishAck | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.error_code = error_code
        self.description = description
        self.batch_sequence = batch_sequence
        self.publish_ack = publish_ack


class FastPublishConfigError(FastPublishError):
    """The publisher configuration or generated inbox is invalid."""


class FastPublishClosedError(FastPublishError):
    """The publisher has committed, closed, timed out, or failed fatally."""


class FastPublishEmptyBatchError(FastPublishError):
    """An empty publisher cannot be closed with an end-of-batch marker."""


class FastPublishTimeoutError(FastPublishError):
    """A flow acknowledgement or final publish acknowledgement timed out."""


class FastPublishSubscribeError(FastPublishError):
    """Creating the persistent inbox subscription failed."""


class FastPublishPublishError(FastPublishError):
    """Publishing a batch protocol message failed."""


class FastPublishResponseError(FastPublishError):
    """The server sent an invalid or unrecognized response."""


class FastPublishNotEnabledError(FastPublishError):
    """The target stream does not have ``allow_batched`` enabled."""


class FastPublishInvalidPatternError(FastPublishError):
    """The server rejected the fast-ingest reply subject pattern."""


class FastPublishInvalidBatchIdError(FastPublishError):
    """The server rejected the fast-ingest batch identifier."""


class FastPublishUnknownBatchIdError(FastPublishError):
    """The server no longer knows the fast-ingest batch identifier."""


class FastPublishTooManyInflightError(FastPublishError):
    """The server has too many fast-ingest batches in flight."""


class FastPublishGapError(FastPublishError):
    """The server detected one or more missing batch messages."""

    def __init__(self, expected_last_sequence: int, current_sequence: int) -> None:
        super().__init__(
            f"fast batch gap detected: expected sequence after {expected_last_sequence}, got {current_sequence}",
            batch_sequence=current_sequence,
        )
        self.expected_last_sequence = expected_last_sequence
        self.current_sequence = current_sequence


class FastPublishFlowError(FastPublishError):
    """The server rejected an individual fast-ingest message."""


@dataclass(frozen=True, slots=True)
class FastPubAck:
    """Progress returned after a successful :meth:`FastPublisher.add`."""

    batch_sequence: int
    """Sequence of the message within this fast-ingest batch."""

    ack_sequence: int
    """Highest batch sequence acknowledged by the server so far."""


@dataclass(frozen=True, slots=True)
class _FlowAck:
    sequence: int
    messages: int


@dataclass(frozen=True, slots=True)
class _FlowGap:
    expected_last_sequence: int
    current_sequence: int


@dataclass(frozen=True, slots=True)
class _FlowError:
    sequence: int
    error: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _TerminalAck:
    ack: PublishAck


@dataclass(frozen=True, slots=True)
class _InitialError:
    error: Mapping[str, object]


type _ServerEvent = _FlowAck | _FlowGap | _FlowError | _TerminalAck | _InitialError


def _integer(data: Mapping[str, object], key: str) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise FastPublishResponseError(f"fast publish response has invalid {key!r}: {value!r}")
    return value


def _positive_integer(data: Mapping[str, object], key: str, *, maximum: int = _MAX_U64) -> int:
    value = _integer(data, key)
    if value <= 0 or value > maximum:
        raise FastPublishResponseError(f"fast publish response has invalid {key!r}: {value!r}")
    return value


def _nonnegative_integer(data: Mapping[str, object], key: str) -> int:
    value = _integer(data, key)
    if value < 0 or value > _MAX_U64:
        raise FastPublishResponseError(f"fast publish response has invalid {key!r}: {value!r}")
    return value


def _classify(payload: bytes) -> _ServerEvent:
    try:
        decoded = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise FastPublishResponseError("fast publish response is not valid JSON") from exc

    if not isinstance(decoded, dict):
        raise FastPublishResponseError("fast publish response must be a JSON object")

    event_type = decoded.get("type")
    if event_type == "ack":
        return _FlowAck(
            sequence=_nonnegative_integer(decoded, "seq"),
            messages=_positive_integer(decoded, "msgs", maximum=_MAX_U16),
        )
    if event_type == "gap":
        return _FlowGap(
            expected_last_sequence=_nonnegative_integer(decoded, "last_seq"),
            current_sequence=_positive_integer(decoded, "seq"),
        )
    if event_type == "err":
        error = decoded.get("error")
        if not isinstance(error, dict):
            raise FastPublishResponseError("fast publish flow error is missing its API error")
        return _FlowError(sequence=_positive_integer(decoded, "seq"), error=error)
    if event_type is not None:
        raise FastPublishResponseError(f"unknown fast publish response type: {event_type!r}")

    error = decoded.get("error")
    if isinstance(error, dict):
        return _InitialError(error=error)

    stream = decoded.get("stream")
    sequence = decoded.get("seq")
    batch_id = decoded.get("batch")
    batch_size = decoded.get("count")
    if not isinstance(stream, str) or not stream:
        raise FastPublishResponseError("final fast publish acknowledgement has an invalid stream")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or not 0 <= sequence <= _MAX_U64:
        raise FastPublishResponseError("final fast publish acknowledgement has an invalid stream sequence")
    if not isinstance(batch_id, str) or not batch_id:
        raise FastPublishResponseError("final fast publish acknowledgement has an invalid batch id")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or not 0 <= batch_size <= _MAX_U64:
        raise FastPublishResponseError("final fast publish acknowledgement has an invalid batch count")

    return _TerminalAck(ack=PublishAck.from_response(decoded.copy()))


def _api_integer(error: Mapping[str, object], key: str) -> int | None:
    value = error.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _server_error(error: Mapping[str, object], *, batch_sequence: int | None = None) -> FastPublishError:
    code = _api_integer(error, "code")
    error_code = _api_integer(error, "err_code")
    raw_description = error.get("description")
    description = raw_description if isinstance(raw_description, str) else None

    # nats-server 2.14 error codes. orbit.go's older 10203..10206 constants
    # predate two newly assigned errors; orbit.rs and server 2.14 use these.
    error_types: dict[int, type[FastPublishError]] = {
        10205: FastPublishNotEnabledError,
        10206: FastPublishInvalidPatternError,
        10207: FastPublishInvalidBatchIdError,
        10208: FastPublishUnknownBatchIdError,
        10211: FastPublishTooManyInflightError,
    }
    error_type = error_types.get(error_code, FastPublishFlowError)
    message = description or "fast batch flow error"
    return error_type(
        message,
        code=code,
        error_code=error_code,
        description=description,
        batch_sequence=batch_sequence,
    )


def _reply_prefix(inbox: str, flow: int, gap_mode: GapMode) -> str:
    return f"{inbox}.{flow}.{gap_mode.value}."


def _reply(prefix: str, sequence: int, operation: _Operation) -> str:
    return f"{prefix}{sequence}.{int(operation)}.$FI"


def _should_stall(last_ack_sequence: int, effective_flow: int, max_outstanding_acks: int, next_sequence: int) -> bool:
    return last_ack_sequence + effective_flow * max_outstanding_acks <= next_sequence


class FastPublisher:
    """A non-atomic, high-throughput JetStream batch publisher.

    Messages are persisted immediately; :meth:`commit` and :meth:`close` end
    the batch and return its final :class:`nats.jetstream.PublishAck`.

    Args:
        js: JetStream context whose core client is used for the protocol.
        flow: Requested maximum number of messages between flow acks.
        max_outstanding_acks: Ack windows allowed in flight (``1..=3``).
        ack_timeout: Hard deadline, in seconds, for each wait cycle.
        gap_mode: Abandon on a gap (``fail``) or continue (``ok``).
        on_error: Fast synchronous callback for asynchronous gap/flow errors.

    The reply subject always retains the requested initial ``flow``. A lower
    flow selected by the server affects only the local outstanding-ack window,
    matching ADR-50 follower recovery behavior.
    """

    def __init__(
        self,
        js: JetStream,
        *,
        flow: int = _DEFAULT_FLOW,
        max_outstanding_acks: int = _DEFAULT_MAX_OUTSTANDING_ACKS,
        ack_timeout: float = _DEFAULT_ACK_TIMEOUT,
        gap_mode: GapMode | str = GapMode.FAIL,
        on_error: Callable[[FastPublishError], None] | None = None,
    ) -> None:
        if isinstance(flow, bool) or not isinstance(flow, int) or not 1 <= flow <= 65_535:
            raise FastPublishConfigError("flow must be between 1 and 65535")
        if (
            isinstance(max_outstanding_acks, bool)
            or not isinstance(max_outstanding_acks, int)
            or not 1 <= max_outstanding_acks <= 3
        ):
            raise FastPublishConfigError("max_outstanding_acks must be between 1 and 3")
        if (
            isinstance(ack_timeout, bool)
            or not isinstance(ack_timeout, (int, float))
            or not math.isfinite(ack_timeout)
            or ack_timeout <= 0
        ):
            raise FastPublishConfigError("ack_timeout must be greater than 0")
        try:
            resolved_gap_mode = GapMode(gap_mode)
        except ValueError as exc:
            raise FastPublishConfigError(f"invalid gap mode: {gap_mode!r}") from exc

        self._client = js.client
        self._inbox = self._client.new_inbox()
        parts = self._inbox.split(".")
        if len(parts) < 2 or not all(parts):
            raise FastPublishConfigError("fast publish inbox must contain at least two nonempty subject tokens")
        self._batch_id = parts[-1]

        self._flow = flow
        self._effective_flow = flow
        self._max_outstanding_acks = max_outstanding_acks
        self._ack_timeout = float(ack_timeout)
        self._gap_mode = resolved_gap_mode
        self._on_error = on_error
        self._reply_prefix = _reply_prefix(self._inbox, flow, resolved_gap_mode)

        self._subscription: Subscription | None = None
        self._sequence = 0
        self._message_count = 0
        self._last_ack_sequence = 0
        self._initial_ack_received = False
        self._pending_pub_ack: PublishAck | None = None
        self._first_subject: str | None = None
        self._closed = False
        self._fatal: FastPublishError | None = None
        self._fatal_needs_terminal_ack = False

    @property
    def size(self) -> int:
        """Messages successfully handed to the core client for publishing.

        A message rejected asynchronously by the server can still be included;
        a synchronous publish failure or cancellation is not.
        """

        return self._message_count

    @property
    def is_closed(self) -> bool:
        """Whether the publisher has ended or failed fatally."""

        return self._closed or self._fatal is not None

    @property
    def batch_id(self) -> str:
        """Identifier reported in the final JetStream publish ack."""

        return self._batch_id

    @property
    def inbox(self) -> str:
        """Persistent reply inbox owned by this publisher."""

        return self._inbox

    @property
    def gap_mode(self) -> GapMode:
        """Configured gap handling mode."""

        return self._gap_mode

    @property
    def last_ack_sequence(self) -> int:
        """Highest batch sequence acknowledged by the server."""

        return self._last_ack_sequence

    async def add(
        self,
        subject: str,
        data: bytes,
        *,
        headers: Headers | dict[str, str | list[str]] | None = None,
    ) -> FastPubAck:
        """Publish and immediately persist one message in the batch."""

        return await self._run_operation(
            self._add_message(Message(subject=subject, data=data, headers=_headers(headers)))
        )

    async def __aenter__(self) -> FastPublisher:
        """Enter an async context that owns this publisher's inbox."""

        await self._raise_if_unusable()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: object | None,
    ) -> None:
        """Release an unfinished publisher when leaving an async context."""

        await self.abort()

    def __del__(self) -> None:
        """Schedule best-effort inbox cleanup when an unfinished publisher is dropped.

        Finalization cannot be made reliable once the event loop is gone; callers
        that intentionally abandon a batch should use :meth:`abort`, :meth:`aclose`,
        or the async context-manager protocol.
        """

        subscription = getattr(self, "_subscription", None)
        if subscription is None or subscription.closed:
            return
        self._closed = True
        self._subscription = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        cleanup = loop.create_task(subscription.unsubscribe())
        cleanup.add_done_callback(_consume_task_exception)

    async def add_message(self, message: Message) -> FastPubAck:
        """Publish a pre-constructed message, replacing its reply subject."""

        return await self._run_operation(self._add_message(message))

    async def _add_message(self, message: Message) -> FastPubAck:
        await self._raise_if_unusable()
        await self._ensure_subscribed()
        await self._drain_ready()
        await self._raise_if_unusable()

        self._sequence += 1
        operation = _Operation.START if self._sequence == 1 else _Operation.APPEND
        if self._first_subject is None:
            self._first_subject = message.subject
        await self._publish(message, operation)
        self._message_count += 1

        if self._sequence == 1:
            await self._wait_for_first_reply()

        await self._drain_ready()
        await self._raise_if_unusable()

        # Gate after publishing, as orbit.go and ADR-50 do. The message at the
        # exact boundary is what triggers the flow ack; waiting before sending
        # it deadlocks when flow=1 and max_outstanding_acks=1.
        if _should_stall(
            self._last_ack_sequence,
            self._effective_flow,
            self._max_outstanding_acks,
            self._sequence,
        ):
            await self._wait_for_window(self._sequence)
            await self._raise_if_unusable()
        return FastPubAck(batch_sequence=self._sequence, ack_sequence=self._last_ack_sequence)

    async def commit(
        self,
        subject: str,
        data: bytes,
        *,
        headers: Headers | dict[str, str | list[str]] | None = None,
    ) -> PublishAck:
        """Store a final message and end the batch."""

        return await self._run_operation(
            self._commit_message(Message(subject=subject, data=data, headers=_headers(headers)), eob=False)
        )

    async def commit_message(self, message: Message) -> PublishAck:
        """Store a pre-constructed final message and end the batch."""

        return await self._run_operation(self._commit_message(message, eob=False))

    async def close(self) -> PublishAck:
        """End the batch without storing another message."""

        return await self._run_operation(self._close())

    async def abort(self) -> None:
        """Abandon this client-side publisher without sending a commit marker.

        Messages already accepted by the server remain stored. The server-side
        fast batch expires after its inactivity timeout.
        """

        await self._finish()

    async def aclose(self) -> None:
        """Alias for :meth:`abort`, suitable for generic async cleanup code."""

        await self.abort()

    async def _close(self) -> PublishAck:
        await self._raise_if_unusable()
        if self._sequence == 0 or self._first_subject is None:
            raise FastPublishEmptyBatchError("cannot close an empty fast publish batch")
        message = Message(subject=self._first_subject, data=b"")
        return await self._commit_message(message, eob=True)

    async def _commit_message(self, message: Message, *, eob: bool) -> PublishAck:
        await self._raise_if_unusable()
        await self._ensure_subscribed()
        await self._drain_ready()
        await self._raise_fatal()

        # A commit is terminal and its final pub ack closes the outstanding
        # window, so it is sent immediately (matching orbit.go).
        self._sequence += 1
        operation = _Operation.COMMIT_EOB if eob else _Operation.COMMIT
        if self._first_subject is None:
            self._first_subject = message.subject

        try:
            await self._publish(message, operation)
            if not eob:
                self._message_count += 1
            self._closed = True
            ack = await self._wait_for_pub_ack(expected_count=self._message_count)
        except BaseException:
            await self._finish(preserve_exception=True)
            raise
        else:
            await self._finish()
            return ack

    async def _run_operation(self, operation: Awaitable[_T]) -> _T:
        try:
            return await operation
        except FastPublishEmptyBatchError:
            # No protocol operation was attempted, so the publisher remains
            # usable (matching orbit.go's empty-close behavior).
            raise
        except BaseException:
            # Preserve the exact cancellation/callback exception. Cleanup is
            # shielded so a second cancellation cannot leave the inbox live.
            await self._finish(preserve_exception=True)
            raise

    async def _raise_if_unusable(self) -> None:
        await self._raise_fatal()
        if self._closed:
            raise FastPublishClosedError("fast publisher is closed")

    async def _raise_fatal(self) -> None:
        if self._fatal is not None:
            error = self._fatal
            if self._fatal_needs_terminal_ack:
                error.publish_ack = await self._collect_terminal_ack_best_effort()
                self._fatal_needs_terminal_ack = False
            await self._finish(preserve_exception=True)
            raise error

    async def _ensure_subscribed(self) -> None:
        if self._subscription is not None:
            return
        try:
            self._subscription = await self._client.subscribe(f"{self._inbox}.>")
        except Exception as exc:
            self._closed = True
            raise FastPublishSubscribeError("failed to subscribe to fast publish inbox") from exc

    async def _publish(self, message: Message, operation: _Operation) -> None:
        reply = _reply(self._reply_prefix, self._sequence, operation)
        try:
            await self._client.publish(
                message.subject,
                message.data,
                reply=reply,
                headers=message.headers,
            )
        except Exception as exc:
            await self._finish(preserve_exception=True)
            raise FastPublishPublishError(
                f"failed to publish fast batch message {self._sequence}",
                batch_sequence=self._sequence,
            ) from exc

    async def _drain_ready(self) -> None:
        subscription = self._subscription
        if subscription is None:
            return
        while subscription.pending[0] > 0:
            try:
                self._handle_event(_classify((await subscription.next()).data))
            except FastPublishError as error:
                self._fatal = error
                await self._finish(preserve_exception=True)
                raise

    def _handle_event(self, event: _ServerEvent) -> None:
        if isinstance(event, _FlowAck):
            self._initial_ack_received = True
            self._last_ack_sequence = max(self._last_ack_sequence, event.sequence)
            self._effective_flow = max(1, event.messages)
            return
        if isinstance(event, _FlowGap):
            error = FastPublishGapError(event.expected_last_sequence, event.current_sequence)
            if self._gap_mode is GapMode.FAIL and self._fatal is None:
                self._fatal = error
                self._fatal_needs_terminal_ack = True
            self._notify(error)
            return
        if isinstance(event, _FlowError):
            error = _server_error(event.error, batch_sequence=event.sequence)
            if self._gap_mode is GapMode.FAIL and self._fatal is None:
                self._fatal = error
                self._fatal_needs_terminal_ack = True
            self._notify(error)
            return
        if isinstance(event, _TerminalAck):
            self._pending_pub_ack = event.ack
            return
        self._fatal = _server_error(event.error)

    def _notify(self, error: FastPublishError) -> None:
        if self._on_error is not None:
            self._on_error(error)

    async def _wait_for_first_reply(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._ack_timeout
        while not self._initial_ack_received and self._pending_pub_ack is None:
            await self._wait_for_event(deadline)
            await self._raise_fatal()

    async def _collect_terminal_ack_best_effort(self) -> PublishAck | None:
        """Collect the terminal ack that follows a fail-mode flow event.

        The original typed gap/flow error always wins. A malformed response,
        closed subscription, or timeout merely leaves ``publish_ack`` unset;
        cancellation and other ``BaseException`` instances still propagate.
        """

        if self._pending_pub_ack is not None:
            ack, self._pending_pub_ack = self._pending_pub_ack, None
            try:
                return self._validate_terminal_ack(ack, allow_partial=True)
            except FastPublishResponseError:
                return None

        subscription = self._subscription
        if subscription is None:
            return None

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._ack_timeout
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                return None
            try:
                message = await subscription.next(remaining)
            except (TimeoutError, RuntimeError):
                return None

            try:
                event = _classify(message.data)
            except FastPublishError:
                return None

            if isinstance(event, _TerminalAck):
                try:
                    return self._validate_terminal_ack(event.ack, allow_partial=True)
                except FastPublishResponseError:
                    return None
            if isinstance(event, _FlowAck):
                self._last_ack_sequence = max(self._last_ack_sequence, event.sequence)
                self._effective_flow = max(1, event.messages)
            elif isinstance(event, _InitialError):
                return None

    async def _wait_for_window(self, next_sequence: int) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._ack_timeout
        ping_interval = max(self._ack_timeout / 3, 0.1)
        ping_at = loop.time() + ping_interval

        while _should_stall(
            self._last_ack_sequence,
            self._effective_flow,
            self._max_outstanding_acks,
            next_sequence,
        ):
            await self._wait_for_event(deadline, wake_at=ping_at)
            await self._raise_fatal()
            now = loop.time()
            if now >= ping_at and _should_stall(
                self._last_ack_sequence,
                self._effective_flow,
                self._max_outstanding_acks,
                next_sequence,
            ):
                await self._send_ping()
                ping_at = loop.time() + ping_interval

    async def _wait_for_pub_ack(self, *, expected_count: int | None = None) -> PublishAck:
        # Commit-time pings solicit flow progress/liveness only. nats-server
        # 2.14 does not replay a lost terminal PubAck in response to a ping.
        if self._pending_pub_ack is not None:
            ack, self._pending_pub_ack = self._pending_pub_ack, None
            return self._validate_terminal_ack(ack, expected_count=expected_count)

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._ack_timeout
        ping_interval = max(self._ack_timeout / 3, 0.1)
        ping_at = loop.time() + ping_interval
        while True:
            await self._wait_for_event(deadline, wake_at=ping_at)
            await self._raise_fatal()
            if self._pending_pub_ack is not None:
                break
            if loop.time() >= ping_at:
                await self._send_ping()
                ping_at = loop.time() + ping_interval

        ack, self._pending_pub_ack = self._pending_pub_ack, None
        assert ack is not None
        return self._validate_terminal_ack(ack, expected_count=expected_count)

    def _validate_terminal_ack(
        self,
        ack: PublishAck,
        *,
        expected_count: int | None = None,
        allow_partial: bool = False,
    ) -> PublishAck:
        """Validate that a terminal PubAck belongs to this publisher."""

        if ack.batch_id != self._batch_id:
            raise FastPublishResponseError(
                f"final fast publish acknowledgement has batch id {ack.batch_id!r}, expected {self._batch_id!r}"
            )
        count = cast(int, ack.batch_size)
        sequence = cast(int, ack.sequence)
        if sequence == 0 and not (allow_partial and count == 0):
            raise FastPublishResponseError("final fast publish acknowledgement has an invalid stream sequence")
        if allow_partial:
            if count > self._message_count:
                raise FastPublishResponseError(
                    f"final fast publish acknowledgement count {count} exceeds published count {self._message_count}"
                )
        elif expected_count is not None and count != expected_count:
            raise FastPublishResponseError(
                f"final fast publish acknowledgement count {count} does not match published count {expected_count}"
            )
        return ack

    async def _wait_for_event(self, deadline: float, *, wake_at: float | None = None) -> None:
        loop = asyncio.get_running_loop()
        now = loop.time()
        if now >= deadline:
            await self._timeout()
        next_wake = min(deadline, wake_at) if wake_at is not None else deadline
        subscription = self._subscription
        if subscription is None:
            raise FastPublishClosedError("fast publish inbox subscription is closed")
        try:
            message = await subscription.next(max(0, next_wake - now))
        except TimeoutError:
            if loop.time() >= deadline:
                await self._timeout()
            return
        except RuntimeError as exc:
            await self._finish(preserve_exception=True)
            raise FastPublishClosedError("fast publish inbox subscription ended") from exc
        try:
            self._handle_event(_classify(message.data))
        except FastPublishError as error:
            self._fatal = error
            await self._finish(preserve_exception=True)
            raise

    async def _send_ping(self) -> None:
        if self._first_subject is None:
            await self._finish()
            raise FastPublishResponseError("cannot ping a fast batch before its first message")
        reply = _reply(self._reply_prefix, self._sequence, _Operation.PING)
        try:
            await self._client.publish(self._first_subject, b"", reply=reply)
        except Exception as exc:
            await self._finish(preserve_exception=True)
            raise FastPublishPublishError("failed to ping fast publish batch") from exc

    async def _timeout(self) -> None:
        await self._finish()
        raise FastPublishTimeoutError("timed out waiting for fast publish acknowledgement")

    async def _finish(self, *, preserve_exception: bool = False) -> None:
        self._closed = True
        subscription, self._subscription = self._subscription, None
        if subscription is not None and not subscription.closed:
            cleanup = asyncio.create_task(subscription.unsubscribe())
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                cleanup.add_done_callback(_consume_task_exception)
                if not preserve_exception:
                    raise
            except BaseException:
                # Transport teardown is best-effort and must not replace the
                # operation outcome that made this publisher terminal.
                pass


def _headers(headers: Headers | dict[str, str | list[str]] | None) -> Headers | None:
    if headers is None or isinstance(headers, Headers):
        return headers
    return Headers(headers)


def _consume_task_exception(task: asyncio.Task[None]) -> None:
    """Retrieve a finalizer cleanup exception so asyncio does not log it."""

    if not task.cancelled():
        task.exception()


def fast_publish(
    js: JetStream,
    *,
    flow: int = _DEFAULT_FLOW,
    max_outstanding_acks: int = _DEFAULT_MAX_OUTSTANDING_ACKS,
    ack_timeout: float = _DEFAULT_ACK_TIMEOUT,
    gap_mode: GapMode | str = GapMode.FAIL,
    on_error: Callable[[FastPublishError], None] | None = None,
) -> FastPublisher:
    """Create a fast-ingest publisher bound to ``js``.

    This factory mirrors the package's ``get_batch(js, ...)`` style while the
    returned object owns the state for one batch.
    """

    return FastPublisher(
        js,
        flow=flow,
        max_outstanding_acks=max_outstanding_acks,
        ack_timeout=ack_timeout,
        gap_mode=gap_mode,
        on_error=on_error,
    )
