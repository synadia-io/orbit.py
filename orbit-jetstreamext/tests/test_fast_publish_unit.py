"""Unit tests for fast-ingest reply parsing and flow control."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from nats.client.message import Message

from orbit.jetstreamext import (
    FastPublishClosedError,
    FastPublishConfigError,
    FastPublisher,
    FastPublishError,
    FastPublishFlowError,
    FastPublishGapError,
    FastPublishNotEnabledError,
    FastPublishPublishError,
    FastPublishTimeoutError,
    FastPublishUnknownBatchIdError,
    GapMode,
)
from orbit.jetstreamext.fast_publish import (
    _classify,
    _FlowAck,
    _FlowError,
    _FlowGap,
    _InitialError,
    _Operation,
    _reply,
    _reply_prefix,
    _server_error,
    _should_stall,
    _TerminalAck,
)


class _FakeClient:
    def __init__(self, inbox: str = "_INBOX.abc123") -> None:
        self.inbox = inbox

    def new_inbox(self) -> str:
        return self.inbox


class _FakeJetStream:
    def __init__(self, inbox: str = "_INBOX.abc123") -> None:
        self.client = _FakeClient(inbox)


class _PingSubscription:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[Message] = asyncio.Queue()
        self.closed = False

    @property
    def pending(self) -> tuple[int, int]:
        return self.queue.qsize(), 0

    async def next(self, timeout: float | None = None) -> Message:
        return await asyncio.wait_for(self.queue.get(), timeout)

    async def unsubscribe(self) -> None:
        self.closed = True


class _PingClient(_FakeClient):
    def __init__(self) -> None:
        super().__init__()
        self.subscription = _PingSubscription()
        self.replies: list[str] = []

    async def publish(self, subject: str, data: bytes, *, reply: str, headers=None) -> None:
        self.replies.append(reply)
        if reply.endswith(".4.$FI"):
            await self.subscription.queue.put(Message("_INBOX.abc123", b'{"type":"ack","seq":1,"msgs":1}'))


class _PingJetStream:
    def __init__(self) -> None:
        self.client = _PingClient()


class _OperationSubscription(_PingSubscription):
    def __init__(self, next_errors: list[BaseException] | None = None) -> None:
        super().__init__()
        self.next_errors = next_errors or []

    async def next(self, timeout: float | None = None) -> Message:
        if self.next_errors:
            raise self.next_errors.pop(0)
        return await super().next(timeout)


class _OperationClient(_FakeClient):
    def __init__(
        self,
        actions: list[bytes | BaseException | None],
        *,
        next_errors: list[BaseException] | None = None,
    ) -> None:
        super().__init__()
        self.actions = actions
        self.subscription = _OperationSubscription(next_errors)

    async def subscribe(self, subject: str) -> _OperationSubscription:
        return self.subscription

    async def publish(self, subject: str, data: bytes, *, reply: str, headers=None) -> None:
        action = self.actions.pop(0) if self.actions else None
        if isinstance(action, BaseException):
            raise action
        if isinstance(action, bytes):
            await self.subscription.queue.put(Message("_INBOX.abc123", action))


class _OperationJetStream:
    def __init__(
        self,
        actions: list[bytes | BaseException | None],
        *,
        next_errors: list[BaseException] | None = None,
    ) -> None:
        self.client = _OperationClient(actions, next_errors=next_errors)


class _CallbackAbort(BaseException):
    pass


def _publisher(js: object, **kwargs: Any) -> FastPublisher:
    return FastPublisher(cast(Any, js), **kwargs)


def test_reply_subject_contains_protocol_fields() -> None:
    prefix = _reply_prefix("_INBOX.abc", 50, GapMode.FAIL)
    assert prefix == "_INBOX.abc.50.fail."
    assert _reply(prefix, 42, _Operation.START) == "_INBOX.abc.50.fail.42.0.$FI"
    assert _reply(prefix, 42, _Operation.APPEND) == "_INBOX.abc.50.fail.42.1.$FI"
    assert _reply(prefix, 42, _Operation.COMMIT) == "_INBOX.abc.50.fail.42.2.$FI"
    assert _reply(prefix, 42, _Operation.COMMIT_EOB) == "_INBOX.abc.50.fail.42.3.$FI"
    assert _reply(prefix, 42, _Operation.PING) == "_INBOX.abc.50.fail.42.4.$FI"


def test_stall_gate_is_inclusive_at_window_boundary() -> None:
    assert not _should_stall(0, 10, 2, 19)
    assert _should_stall(0, 10, 2, 20)
    assert _should_stall(10, 10, 2, 30)
    assert not _should_stall(10, 10, 2, 29)


def test_classify_flow_ack() -> None:
    assert _classify(b'{"type":"ack","seq":10,"msgs":15}') == _FlowAck(10, 15)


def test_classify_gap() -> None:
    assert _classify(b'{"type":"gap","last_seq":10,"seq":15}') == _FlowGap(10, 15)


def test_classify_flow_error() -> None:
    event = _classify(b'{"type":"err","seq":7,"error":{"code":400,"err_code":10071,"description":"wrong sequence"}}')
    assert isinstance(event, _FlowError)
    assert event.sequence == 7
    assert event.error["err_code"] == 10071


def test_classify_terminal_ack_uses_nats_primitive() -> None:
    event = _classify(b'{"stream":"TEST","seq":42,"batch":"_INBOX.abc","count":10}')
    assert isinstance(event, _TerminalAck)
    assert event.ack.stream == "TEST"
    assert event.ack.sequence == 42
    assert event.ack.batch_id == "_INBOX.abc"
    assert event.ack.batch_size == 10


def test_classify_initial_error() -> None:
    event = _classify(b'{"error":{"code":400,"err_code":10205,"description":"fast batch publish not enabled"}}')
    assert isinstance(event, _InitialError)
    assert isinstance(_server_error(event.error), FastPublishNotEnabledError)


def test_server_error_retains_api_details() -> None:
    error = _server_error(
        {"code": 400, "err_code": 10071, "description": "wrong last sequence"},
        batch_sequence=3,
    )
    assert isinstance(error, FastPublishFlowError)
    assert error.code == 400
    assert error.error_code == 10071
    assert error.batch_sequence == 3


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"flow": 0}, "flow"),
        ({"flow": 1.5}, "flow"),
        ({"flow": True}, "flow"),
        ({"flow": 65_536}, "flow"),
        ({"max_outstanding_acks": 0}, "max_outstanding_acks"),
        ({"max_outstanding_acks": 1.5}, "max_outstanding_acks"),
        ({"max_outstanding_acks": True}, "max_outstanding_acks"),
        ({"max_outstanding_acks": 4}, "max_outstanding_acks"),
        ({"ack_timeout": 0}, "ack_timeout"),
        ({"ack_timeout": True}, "ack_timeout"),
        ({"ack_timeout": float("nan")}, "ack_timeout"),
        ({"ack_timeout": float("inf")}, "ack_timeout"),
        ({"ack_timeout": "five"}, "ack_timeout"),
        ({"gap_mode": "sometimes"}, "gap mode"),
    ],
)
def test_invalid_configuration(kwargs: dict[str, Any], message: str) -> None:
    with pytest.raises(FastPublishConfigError, match=message):
        _publisher(_FakeJetStream(), **kwargs)


def test_custom_multi_token_inbox_uses_final_token_as_batch_id() -> None:
    publisher = _publisher(_FakeJetStream("_INBOX.application.abc"))
    assert publisher.inbox == "_INBOX.application.abc"
    assert publisher.batch_id == "abc"


@pytest.mark.parametrize("inbox", ["one-token", "_INBOX..abc", ".abc"])
def test_inbox_requires_two_or_more_nonempty_tokens(inbox: str) -> None:
    with pytest.raises(FastPublishConfigError, match="at least two nonempty"):
        _publisher(_FakeJetStream(inbox))


def test_flow_ack_changes_window_but_not_cached_reply_prefix() -> None:
    publisher = _publisher(_FakeJetStream(), flow=10)
    original_prefix = publisher._reply_prefix
    publisher._handle_event(_FlowAck(sequence=5, messages=1))
    assert publisher.last_ack_sequence == 5
    assert publisher._effective_flow == 1
    assert publisher._reply_prefix == original_prefix


def test_gap_fail_is_fatal_and_typed() -> None:
    observed = []
    publisher = _publisher(_FakeJetStream(), on_error=observed.append)
    publisher._handle_event(_FlowGap(expected_last_sequence=3, current_sequence=5))
    assert publisher.is_closed
    assert isinstance(publisher._fatal, FastPublishGapError)
    assert observed == [publisher._fatal]


def test_gap_ok_notifies_and_continues() -> None:
    observed = []
    publisher = _publisher(
        _FakeJetStream(),
        gap_mode=GapMode.OK,
        on_error=observed.append,
    )
    publisher._handle_event(_FlowGap(expected_last_sequence=3, current_sequence=5))
    assert not publisher.is_closed
    assert publisher._fatal is None
    assert isinstance(observed[0], FastPublishGapError)


async def test_ping_recovers_lost_flow_ack() -> None:
    js = _PingJetStream()
    publisher = _publisher(js, flow=1, max_outstanding_acks=1, ack_timeout=0.2)
    publisher._subscription = cast(Any, js.client.subscription)
    publisher._first_subject = "test.msg"
    publisher._sequence = 1

    await publisher._wait_for_window(1)

    assert publisher.last_ack_sequence == 1
    assert js.client.replies[-1].endswith(".1.4.$FI")


async def test_pending_commit_ping_reports_liveness_before_final_ack() -> None:
    js = _PingJetStream()
    publisher = _publisher(js, ack_timeout=0.3)
    publisher._subscription = cast(Any, js.client.subscription)
    publisher._first_subject = "test.msg"
    publisher._sequence = 1

    async def deliver_terminal_ack() -> None:
        await asyncio.sleep(0.15)
        await js.client.subscription.queue.put(
            Message("_INBOX.abc123", b'{"stream":"TEST","seq":1,"batch":"abc123","count":1}')
        )

    delivery = asyncio.create_task(deliver_terminal_ack())
    ack = await publisher._wait_for_pub_ack()
    await delivery

    assert ack.batch_size == 1
    assert js.client.replies[-1].endswith(".1.4.$FI")


async def test_commit_ping_times_out_if_server_only_reports_flow_progress() -> None:
    js = _PingJetStream()
    publisher = _publisher(js, ack_timeout=0.2)
    publisher._subscription = cast(Any, js.client.subscription)
    publisher._first_subject = "test.msg"
    publisher._sequence = 1

    with pytest.raises(FastPublishTimeoutError):
        await publisher._wait_for_pub_ack()

    assert js.client.replies
    assert js.client.subscription.closed


async def test_commit_ping_can_report_unknown_batch_after_lost_terminal_ack() -> None:
    js = _OperationJetStream(
        [
            b'{"type":"ack","seq":1,"msgs":100}',
            None,
            b'{"error":{"code":400,"err_code":10208,"description":"unknown batch id"}}',
        ]
    )
    publisher = _publisher(js, ack_timeout=0.2)

    await publisher.add("test.msg", b"one")
    with pytest.raises(FastPublishUnknownBatchIdError):
        await publisher.commit("test.msg", b"two")

    assert js.client.subscription.closed


async def test_fatal_event_from_ready_drain_closes_subscription() -> None:
    js = _PingJetStream()
    publisher = _publisher(js)
    subscription = js.client.subscription
    publisher._subscription = cast(Any, subscription)
    await subscription.queue.put(Message("_INBOX.abc123", b'{"type":"gap","last_seq":1,"seq":3}'))
    await subscription.queue.put(Message("_INBOX.abc123", b'{"stream":"TEST","seq":1,"batch":"abc123","count":1}'))

    await publisher._drain_ready()
    with pytest.raises(FastPublishGapError) as raised:
        await publisher._raise_if_unusable()

    assert raised.value.publish_ack is not None
    assert raised.value.publish_ack.batch_size == 1
    assert subscription.closed
    assert publisher._subscription is None


async def test_fatal_event_while_stalled_closes_subscription() -> None:
    js = _PingJetStream()
    publisher = _publisher(js, flow=1, max_outstanding_acks=1)
    subscription = js.client.subscription
    publisher._subscription = cast(Any, subscription)
    publisher._first_subject = "test.msg"
    publisher._sequence = 1
    await subscription.queue.put(
        Message(
            "_INBOX.abc123",
            b'{"type":"err","seq":1,"error":{"code":400,"err_code":10071,"description":"bad"}}',
        )
    )
    await subscription.queue.put(Message("_INBOX.abc123", b'{"stream":"TEST","seq":1,"batch":"abc123","count":1}'))

    with pytest.raises(FastPublishFlowError) as raised:
        await publisher._wait_for_window(1)

    assert raised.value.publish_ack is not None
    assert subscription.closed
    assert publisher._subscription is None


async def test_add_publish_cancellation_is_terminal_and_preserves_size() -> None:
    cancellation = asyncio.CancelledError("publish cancelled")
    js = _OperationJetStream([cancellation])
    publisher = _publisher(js)

    with pytest.raises(asyncio.CancelledError) as raised:
        await publisher.add("test.msg", b"data")

    assert raised.value is cancellation
    assert publisher.size == 0
    assert publisher.is_closed
    assert js.client.subscription.closed
    with pytest.raises(FastPublishClosedError):
        await publisher.add("test.again", b"data")


async def test_add_publish_failure_is_terminal_and_preserves_size() -> None:
    js = _OperationJetStream([RuntimeError("publish failed")])
    publisher = _publisher(js)

    with pytest.raises(FastPublishPublishError):
        await publisher.add("test.msg", b"data")

    assert publisher.size == 0
    assert publisher.is_closed
    assert js.client.subscription.closed


async def test_first_ack_cancellation_is_terminal_after_handoff() -> None:
    cancellation = asyncio.CancelledError("ack wait cancelled")
    js = _OperationJetStream([None], next_errors=[cancellation])
    publisher = _publisher(js)

    with pytest.raises(asyncio.CancelledError) as raised:
        await publisher.add("test.msg", b"data")

    assert raised.value is cancellation
    assert publisher.size == 1
    assert publisher.is_closed
    assert js.client.subscription.closed


async def test_commit_publish_cancellation_does_not_increment_size() -> None:
    cancellation = asyncio.CancelledError("commit cancelled")
    initial_ack = b'{"type":"ack","seq":1,"msgs":100}'
    js = _OperationJetStream([initial_ack, cancellation])
    publisher = _publisher(js)
    await publisher.add("test.first", b"first")

    with pytest.raises(asyncio.CancelledError) as raised:
        await publisher.commit("test.final", b"final")

    assert raised.value is cancellation
    assert publisher.size == 1
    assert publisher.is_closed
    assert js.client.subscription.closed


@pytest.mark.parametrize("gap_mode", [GapMode.FAIL, GapMode.OK])
async def test_callback_base_exception_is_terminal_and_propagates(gap_mode: GapMode) -> None:
    callback_abort = _CallbackAbort("callback aborted")
    js = _OperationJetStream([])
    publisher: FastPublisher

    def callback(error: FastPublishError) -> None:
        if gap_mode is GapMode.FAIL:
            assert publisher._fatal is error
        else:
            assert publisher._fatal is None
        raise callback_abort

    publisher = _publisher(js, gap_mode=gap_mode, on_error=callback)
    publisher._subscription = cast(Any, js.client.subscription)
    await js.client.subscription.queue.put(Message("_INBOX.abc123", b'{"type":"gap","last_seq":1,"seq":3}'))

    with pytest.raises(_CallbackAbort) as raised:
        await publisher.add("test.msg", b"data")

    assert raised.value is callback_abort
    assert publisher.is_closed
    assert js.client.subscription.closed
    assert publisher._subscription is None
