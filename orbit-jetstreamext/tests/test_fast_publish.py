"""Fast-ingest tests driven exclusively through a live nats-server.

Malformed and otherwise impractical server responses are controlled inputs,
but they are always published through Core NATS and consumed from the real
persistent inbox subscription; no response bypasses the transport.
"""

from __future__ import annotations

import asyncio
import gc
import json
import weakref
from typing import TYPE_CHECKING, Any

import pytest
from nats.client import connect
from nats.client.message import Headers, Message
from nats.jetstream import new as new_jetstream

from orbit.jetstreamext import (
    FastPublishClosedError,
    FastPublishConfigError,
    FastPublishEmptyBatchError,
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
from orbit.jetstreamext.fast_publish import _consume_task_exception, _Operation, _reply

if TYPE_CHECKING:
    from collections.abc import Callable

    from nats.jetstream import JetStream, Stream


async def _create_fast_stream(
    js: JetStream,
    name: str,
    subjects: list[str],
    **config: Any,
) -> Stream:
    return await js.create_stream(name=name, subjects=subjects, allow_batched=True, **config)


def _subscription_count(js: JetStream) -> int:
    return len(js.client._subscriptions)


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def _wait_for_inbox_message(publisher: Any) -> None:
    await _wait_until(lambda: publisher._subscription is not None and publisher._subscription.pending[0] > 0)


async def _inject(js: JetStream, publisher: Any, payload: bytes) -> None:
    await js.client.publish(f"{publisher.inbox}.injected", payload)
    await js.client.flush()
    await _wait_for_inbox_message(publisher)


async def _publish_inbox_payload(js: JetStream, publisher: Any, payload: bytes) -> None:
    await js.client.publish(f"{publisher.inbox}.injected", payload)
    await js.client.flush()


async def test_add_commit_message_headers_and_state(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_BASIC", ["fp.basic.>"])
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream)

    first = await publisher.add("fp.basic.1", b"one", headers={"X-Source": "add"})
    second = await publisher.add_message(Message("fp.basic.2", b"two", headers=Headers({"X-Source": "message"})))
    ack = await publisher.commit_message(Message("fp.basic.3", b"three", headers=Headers({"X-Source": "commit"})))

    assert first.batch_sequence == 1
    assert second.batch_sequence == 2
    assert first.ack_sequence <= first.batch_sequence
    assert second.ack_sequence <= second.batch_sequence
    assert ack.stream == "FP_BASIC"
    assert ack.batch_id == publisher.batch_id
    assert ack.batch_size == 3
    assert publisher.size == 3
    assert publisher.is_closed
    assert publisher.gap_mode is GapMode.FAIL
    assert _subscription_count(fast_jetstream) == baseline
    assert (await stream.get_info()).state.messages == 3
    assert (await stream.get_message(1)).headers == Headers({"X-Source": "add"})
    assert (await stream.get_message(2)).headers == Headers({"X-Source": "message"})
    assert (await stream.get_message(3)).headers == Headers({"X-Source": "commit"})

    with pytest.raises(FastPublishClosedError):
        await publisher.add("fp.basic.4", b"four")


async def test_wire_operations_and_requested_prefix_are_stable(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(fast_jetstream, "FP_WIRE", ["fp.wire.>"])
    observer = await fast_jetstream.client.subscribe("fp.wire.>")
    await fast_jetstream.client.flush()
    publisher = fast_publish(fast_jetstream, flow=50, gap_mode=GapMode.FAIL)

    await publisher.add("fp.wire.msg", b"one")
    start = await observer.next(2)
    await publisher.add("fp.wire.msg", b"two")
    append = await observer.next(2)
    await publisher._send_ping()
    ping = await observer.next(2)
    ack = await publisher.commit("fp.wire.msg", b"three")
    commit = await observer.next(2)

    prefix = f"{publisher.inbox}.50.fail."
    assert start.reply == f"{prefix}1.0.$FI"
    assert append.reply == f"{prefix}2.1.$FI"
    assert ping.reply == f"{prefix}2.4.$FI"
    assert commit.reply == f"{prefix}3.2.$FI"
    assert ack.batch_size == 3
    await observer.unsubscribe()


async def test_close_uses_eob_and_does_not_store_it(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_CLOSE", ["fp.close.>"])
    observer = await fast_jetstream.client.subscribe("fp.close.>")
    await fast_jetstream.client.flush()
    publisher = fast_publish(fast_jetstream)
    await publisher.add("fp.close.msg", b"one")
    await observer.next(2)
    await publisher.add("fp.close.msg", b"two")
    await observer.next(2)

    ack = await publisher.close()
    eob = await observer.next(2)

    assert eob.reply == f"{publisher.inbox}.100.fail.3.3.$FI"
    assert ack.batch_size == 2
    assert publisher.size == 2
    assert (await stream.get_info()).state.messages == 2
    await observer.unsubscribe()


async def test_empty_close_errors_without_poisoning_publisher(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(fast_jetstream, "FP_EMPTY", ["fp.empty.>"])
    publisher = fast_publish(fast_jetstream)
    with pytest.raises(FastPublishEmptyBatchError):
        await publisher.close()
    assert not publisher.is_closed
    assert (await publisher.commit("fp.empty.one", b"one")).batch_size == 1


async def test_exact_boundary_ack_sequence_matches_go_behavior(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(fast_jetstream, "FP_BOUNDARY", ["fp.boundary.>"])
    publisher = fast_publish(fast_jetstream, flow=1, max_outstanding_acks=1)

    for sequence in range(1, 41):
        progress = await publisher.add("fp.boundary.msg", str(sequence).encode())
        assert progress.batch_sequence == sequence
        assert progress.ack_sequence >= progress.batch_sequence

    assert (await publisher.close()).batch_size == 40


async def test_maximum_window_three_and_custom_flow(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_WINDOW", ["fp.window.>"])
    publisher = fast_publish(fast_jetstream, flow=50, max_outstanding_acks=3)
    for sequence in range(300):
        await publisher.add("fp.window.msg", str(sequence).encode())

    ack = await publisher.close()
    assert ack.batch_size == 300
    assert publisher.last_ack_sequence > 0
    assert (await stream.get_info()).state.messages == 300


async def test_server_selected_flow_does_not_rewrite_wire_prefix(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(fast_jetstream, "FP_DYNAMIC", ["fp.dynamic.>"])
    observer = await fast_jetstream.client.subscribe("fp.dynamic.>")
    publisher = fast_publish(fast_jetstream, flow=10, max_outstanding_acks=3)
    inbox_observer = await fast_jetstream.client.subscribe(f"{publisher.inbox}.>")
    await fast_jetstream.client.flush()

    await publisher.add("fp.dynamic.msg", b"one")
    initial = json.loads((await inbox_observer.next(2)).data)
    initial["msgs"] = 1
    await fast_jetstream.client.publish(
        f"{publisher.inbox}.selected-flow",
        json.dumps(initial).encode(),
    )
    await fast_jetstream.client.flush()
    await publisher.add("fp.dynamic.msg", b"2")
    assert publisher._effective_flow == 1
    for sequence in range(3, 12):
        await publisher.add("fp.dynamic.msg", str(sequence).encode())
    ack = await publisher.close()

    replies = [(await observer.next(2)).reply for _ in range(12)]
    assert all(reply is not None and reply.startswith(f"{publisher.inbox}.10.fail.") for reply in replies)
    assert ack.batch_size == 11
    await observer.unsubscribe()
    await inbox_observer.unsubscribe()


async def test_sustained_ten_thousand_message_batch(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_LARGE", ["fp.large.>"])
    publisher = fast_publish(fast_jetstream, flow=100, max_outstanding_acks=2, ack_timeout=10)
    for sequence in range(10_000):
        await publisher.add("fp.large.msg", str(sequence).encode())

    ack = await publisher.close()
    assert ack.batch_size == 10_000
    assert publisher.size == 10_000
    assert (await stream.get_info()).state.messages == 10_000


async def test_gap_ok_flow_error_notifies_and_continues(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_OK_ERROR", ["fp.ok.error.>"])
    observed: list[FastPublishError] = []
    publisher = fast_publish(
        fast_jetstream,
        gap_mode=GapMode.OK,
        on_error=observed.append,
    )
    await publisher.add("fp.ok.error.good", b"one")
    await publisher.add(
        "fp.ok.error.bad",
        b"bad",
        headers={"Nats-Expected-Last-Sequence": "99"},
    )
    ack = await publisher.commit("fp.ok.error.good", b"two")

    assert len(observed) == 1
    assert isinstance(observed[0], FastPublishFlowError)
    assert observed[0].error_code == 10071
    assert ack.batch_size == 3
    assert publisher.size == 3
    assert (await stream.get_info()).state.messages == 2


async def test_real_gap_fail_is_typed_and_retains_partial_ack(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_GAP_FAIL", ["fp.gap.fail.>"])
    observed: list[FastPublishError] = []
    publisher = fast_publish(fast_jetstream, flow=1, on_error=observed.append)
    await publisher.add("fp.gap.fail.msg", b"one")

    await fast_jetstream.client.publish(
        "fp.gap.fail.msg",
        b"three",
        reply=_reply(publisher._reply_prefix, 3, _Operation.APPEND),
    )
    await fast_jetstream.client.flush()
    await _wait_for_inbox_message(publisher)

    with pytest.raises(FastPublishGapError) as raised:
        await publisher.add("fp.gap.fail.msg", b"four")

    assert observed == [raised.value]
    assert raised.value.expected_last_sequence == 2
    assert raised.value.current_sequence == 3
    assert raised.value.publish_ack is not None
    assert raised.value.publish_ack.batch_size == 1
    assert publisher.is_closed
    assert (await stream.get_info()).state.messages == 1


async def test_real_gap_ok_notifies_and_continues(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_GAP_OK", ["fp.gap.ok.>"])
    observed: list[FastPublishError] = []
    publisher = fast_publish(
        fast_jetstream,
        flow=1,
        gap_mode=GapMode.OK,
        on_error=observed.append,
    )
    await publisher.add("fp.gap.ok.msg", b"one")
    await fast_jetstream.client.publish(
        "fp.gap.ok.msg",
        b"three",
        reply=_reply(publisher._reply_prefix, 3, _Operation.APPEND),
    )
    await fast_jetstream.client.flush()
    await _wait_for_inbox_message(publisher)
    await publisher._drain_ready()
    publisher._sequence = 3
    publisher._message_count = 3

    await publisher.add("fp.gap.ok.msg", b"four")
    ack = await publisher.close()

    assert len(observed) == 1
    assert isinstance(observed[0], FastPublishGapError)
    assert ack.batch_size == 4
    assert publisher.size == 4
    assert (await stream.get_info()).state.messages == 3


async def test_not_enabled_is_typed_and_unsubscribes(fast_jetstream: JetStream) -> None:
    await fast_jetstream.create_stream(name="FP_DISABLED", subjects=["fp.disabled.>"])
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream, ack_timeout=2)
    with pytest.raises(FastPublishNotEnabledError):
        await publisher.add("fp.disabled.msg", b"data")
    assert publisher.is_closed
    assert _subscription_count(fast_jetstream) == baseline


async def test_fail_mode_flow_error_retains_terminal_ack(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_ERROR_ACK", ["fp.error.ack.>"])
    publisher = fast_publish(fast_jetstream, flow=1, max_outstanding_acks=1)
    await publisher.add("fp.error.ack.first", b"first")

    with pytest.raises(FastPublishFlowError) as raised:
        await publisher.add(
            "fp.error.ack.bad",
            b"bad",
            headers={"Nats-Expected-Last-Sequence": "99"},
        )

    assert raised.value.error_code == 10071
    assert raised.value.batch_sequence == 2
    assert raised.value.publish_ack is not None
    assert raised.value.publish_ack.batch_size == 1
    assert (await stream.get_info()).state.messages == 1


async def test_zero_count_partial_terminal_ack_allows_stream_sequence_zero(
    fast_jetstream: JetStream,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_ZERO_PARTIAL", ["fp.zero.partial.>"])
    publisher = fast_publish(fast_jetstream)
    await publisher._ensure_subscribed()
    publisher._sequence = 1
    publisher._message_count = 1
    flow_error = {
        "type": "err",
        "seq": 1,
        "error": {"code": 400, "err_code": 10071, "description": "message rejected"},
    }
    terminal = {
        "stream": "FP_ZERO_PARTIAL",
        "seq": 0,
        "batch": publisher.batch_id,
        "count": 0,
    }
    await fast_jetstream.client.publish(
        f"{publisher.inbox}.flow-error",
        json.dumps(flow_error).encode(),
    )
    await fast_jetstream.client.publish(
        f"{publisher.inbox}.terminal",
        json.dumps(terminal).encode(),
    )
    await fast_jetstream.client.flush()

    with pytest.raises(FastPublishFlowError) as raised:
        await publisher._wait_for_first_reply()
    assert raised.value.publish_ack is not None
    assert raised.value.publish_ack.sequence == 0
    assert raised.value.publish_ack.batch_size == 0
    assert publisher.is_closed


async def test_fatal_collector_rejects_an_already_pending_terminal_ack(
    fast_jetstream: JetStream,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_FATAL_PENDING", ["fp.fatal.pending.>"])
    publisher = fast_publish(fast_jetstream)
    await publisher._ensure_subscribed()
    publisher._sequence = 1
    publisher._message_count = 1
    terminal = {
        "stream": "FP_FATAL_PENDING",
        "seq": 1,
        "batch": "wrong",
        "count": 1,
    }
    flow_error = {
        "type": "err",
        "seq": 1,
        "error": {"code": 400, "err_code": 10071, "description": "rejected"},
    }
    await _publish_inbox_payload(fast_jetstream, publisher, json.dumps(terminal).encode())
    await publisher._wait_for_event(asyncio.get_running_loop().time() + 1)
    await _publish_inbox_payload(fast_jetstream, publisher, json.dumps(flow_error).encode())
    await publisher._wait_for_event(asyncio.get_running_loop().time() + 1)

    with pytest.raises(FastPublishFlowError) as raised:
        await publisher._raise_fatal()
    assert raised.value.publish_ack is None
    assert publisher.is_closed


async def test_fatal_collector_without_subscription_preserves_original_error(
    fast_jetstream: JetStream,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_FATAL_NO_SUB", ["fp.fatal.no.sub.>"])
    publisher = fast_publish(fast_jetstream)
    await publisher._ensure_subscribed()
    flow_error = {
        "type": "err",
        "seq": 1,
        "error": {"code": 400, "err_code": 10071, "description": "rejected"},
    }
    await _publish_inbox_payload(fast_jetstream, publisher, json.dumps(flow_error).encode())
    await publisher._wait_for_event(asyncio.get_running_loop().time() + 1)
    subscription, publisher._subscription = publisher._subscription, None
    assert subscription is not None
    await subscription.unsubscribe()

    with pytest.raises(FastPublishFlowError) as raised:
        await publisher._raise_fatal()
    assert raised.value.publish_ack is None


@pytest.mark.parametrize(
    "tail",
    [
        b"not-json",
        b'{"stream":"FP_FATAL_TAIL","seq":1,"batch":"wrong","count":1}',
        b'{"stream":"FP_FATAL_TAIL","seq":1,"batch":"placeholder","count":2}',
        b'{"error":{"code":400,"err_code":10208,"description":"unknown"}}',
        b'{"type":"err","seq":2,"error":{"code":400,"err_code":10071}}',
    ],
)
async def test_fatal_collector_defensive_tail_paths(
    fast_jetstream: JetStream,
    tail: bytes,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_FATAL_TAIL", ["fp.fatal.tail.>"])
    publisher = fast_publish(fast_jetstream, ack_timeout=0.1)
    await publisher._ensure_subscribed()
    publisher._sequence = 1
    publisher._message_count = 1
    if b"placeholder" in tail:
        tail = tail.replace(b"placeholder", publisher.batch_id.encode())
    flow_error = {
        "type": "err",
        "seq": 1,
        "error": {"code": 400, "err_code": 10071, "description": "rejected"},
    }
    await fast_jetstream.client.publish(
        f"{publisher.inbox}.flow-error",
        json.dumps(flow_error).encode(),
    )
    await fast_jetstream.client.publish(f"{publisher.inbox}.tail", tail)
    await fast_jetstream.client.flush()
    await publisher._wait_for_event(asyncio.get_running_loop().time() + 1)

    with pytest.raises(FastPublishFlowError) as raised:
        await publisher._raise_fatal()
    assert raised.value.publish_ack is None


async def test_fatal_collector_processes_progress_before_initial_error(
    fast_jetstream: JetStream,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_FATAL_PROGRESS", ["fp.fatal.progress.>"])
    publisher = fast_publish(fast_jetstream, ack_timeout=0.1)
    await publisher._ensure_subscribed()
    flow_error = {
        "type": "err",
        "seq": 1,
        "error": {"code": 400, "err_code": 10071, "description": "rejected"},
    }
    progress = {"type": "ack", "seq": 7, "msgs": 3}
    initial_error = {"error": {"code": 400, "err_code": 10208, "description": "unknown"}}
    for suffix, payload in (
        ("flow-error", flow_error),
        ("progress", progress),
        ("initial-error", initial_error),
    ):
        await fast_jetstream.client.publish(
            f"{publisher.inbox}.{suffix}",
            json.dumps(payload).encode(),
        )
    await fast_jetstream.client.flush()
    await publisher._wait_for_event(asyncio.get_running_loop().time() + 1)

    with pytest.raises(FastPublishFlowError):
        await publisher._raise_fatal()
    assert publisher.last_ack_sequence == 7
    assert publisher._effective_flow == 3


async def test_fatal_collector_timeout_preserves_original_error(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(fast_jetstream, "FP_FATAL_TIMEOUT", ["fp.fatal.timeout.>"])
    publisher = fast_publish(fast_jetstream, ack_timeout=0.02)
    await publisher._ensure_subscribed()
    flow_error = {
        "type": "err",
        "seq": 1,
        "error": {"code": 400, "err_code": 10071, "description": "rejected"},
    }
    await _publish_inbox_payload(fast_jetstream, publisher, json.dumps(flow_error).encode())
    await publisher._wait_for_event(asyncio.get_running_loop().time() + 1)

    with pytest.raises(FastPublishFlowError) as raised:
        await publisher._raise_fatal()
    assert raised.value.publish_ack is None


async def test_fatal_collector_expires_while_consuming_real_progress(
    fast_jetstream: JetStream,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_FATAL_EXPIRE", ["fp.fatal.expire.>"])
    publisher = fast_publish(fast_jetstream, ack_timeout=0.000_1)
    await publisher._ensure_subscribed()
    flow_error = {
        "type": "err",
        "seq": 1,
        "error": {"code": 400, "err_code": 10071, "description": "rejected"},
    }
    await fast_jetstream.client.publish(
        f"{publisher.inbox}.flow-error",
        json.dumps(flow_error).encode(),
    )
    progress = b'{"type":"ack","seq":0,"msgs":1}'
    for index in range(250):
        await fast_jetstream.client.publish(f"{publisher.inbox}.progress.{index}", progress)
    await fast_jetstream.client.flush()
    await publisher._wait_for_event(asyncio.get_running_loop().time() + 1)

    with pytest.raises(FastPublishFlowError):
        await publisher._raise_fatal()


async def test_concurrent_publishers_have_independent_inboxes(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_CONCURRENT", ["fp.concurrent.>"])

    async def publish(worker: int) -> tuple[str, int | None]:
        publisher = fast_publish(fast_jetstream, flow=5, max_outstanding_acks=2)
        for sequence in range(100):
            await publisher.add("fp.concurrent.msg", f"{worker}:{sequence}".encode())
        ack = await publisher.close()
        return publisher.batch_id, ack.batch_size

    results = await asyncio.gather(*(publish(worker) for worker in range(4)))
    assert len({batch_id for batch_id, _ in results}) == 4
    assert {size for _, size in results} == {100}
    assert (await stream.get_info()).state.messages == 400


async def test_custom_multi_token_inbox_uses_final_token(
    fast_jetstream: JetStream,
    fast_server_url: str,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_INBOX", ["fp.inbox.>"])
    client = await connect(fast_server_url, inbox_prefix="_INBOX.application")
    try:
        publisher = fast_publish(new_jetstream(client, strict=True))
        ack = await publisher.commit("fp.inbox.msg", b"one")
        assert publisher.inbox.startswith("_INBOX.application.")
        assert publisher.batch_id == publisher.inbox.rsplit(".", 1)[-1]
        assert ack.batch_id == publisher.batch_id
    finally:
        await client.close()


async def test_real_client_invalid_inbox_tokens_are_rejected(
    fast_jetstream: JetStream,
    fast_server_url: str,
) -> None:
    client = await connect(fast_server_url, inbox_prefix="_INBOX..invalid")
    try:
        with pytest.raises(FastPublishConfigError, match="nonempty subject tokens"):
            fast_publish(new_jetstream(client, strict=True))
    finally:
        await client.close()


@pytest.mark.parametrize(
    ("error_code", "error_type"),
    [
        (10206, FastPublishInvalidPatternError),
        (10207, FastPublishInvalidBatchIdError),
        (10208, FastPublishUnknownBatchIdError),
        (10211, FastPublishTooManyInflightError),
    ],
)
async def test_server_error_mappings_via_real_inbox(
    fast_jetstream: JetStream,
    error_code: int,
    error_type: type[FastPublishError],
) -> None:
    name = f"FP_MAPPING_{error_code}"
    subject = f"fp.mapping.{error_code}.msg"
    await _create_fast_stream(fast_jetstream, name, [f"fp.mapping.{error_code}.>"])
    publisher = fast_publish(fast_jetstream)
    await publisher.add(subject, b"one")
    payload = json.dumps(
        {
            "error": {
                "code": 400,
                "err_code": error_code,
                "description": f"injected {error_code}",
            }
        }
    ).encode()
    await _inject(fast_jetstream, publisher, payload)

    with pytest.raises(error_type) as raised:
        await publisher.add(subject, b"two")
    assert raised.value.code == 400
    assert raised.value.error_code == error_code
    assert publisher.is_closed


@pytest.mark.parametrize(
    "payload",
    [
        b"not-json",
        b"\xff\xfe",
        b"[]",
        b'{"type":"new"}',
        b'{"type":"err","seq":2}',
        b'{"type":"err","seq":0,"error":{}}',
        b'{"type":"err","seq":-1,"error":{}}',
        b'{"type":"err","seq":18446744073709551616,"error":{}}',
        b'{"type":"ack","seq":"2","msgs":1}',
        b'{"type":"ack","seq":true,"msgs":1}',
        b'{"type":"ack","seq":-1,"msgs":1}',
        b'{"type":"ack","seq":18446744073709551616,"msgs":1}',
        b'{"type":"ack","seq":2,"msgs":0}',
        b'{"type":"ack","seq":2,"msgs":-1}',
        b'{"type":"ack","seq":2,"msgs":65536}',
        b'{"type":"gap","last_seq":-1,"seq":2}',
        b'{"type":"gap","last_seq":18446744073709551616,"seq":2}',
        b'{"type":"gap","last_seq":1,"seq":false}',
        b'{"type":"gap","last_seq":1,"seq":-2}',
        b'{"type":"gap","last_seq":1,"seq":18446744073709551616}',
        b'{"stream":"FP_BAD","seq":1,"batch":"","count":1}',
        b'{"stream":"FP_BAD","seq":1,"batch":"id"}',
        b'{"stream":"FP_BAD","seq":18446744073709551616,"batch":"id","count":1}',
        b'{"stream":"FP_BAD","seq":1,"batch":"id","count":18446744073709551616}',
    ],
)
async def test_malformed_inbox_responses_are_terminal(
    fast_jetstream: JetStream,
    payload: bytes,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_MALFORMED", ["fp.malformed.>"])
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream)
    await publisher.add("fp.malformed.msg", b"one")
    await _inject(fast_jetstream, publisher, payload)

    with pytest.raises(FastPublishResponseError):
        await publisher.add("fp.malformed.msg", b"two")
    assert publisher.is_closed
    assert _subscription_count(fast_jetstream) == baseline


@pytest.mark.parametrize(
    "override",
    [
        {"batch": "wrong"},
        {"count": 1},
        {"count": 99},
        {"stream": ""},
        {"stream": 123},
        {"seq": 0},
        {"seq": "2"},
    ],
)
async def test_terminal_ack_validation_rejects_untrusted_values(
    fast_jetstream: JetStream,
    override: dict[str, object],
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_TERMINAL", ["fp.terminal.>"])
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream)
    await publisher.add("fp.terminal.msg", b"one")
    response: dict[str, object] = {
        "stream": "FP_TERMINAL",
        "seq": 2,
        "batch": publisher.batch_id,
        "count": 2,
    }
    response.update(override)
    await _inject(fast_jetstream, publisher, json.dumps(response).encode())

    with pytest.raises(FastPublishResponseError):
        await publisher.commit("fp.terminal.msg", b"two")
    assert publisher.is_closed
    assert _subscription_count(fast_jetstream) == baseline


async def test_timeout_closes_and_unsubscribes(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(
        fast_jetstream,
        "FP_TIMEOUT",
        ["fp.timeout.>"],
        no_ack=True,
    )
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream, ack_timeout=0.15)

    with pytest.raises(FastPublishTimeoutError):
        await publisher.add("fp.timeout.msg", b"one")
    assert publisher.is_closed
    assert _subscription_count(fast_jetstream) == baseline


async def test_direct_no_ack_commit_pings_then_times_out_after_terminal_loss(
    fast_jetstream: JetStream,
) -> None:
    await _create_fast_stream(
        fast_jetstream,
        "FP_NO_ACK_COMMIT",
        ["fp.no.ack.commit.>"],
        no_ack=True,
    )
    observer = await fast_jetstream.client.subscribe("fp.no.ack.commit.>")
    publisher = fast_publish(fast_jetstream, ack_timeout=0.35)
    await publisher._ensure_subscribed()
    assert publisher._subscription is not None
    await publisher._subscription.unsubscribe()
    publisher._subscription = await fast_jetstream.client.subscribe(f"{publisher.inbox}.never.>")
    await fast_jetstream.client.flush()

    with pytest.raises(FastPublishTimeoutError):
        await publisher.commit("fp.no.ack.commit.msg", b"one")

    replies = []
    while observer.pending[0] > 0:
        replies.append((await observer.next()).reply)
    assert any(reply is not None and reply.endswith(".4.$FI") for reply in replies)
    await observer.unsubscribe()


async def test_cancellation_after_handoff_closes_and_unsubscribes(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(
        fast_jetstream,
        "FP_CANCEL",
        ["fp.cancel.>"],
        no_ack=True,
    )
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream, ack_timeout=5)
    operation = asyncio.create_task(publisher.add("fp.cancel.msg", b"one"))
    await _wait_until(lambda: publisher.size == 1)
    operation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await operation
    assert publisher.size == 1
    assert publisher.is_closed
    assert _subscription_count(fast_jetstream) == baseline


async def test_normal_abort_propagates_cancellation_during_real_unsubscribe(
    fast_jetstream: JetStream,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_ABORT_CANCEL", ["fp.abort.cancel.>"])
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream)
    await publisher.add("fp.abort.cancel.msg", b"one")

    operation = asyncio.create_task(publisher.abort())
    await asyncio.sleep(0)
    operation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await operation
    assert publisher.is_closed
    await _wait_until(lambda: _subscription_count(fast_jetstream) == baseline)


async def test_second_cancellation_does_not_replace_active_operation_cancellation(
    fast_jetstream: JetStream,
) -> None:
    await _create_fast_stream(
        fast_jetstream,
        "FP_DOUBLE_CANCEL",
        ["fp.double.cancel.>"],
        no_ack=True,
    )
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream, ack_timeout=5)
    operation = asyncio.create_task(publisher.add("fp.double.cancel.msg", b"one"))
    await _wait_until(lambda: publisher.size == 1)
    operation.cancel()
    asyncio.get_running_loop().call_soon(operation.cancel)

    with pytest.raises(asyncio.CancelledError):
        await operation
    assert publisher.is_closed
    await _wait_until(lambda: _subscription_count(fast_jetstream) == baseline)


async def test_final_ack_wait_cancellation_closes_and_unsubscribes(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(fast_jetstream, "FP_COMMIT_CANCEL", ["fp.commit.cancel.>"])
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream, ack_timeout=5)
    await publisher.add("fp.commit.cancel.msg", b"one")
    assert publisher._subscription is not None
    await publisher._subscription.unsubscribe()
    publisher._subscription = await fast_jetstream.client.subscribe(f"{publisher.inbox}.never.>")

    operation = asyncio.create_task(publisher.commit("fp.commit.cancel.msg", b"two"))
    await _wait_until(lambda: publisher.size == 2)
    operation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await operation
    assert publisher.size == 2
    assert publisher.is_closed
    assert _subscription_count(fast_jetstream) == baseline


async def test_real_subscription_end_is_typed_during_wait(
    fast_jetstream: JetStream,
    fast_server_url: str,
) -> None:
    client = await connect(fast_server_url, allow_reconnect=False)
    publisher = fast_publish(new_jetstream(client, strict=True))
    await publisher._ensure_subscribed()
    await client.close()

    with pytest.raises(FastPublishClosedError, match="subscription ended"):
        await publisher._wait_for_event(asyncio.get_running_loop().time() + 1)
    assert publisher.is_closed


async def test_wait_defensive_paths_use_real_subscriptions(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(fast_jetstream, "FP_WAIT_PATHS", ["fp.wait.paths.>"])

    publisher = fast_publish(fast_jetstream)
    await publisher._drain_ready()
    with pytest.raises(FastPublishClosedError, match="subscription is closed"):
        await publisher._wait_for_event(asyncio.get_running_loop().time() + 1)
    await publisher.abort()

    expired = fast_publish(fast_jetstream)
    with pytest.raises(FastPublishTimeoutError):
        await expired._wait_for_event(asyncio.get_running_loop().time() - 1)

    malformed = fast_publish(fast_jetstream)
    await malformed._ensure_subscribed()
    await _publish_inbox_payload(fast_jetstream, malformed, b'{"type":"unknown"}')
    with pytest.raises(FastPublishResponseError):
        await malformed._wait_for_event(asyncio.get_running_loop().time() + 1)
    assert malformed.is_closed


async def test_ping_before_first_message_is_terminal(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(fast_jetstream, "FP_PING_EMPTY", ["fp.ping.empty.>"])
    publisher = fast_publish(fast_jetstream)

    with pytest.raises(FastPublishResponseError, match="before its first message"):
        await publisher._send_ping()
    assert publisher.is_closed


async def test_ping_publish_failure_after_real_transport_close(
    fast_jetstream: JetStream,
    fast_server_url: str,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_PING_CLOSE", ["fp.ping.close.>"])
    client = await connect(fast_server_url, allow_reconnect=False)
    publisher = fast_publish(new_jetstream(client, strict=True))
    await publisher.add("fp.ping.close.msg", b"one")
    await client.close()

    with pytest.raises(FastPublishPublishError, match="failed to ping"):
        await publisher._send_ping()
    assert publisher.is_closed


async def test_closed_client_subscription_failure_is_typed(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(fast_jetstream, "FP_SUB_FAIL", ["fp.sub.fail.>"])
    publisher = fast_publish(fast_jetstream)
    await fast_jetstream.client.close()

    with pytest.raises(FastPublishSubscribeError):
        await publisher.add("fp.sub.fail.msg", b"one")
    assert publisher.is_closed


async def test_closed_client_publish_failure_is_typed(
    fast_jetstream: JetStream,
    fast_server_url: str,
) -> None:
    client = await connect(fast_server_url)
    js = new_jetstream(client, strict=True)
    publisher = fast_publish(js)
    await publisher._ensure_subscribed()
    await client.close()

    with pytest.raises(FastPublishPublishError):
        await publisher.add("fp.publish.fail.msg", b"one")
    assert publisher.is_closed


@pytest.mark.parametrize("gap_mode", [GapMode.FAIL, GapMode.OK])
async def test_actual_flow_error_callback_exception_propagates_and_cleans_up(
    fast_jetstream: JetStream,
    gap_mode: GapMode,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_CALLBACK", ["fp.callback.>"])
    baseline = _subscription_count(fast_jetstream)

    def abort_callback(error: FastPublishError) -> None:
        assert isinstance(error, FastPublishFlowError)
        raise RuntimeError("callback aborted publication")

    publisher = fast_publish(fast_jetstream, gap_mode=gap_mode, on_error=abort_callback)
    await publisher.add("fp.callback.good", b"one")
    await publisher.add(
        "fp.callback.bad",
        b"bad",
        headers={"Nats-Expected-Last-Sequence": "99"},
    )
    await _wait_for_inbox_message(publisher)

    with pytest.raises(RuntimeError, match="callback aborted"):
        await publisher.add("fp.callback.good", b"two")
    assert publisher.is_closed
    assert _subscription_count(fast_jetstream) == baseline


async def test_abort_is_idempotent_and_releases_inbox(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_ABORT", ["fp.abort.>"])
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream)
    await publisher.add("fp.abort.msg", b"one")
    assert _subscription_count(fast_jetstream) == baseline + 1

    await publisher.abort()
    await publisher.aclose()

    assert publisher.is_closed
    assert _subscription_count(fast_jetstream) == baseline
    assert (await stream.get_info()).state.messages == 1
    with pytest.raises(FastPublishClosedError):
        await publisher.add("fp.abort.msg", b"two")


async def test_cleanup_tolerates_real_transport_teardown(
    fast_jetstream: JetStream,
    fast_server_url: str,
) -> None:
    client = await connect(fast_server_url, allow_reconnect=False)
    publisher = fast_publish(new_jetstream(client, strict=True))
    await publisher._ensure_subscribed()
    await client._connection.close()

    await publisher.abort()
    assert publisher.is_closed
    await client.close()


async def test_async_context_aborts_unfinished_batch(fast_jetstream: JetStream) -> None:
    stream = await _create_fast_stream(fast_jetstream, "FP_CONTEXT", ["fp.context.>"])
    baseline = _subscription_count(fast_jetstream)

    async with fast_publish(fast_jetstream) as publisher:
        await publisher.add("fp.context.msg", b"one")
        assert _subscription_count(fast_jetstream) == baseline + 1

    assert publisher.is_closed
    assert _subscription_count(fast_jetstream) == baseline
    assert (await stream.get_info()).state.messages == 1


async def test_drop_schedules_best_effort_inbox_cleanup(fast_jetstream: JetStream) -> None:
    await _create_fast_stream(fast_jetstream, "FP_DROP", ["fp.drop.>"])
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream)
    await publisher.add("fp.drop.msg", b"one")
    reference = weakref.ref(publisher)
    assert _subscription_count(fast_jetstream) == baseline + 1

    del publisher
    gc.collect()
    await _wait_until(lambda: reference() is None)
    await _wait_until(lambda: _subscription_count(fast_jetstream) == baseline)

    recovered = fast_publish(fast_jetstream)
    ack = await recovered.commit("fp.drop.recovered", b"two")
    assert ack.batch_size == 1
    assert _subscription_count(fast_jetstream) == baseline


async def test_finalizer_outside_event_loop_leaves_explicit_cleanup_available(
    fast_jetstream: JetStream,
) -> None:
    await _create_fast_stream(fast_jetstream, "FP_DROP_THREAD", ["fp.drop.thread.>"])
    baseline = _subscription_count(fast_jetstream)
    publisher = fast_publish(fast_jetstream)
    await publisher.add("fp.drop.thread.msg", b"one")
    subscription = publisher._subscription
    assert subscription is not None

    await asyncio.to_thread(publisher.__del__)

    assert publisher.is_closed
    assert publisher._subscription is None
    await subscription.unsubscribe()
    assert _subscription_count(fast_jetstream) == baseline


async def test_finalizer_callback_accepts_cancelled_real_cleanup_task(
    fast_jetstream: JetStream,
) -> None:
    subscription = await fast_jetstream.client.subscribe("fp.cancelled.cleanup")
    cleanup = asyncio.create_task(subscription.unsubscribe())
    cleanup.cancel()
    await asyncio.gather(cleanup, return_exceptions=True)

    _consume_task_exception(cleanup)

    assert cleanup.cancelled()
    await subscription.unsubscribe()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"flow": 0}, "flow"),
        ({"flow": True}, "flow"),
        ({"flow": 65_536}, "flow"),
        ({"max_outstanding_acks": 0}, "max_outstanding_acks"),
        ({"max_outstanding_acks": True}, "max_outstanding_acks"),
        ({"max_outstanding_acks": 4}, "max_outstanding_acks"),
        ({"ack_timeout": 0}, "ack_timeout"),
        ({"ack_timeout": True}, "ack_timeout"),
        ({"ack_timeout": float("nan")}, "ack_timeout"),
        ({"gap_mode": "sometimes"}, "gap mode"),
    ],
)
async def test_invalid_configuration_uses_live_client(
    fast_jetstream: JetStream,
    kwargs: dict[str, Any],
    message: str,
) -> None:
    with pytest.raises(FastPublishConfigError, match=message):
        fast_publish(fast_jetstream, **kwargs)
