"""Atomic batch protocol tests over a real nats-server connection."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import pytest
from nats.client.message import Headers, Message
from nats.jetstream_extra import (
    AtomicPublishDuplicateMessageIDError,
    AtomicPublishIncompleteError,
    AtomicPublishInvalidCommitError,
    AtomicPublishInvalidIDError,
    AtomicPublishMirrorError,
    AtomicPublishMissingSequenceError,
    AtomicPublishNotEnabledError,
    AtomicPublishTooManyInflightError,
    AtomicPublishUnsupportedHeaderError,
    BatchClosedError,
    BatchPublisher,
    BatchPublishError,
    BatchPublishRequestError,
    BatchPublishServerError,
    BatchTooLargeError,
    InvalidBatchAckError,
    JetStreamExtError,
    batch_publish,
)

if TYPE_CHECKING:
    from nats.client.subscription import Subscription
    from nats.jetstream import JetStream


async def _subscribe(js: JetStream, subject: str) -> Subscription:
    subscription = await js.client.subscribe(subject)
    await js.client.flush()
    return subscription


async def _next(subscription: Subscription) -> Message:
    return await subscription.next(timeout=2.0)


async def _respond(js: JetStream, message: Message, data: bytes = b"") -> None:
    assert message.reply is not None
    await js.client.publish(message.reply, data)


def _ack(message: Message, **overrides: object) -> bytes:
    assert message.headers is not None
    response: dict[str, object] = {
        "stream": "EVENTS",
        "seq": 42,
        "batch": message.headers.get("Nats-Batch-Id"),
        "count": int(message.headers.get("Nats-Batch-Sequence") or "0"),
    }
    response.update(overrides)
    return json.dumps(response).encode()


async def test_add_sets_protocol_headers_without_mutating_input(atomic_jetstream: JetStream) -> None:
    subscription = await _subscribe(atomic_jetstream, "protocol.headers")
    headers: dict[str, str | list[str]] = {"X-Trace": "abc", "X-Multi": ["one", "two"]}
    publisher = batch_publish(atomic_jetstream)

    task = asyncio.create_task(publisher.add("protocol.headers", b"one", headers=headers))
    message = await _next(subscription)

    assert message.data == b"one"
    assert message.headers is not None
    assert message.headers.get("X-Trace") == "abc"
    assert message.headers.get_all("X-Multi") == ["one", "two"]
    assert message.headers.get("Nats-Batch-Id") == publisher.batch_id
    assert message.headers.get("Nats-Batch-Sequence") == "1"
    assert message.headers.get("Nats-Batch-Commit") is None
    assert headers == {"X-Trace": "abc", "X-Multi": ["one", "two"]}

    await _respond(atomic_jetstream, message)
    await task
    assert publisher.size == 1
    assert not publisher.is_closed
    publisher.discard()


async def test_headers_object_is_copied(atomic_jetstream: JetStream) -> None:
    subscription = await _subscribe(atomic_jetstream, "protocol.headers-object")
    headers = Headers({"X-Values": ["a", "b"]})
    publisher = batch_publish(atomic_jetstream)

    task = asyncio.create_task(publisher.add("protocol.headers-object", b"data", headers=headers))
    message = await _next(subscription)
    assert message.headers is not None
    assert message.headers.get_all("X-Values") == ["a", "b"]
    await _respond(atomic_jetstream, message)
    await task

    assert headers.get_all("X-Values") == ["a", "b"]
    publisher.discard()


async def test_flow_control_requests_first_and_every_nth_message(atomic_jetstream: JetStream) -> None:
    subscription = await _subscribe(atomic_jetstream, "protocol.flow")
    publisher = batch_publish(atomic_jetstream, ack_every=2)

    for sequence in range(1, 5):
        task = asyncio.create_task(publisher.add("protocol.flow", str(sequence).encode()))
        message = await _next(subscription)
        assert message.headers is not None
        assert message.headers.get("Nats-Batch-Sequence") == str(sequence)
        if sequence in {1, 2, 4}:
            assert message.reply is not None
            await _respond(atomic_jetstream, message)
        else:
            assert message.reply is None
        await task

    publisher.discard()


async def test_commit_validates_ack_fields_and_closes(atomic_jetstream: JetStream) -> None:
    subscription = await _subscribe(atomic_jetstream, "protocol.commit")
    publisher = batch_publish(atomic_jetstream, ack_first=False)

    await publisher.add("protocol.commit", b"one")
    first = await _next(subscription)
    assert first.reply is None

    task = asyncio.create_task(publisher.commit("protocol.commit", b"two", headers={"X-Final": "yes"}))
    final = await _next(subscription)
    assert final.reply is not None
    assert final.headers is not None
    assert final.headers.get("Nats-Batch-Sequence") == "2"
    assert final.headers.get("Nats-Batch-Commit") == "1"
    assert final.headers.get("X-Final") == "yes"
    await _respond(atomic_jetstream, final, _ack(final, domain="TEST", val="42"))
    ack = await task

    assert ack.stream == "EVENTS"
    assert ack.sequence == 42
    assert ack.batch_id == publisher.batch_id
    assert ack.batch_size == 2
    assert ack.domain == "TEST"
    assert ack.value == "42"
    assert publisher.size == 2
    assert publisher.is_closed
    with pytest.raises(BatchClosedError):
        await publisher.add("protocol.commit", b"three")
    with pytest.raises(BatchClosedError):
        await publisher.commit("protocol.commit", b"three")
    with pytest.raises(BatchClosedError):
        publisher.discard()


@pytest.mark.parametrize(
    "case",
    ["not-json", "not-object", "empty", "wrong-batch", "wrong-count", "empty-stream", "bad-domain", "bad-value"],
)
async def test_commit_rejects_invalid_ack(atomic_jetstream: JetStream, case: str) -> None:
    subject = f"protocol.invalid-ack.{case}"
    subscription = await _subscribe(atomic_jetstream, subject)
    publisher = batch_publish(atomic_jetstream)

    task = asyncio.create_task(publisher.commit(subject, b"one"))
    message = await _next(subscription)
    if case == "not-json":
        response = b"not json"
    elif case == "not-object":
        response = b"[]"
    elif case == "empty":
        response = b"{}"
    elif case == "wrong-batch":
        response = _ack(message, batch="wrong")
    elif case == "wrong-count":
        response = _ack(message, count=2)
    elif case == "empty-stream":
        response = _ack(message, stream="")
    elif case == "bad-domain":
        response = _ack(message, domain=1)
    else:
        response = _ack(message, val=1)
    await _respond(atomic_jetstream, message, response)

    with pytest.raises(InvalidBatchAckError):
        await task
    assert publisher.is_closed


@pytest.mark.parametrize("response", [b"not json", b"{}", b"[]", b'{"unexpected":true}'])
async def test_flow_control_rejects_nonempty_non_error_ack(atomic_jetstream: JetStream, response: bytes) -> None:
    subject = "protocol.invalid-flow"
    subscription = await _subscribe(atomic_jetstream, subject)
    publisher = batch_publish(atomic_jetstream)

    task = asyncio.create_task(publisher.add(subject, b"one"))
    message = await _next(subscription)
    await _respond(atomic_jetstream, message, response)

    with pytest.raises(InvalidBatchAckError):
        await task
    assert publisher.is_closed


@pytest.mark.parametrize(
    ("error_code", "error_type"),
    [
        (10174, AtomicPublishNotEnabledError),
        (10175, AtomicPublishMissingSequenceError),
        (10176, AtomicPublishIncompleteError),
        (10177, AtomicPublishUnsupportedHeaderError),
        (10179, AtomicPublishInvalidIDError),
        (10198, AtomicPublishMirrorError),
        (10200, AtomicPublishInvalidCommitError),
        (10201, AtomicPublishDuplicateMessageIDError),
        (10210, AtomicPublishTooManyInflightError),
    ],
)
async def test_server_error_mapping_over_real_transport(
    atomic_jetstream: JetStream,
    error_code: int,
    error_type: type[BatchPublishServerError],
) -> None:
    subject = f"protocol.error.{error_code}"
    subscription = await _subscribe(atomic_jetstream, subject)
    publisher = batch_publish(atomic_jetstream)

    task = asyncio.create_task(publisher.add(subject, b"one"))
    message = await _next(subscription)
    response = json.dumps(
        {"error": {"code": 400, "err_code": error_code, "description": f"server error {error_code}"}}
    ).encode()
    await _respond(atomic_jetstream, message, response)

    with pytest.raises(error_type) as raised:
        await task
    assert raised.value.code == 400
    assert raised.value.error_code == error_code
    assert raised.value.description == f"server error {error_code}"
    assert publisher.is_closed
    with pytest.raises(BatchClosedError):
        await publisher.add(subject, b"two")
    with pytest.raises(BatchClosedError):
        await publisher.commit(subject, b"two")


async def test_unknown_server_error_retains_metadata(atomic_jetstream: JetStream) -> None:
    subscription = await _subscribe(atomic_jetstream, "protocol.error.unknown")
    publisher = batch_publish(atomic_jetstream)
    task = asyncio.create_task(publisher.add("protocol.error.unknown", b"one"))
    message = await _next(subscription)
    response = json.dumps({"error": {"code": 503, "err_code": 19999, "description": "future error"}}).encode()
    await _respond(atomic_jetstream, message, response)

    with pytest.raises(BatchPublishServerError) as raised:
        await task
    assert type(raised.value) is BatchPublishServerError
    assert raised.value.code == 503
    assert raised.value.error_code == 19999
    assert raised.value.description == "future error"


async def test_server_too_large_error_maps_to_client_limit_type(atomic_jetstream: JetStream) -> None:
    subscription = await _subscribe(atomic_jetstream, "protocol.error.10199")
    publisher = batch_publish(atomic_jetstream)
    task = asyncio.create_task(publisher.add("protocol.error.10199", b"one"))
    message = await _next(subscription)
    response = json.dumps(
        {"error": {"code": 400, "err_code": 10199, "description": "batch exceeds server limit"}}
    ).encode()
    await _respond(atomic_jetstream, message, response)

    with pytest.raises(BatchTooLargeError, match="server limit"):
        await task
    assert publisher.is_closed


async def test_request_timeout_is_typed_and_closes(atomic_jetstream: JetStream) -> None:
    await _subscribe(atomic_jetstream, "protocol.silent")
    publisher = batch_publish(atomic_jetstream, timeout=0.05)

    with pytest.raises(BatchPublishRequestError) as raised:
        await publisher.add("protocol.silent", b"one")

    assert isinstance(raised.value.__cause__, TimeoutError)
    assert publisher.is_closed


async def test_commit_timeout_is_typed_closes_and_prevents_reuse(atomic_jetstream: JetStream) -> None:
    await _subscribe(atomic_jetstream, "protocol.commit.silent")
    publisher = batch_publish(atomic_jetstream, timeout=0.05)

    with pytest.raises(BatchPublishRequestError) as raised:
        await publisher.commit("protocol.commit.silent", b"one")

    assert isinstance(raised.value.__cause__, TimeoutError)
    assert publisher.is_closed
    with pytest.raises(BatchClosedError):
        await publisher.add("protocol.commit.silent", b"two")


async def test_non_ack_publish_on_closed_connection_is_typed_and_closes(atomic_jetstream: JetStream) -> None:
    publisher = batch_publish(atomic_jetstream, ack_first=False)
    await atomic_jetstream.client.close()

    with pytest.raises(BatchPublishRequestError) as raised:
        await publisher.add("protocol.closed", b"one")

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert publisher.is_closed


async def test_cancellation_during_request_closes_and_propagates(atomic_jetstream: JetStream) -> None:
    subscription = await _subscribe(atomic_jetstream, "protocol.cancel")
    publisher = batch_publish(atomic_jetstream, timeout=5.0)

    task = asyncio.create_task(publisher.add("protocol.cancel", b"one"))
    await _next(subscription)
    task.cancel("stop")

    with pytest.raises(asyncio.CancelledError) as raised:
        await task
    assert raised.value.args == ("stop",)
    assert publisher.is_closed


async def test_cancellation_during_commit_closes_and_propagates(atomic_jetstream: JetStream) -> None:
    subscription = await _subscribe(atomic_jetstream, "protocol.commit.cancel")
    publisher = batch_publish(atomic_jetstream, timeout=5.0)

    task = asyncio.create_task(publisher.commit("protocol.commit.cancel", b"one"))
    await _next(subscription)
    task.cancel("stop commit")

    with pytest.raises(asyncio.CancelledError) as raised:
        await task
    assert raised.value.args == ("stop commit",)
    assert publisher.is_closed
    with pytest.raises(BatchClosedError):
        await publisher.commit("protocol.commit.cancel", b"two")


@pytest.mark.parametrize(
    "header",
    ["nats-expected-last-msg-id", "Nats-Batch-Id", "nats-batch-sequence", "Nats-Batch-Commit"],
)
async def test_managed_and_unsupported_headers_are_rejected_case_insensitively(
    atomic_jetstream: JetStream,
    header: str,
) -> None:
    publisher = batch_publish(atomic_jetstream)

    with pytest.raises(AtomicPublishUnsupportedHeaderError):
        await publisher.add("protocol.validation", b"one", headers={header: "bad"})

    assert not publisher.is_closed
    assert publisher.size == 0


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"ack_every": 0}, "ack_every"),
        ({"ack_every": -1}, "ack_every"),
        ({"ack_every": True}, "ack_every"),
        ({"timeout": 0.0}, "timeout"),
        ({"timeout": -1.0}, "timeout"),
        ({"timeout": float("nan")}, "timeout"),
    ],
)
async def test_invalid_configuration(atomic_jetstream: JetStream, kwargs: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        BatchPublisher(atomic_jetstream, **kwargs)


async def test_batch_errors_share_package_base(atomic_jetstream: JetStream) -> None:
    publisher = batch_publish(atomic_jetstream)
    assert publisher.batch_id
    assert issubclass(BatchTooLargeError, JetStreamExtError)
    assert issubclass(BatchPublishRequestError, BatchPublishError)
