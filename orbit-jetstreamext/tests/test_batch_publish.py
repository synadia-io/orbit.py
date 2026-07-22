"""Unit tests for atomic batch publishing."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, cast

import pytest
from nats.client.errors import NoRespondersError
from nats.client.message import Message, Status

from orbit.jetstreamext import (
    AtomicPublishNotEnabledError,
    AtomicPublishUnsupportedHeaderError,
    BatchClosedError,
    BatchMessage,
    BatchPublisher,
    BatchPublishRequestError,
    BatchTooLargeError,
    EmptyBatchError,
    InvalidBatchAckError,
    JetStreamExtError,
    batch_publish,
    publish_batch,
)

if TYPE_CHECKING:
    from nats.jetstream import JetStream


class FakeClient:
    def __init__(self) -> None:
        self.publishes: list[tuple[str, bytes, dict[str, str | list[str]]]] = []
        self.requests: list[tuple[str, bytes, dict[str, str | list[str]], float]] = []
        self.failure: BaseException | None = None
        self.response_override: bytes | None = None
        self.status_override: Status | None = None

    async def publish(
        self,
        subject: str,
        data: bytes,
        *,
        headers: dict[str, str | list[str]],
    ) -> None:
        if self.failure is not None:
            raise self.failure
        self.publishes.append((subject, data, headers))

    async def request(
        self,
        subject: str,
        data: bytes,
        *,
        headers: dict[str, str | list[str]],
        timeout: float,
        return_on_error: bool,
    ) -> Message:
        assert return_on_error
        if self.failure is not None:
            raise self.failure
        self.requests.append((subject, data, headers, timeout))
        response = self.response_override
        if response is None and headers.get("Nats-Batch-Commit") == "1":
            response = json.dumps(
                {
                    "stream": "EVENTS",
                    "seq": 42,
                    "batch": headers["Nats-Batch-Id"],
                    "count": int(cast("str", headers["Nats-Batch-Sequence"])),
                }
            ).encode()
        return Message(subject="_INBOX.test", data=response or b"", status=self.status_override)


class FakeJetStream:
    def __init__(self) -> None:
        self.client = FakeClient()


def _js() -> tuple[JetStream, FakeClient]:
    fake = FakeJetStream()
    return cast("JetStream", fake), fake.client


async def test_add_sets_protocol_headers_without_mutating_input() -> None:
    js, client = _js()
    headers = {"X-Trace": "abc"}
    publisher = batch_publish(js, ack_first=False)

    await publisher.add("events.one", b"one", headers=headers)

    assert headers == {"X-Trace": "abc"}
    assert publisher.size == 1
    assert not publisher.is_closed
    assert client.publishes == [
        (
            "events.one",
            b"one",
            {
                "X-Trace": "abc",
                "Nats-Batch-Id": publisher.batch_id,
                "Nats-Batch-Sequence": "1",
            },
        )
    ]


async def test_flow_control_requests_first_and_every_nth_message() -> None:
    js, client = _js()
    publisher = batch_publish(js, ack_every=2, timeout=1.25)

    for sequence in range(1, 5):
        await publisher.add("events.data", str(sequence).encode())

    assert [request[2]["Nats-Batch-Sequence"] for request in client.requests] == ["1", "2", "4"]
    assert [publish[2]["Nats-Batch-Sequence"] for publish in client.publishes] == ["3"]
    assert all(request[3] == 1.25 for request in client.requests)


async def test_commit_validates_ack_and_closes_publisher() -> None:
    js, client = _js()
    publisher = batch_publish(js, ack_first=False)
    await publisher.add("events.one", b"one")

    ack = await publisher.commit("events.two", b"two")

    assert ack.stream == "EVENTS"
    assert ack.sequence == 42
    assert ack.batch_id == publisher.batch_id
    assert ack.batch_size == 2
    assert publisher.size == 2
    assert publisher.is_closed
    assert client.requests[-1][2]["Nats-Batch-Commit"] == "1"
    with pytest.raises(BatchClosedError):
        await publisher.add("events.three", b"three")


@pytest.mark.parametrize(
    "response",
    [
        b"not json",
        b"{}",
        b'{"stream":"EVENTS","seq":1,"batch":"wrong","count":1}',
        b'{"stream":"","seq":1,"batch":"unused","count":1}',
    ],
)
async def test_commit_rejects_invalid_ack(response: bytes) -> None:
    js, client = _js()
    client.response_override = response
    publisher = batch_publish(js)
    with pytest.raises(InvalidBatchAckError):
        await publisher.commit("events.one", b"one")
    assert publisher.is_closed


async def test_server_error_maps_to_specific_type_and_closes() -> None:
    js, client = _js()
    client.response_override = json.dumps(
        {"error": {"code": 400, "err_code": 10174, "description": "atomic publish is disabled"}}
    ).encode()
    publisher = batch_publish(js)

    with pytest.raises(AtomicPublishNotEnabledError) as raised:
        await publisher.add("events.one", b"one")

    assert raised.value.code == 400
    assert raised.value.error_code == 10174
    assert publisher.is_closed


async def test_request_failure_is_typed_and_closes() -> None:
    js, client = _js()
    client.failure = TimeoutError("late")
    publisher = batch_publish(js)

    with pytest.raises(BatchPublishRequestError) as raised:
        await publisher.add("events.one", b"one")

    assert isinstance(raised.value.__cause__, TimeoutError)
    assert publisher.is_closed


async def test_status_only_request_failure_is_typed_and_closes() -> None:
    js, client = _js()
    client.status_override = Status(code="503", description="No Responders")
    publisher = batch_publish(js)

    with pytest.raises(BatchPublishRequestError) as raised:
        await publisher.add("events.one", b"one")

    assert "503: No Responders" in str(raised.value)
    assert isinstance(raised.value.__cause__, NoRespondersError)
    assert raised.value.__cause__.status == "503"
    assert raised.value.__cause__.subject == "events.one"
    assert publisher.is_closed


async def test_non_ack_publish_failure_closes() -> None:
    js, client = _js()
    client.failure = RuntimeError("closed")
    publisher = batch_publish(js, ack_first=False)

    with pytest.raises(BatchPublishRequestError) as raised:
        await publisher.add("events.one", b"one")

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert publisher.is_closed


@pytest.mark.parametrize("ack_first", [True, False])
async def test_cancellation_during_add_io_closes_and_propagates(ack_first: bool) -> None:
    js, client = _js()
    cancelled = asyncio.CancelledError("stop")
    client.failure = cancelled
    publisher = batch_publish(js, ack_first=ack_first)

    with pytest.raises(asyncio.CancelledError) as raised:
        await publisher.add("events.one", b"one")

    assert raised.value is cancelled
    assert raised.value.__cause__ is None
    assert publisher.is_closed


@pytest.mark.parametrize(
    "header",
    [
        "nats-expected-last-msg-id",
        "Nats-Batch-Id",
        "nats-batch-sequence",
        "Nats-Batch-Commit",
    ],
)
async def test_managed_and_unsupported_headers_are_rejected_case_insensitively(header: str) -> None:
    js, client = _js()
    publisher = batch_publish(js)

    with pytest.raises(AtomicPublishUnsupportedHeaderError):
        await publisher.add("events.one", b"one", headers={header: "bad"})

    assert not publisher.is_closed
    assert publisher.size == 0
    assert not client.publishes
    assert not client.requests


async def test_unique_message_id_is_preserved() -> None:
    js, client = _js()
    publisher = batch_publish(js, ack_first=False)

    await publisher.add("events.one", b"one", headers={"Nats-Msg-Id": "message-1"})

    assert client.publishes[0][2]["Nats-Msg-Id"] == "message-1"
    assert not publisher.is_closed


async def test_expected_last_sequence_is_allowed_only_on_first_message() -> None:
    js, _ = _js()
    publisher = batch_publish(js)
    await publisher.add("events.one", b"one", headers={"Nats-Expected-Last-Sequence": "0"})

    with pytest.raises(AtomicPublishUnsupportedHeaderError):
        await publisher.add("events.two", b"two", headers={"Nats-Expected-Last-Sequence": "0"})

    assert not publisher.is_closed
    assert publisher.size == 1
    ack = await publisher.commit("events.two", b"two")
    assert ack.batch_size == 2


async def test_batch_limit_includes_commit_message() -> None:
    js, _ = _js()
    publisher = batch_publish(js, ack_first=False)
    for _ in range(999):
        await publisher.add("events.data", b"data")
    ack = await publisher.commit("events.final", b"final")
    assert ack.batch_size == 1000

    publisher = batch_publish(js, ack_first=False)
    for _ in range(1000):
        await publisher.add("events.data", b"data")
    with pytest.raises(BatchTooLargeError):
        await publisher.commit("events.final", b"final")
    assert not publisher.is_closed


async def test_discard_closes_without_io() -> None:
    js, client = _js()
    publisher = batch_publish(js, ack_first=False)
    await publisher.add("events.one", b"one")
    publisher.discard()

    assert publisher.is_closed
    assert not client.requests
    with pytest.raises(BatchClosedError):
        publisher.discard()


async def test_publish_batch_accepts_regular_iterable() -> None:
    js, client = _js()
    ack = await publish_batch(
        js,
        [
            BatchMessage("events.one", b"one", {"X-Source": "test"}),
            BatchMessage("events.two", b"two"),
            BatchMessage("events.three", b"three"),
        ],
        ack_first=False,
    )

    assert ack.batch_size == 3
    assert len(client.publishes) == 2
    assert len(client.requests) == 1
    assert client.requests[0][2]["Nats-Batch-Commit"] == "1"


async def test_publish_batch_accepts_async_iterable() -> None:
    js, _ = _js()

    async def messages() -> AsyncIterator[BatchMessage]:
        yield BatchMessage("events.one", b"one")
        yield BatchMessage("events.two", b"two")

    ack = await publish_batch(js, messages())
    assert ack.batch_size == 2


async def test_publish_batch_single_message_commits_it() -> None:
    js, client = _js()
    ack = await publish_batch(js, [BatchMessage("events.one", b"one")])
    assert ack.batch_size == 1
    assert not client.publishes
    assert client.requests[0][2]["Nats-Batch-Sequence"] == "1"
    assert client.requests[0][2]["Nats-Batch-Commit"] == "1"


async def test_publish_batch_rejects_empty_and_wrong_item() -> None:
    js, _ = _js()
    with pytest.raises(EmptyBatchError):
        await publish_batch(js, [])
    with pytest.raises(TypeError, match="BatchMessage"):
        await publish_batch(js, cast("Any", [("events.one", b"one")]))


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
def test_invalid_configuration(kwargs: dict[str, Any], match: str) -> None:
    js, _ = _js()
    with pytest.raises(ValueError, match=match):
        BatchPublisher(js, **kwargs)


def test_batch_errors_share_package_base() -> None:
    assert issubclass(BatchTooLargeError, JetStreamExtError)
