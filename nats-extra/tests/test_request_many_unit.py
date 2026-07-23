"""Deterministic unit coverage for request-many termination behavior."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from nats.client.message import Message, Status
from nats.extra import RequestMany, TerminationReason, request_many


class FakeSubscription:
    def __init__(self) -> None:
        self.messages: asyncio.Queue[Message] = asyncio.Queue()
        self.unsubscribed = False
        self.closed = False

    async def next(self, timeout: float | None = None) -> Message:
        if self.closed:
            raise RuntimeError("closed")
        if timeout is None:
            return await self.messages.get()
        return await asyncio.wait_for(self.messages.get(), timeout)

    async def unsubscribe(self) -> None:
        self.unsubscribed = True


class FakeClient:
    def __init__(self, on_publish: Callable[[FakeSubscription], None] | None = None) -> None:
        self.subscription = FakeSubscription()
        self.on_publish = on_publish
        self.published: tuple[str, bytes, str | None] | None = None

    def new_inbox(self) -> str:
        return "_INBOX.test"

    async def subscribe(self, subject: str) -> FakeSubscription:
        assert subject == "_INBOX.test"
        return self.subscription

    async def publish(self, subject: str, payload: bytes, *, reply: str | None = None) -> None:
        self.published = (subject, payload, reply)
        if self.on_publish is not None:
            self.on_publish(self.subscription)


def _enqueue(*messages: Message) -> Callable[[FakeSubscription], None]:
    def add(subscription: FakeSubscription) -> None:
        for message in messages:
            subscription.messages.put_nowait(message)

    return add


async def test_builder_stops_at_max_messages() -> None:
    client = FakeClient(_enqueue(Message("reply", b"one"), Message("reply", b"two")))

    responses = await RequestMany(client).max_messages(1).send("work", b"go")

    assert [message.data async for message in responses] == [b"one"]
    assert responses.messages_received == 1
    assert responses.termination_reason is TerminationReason.MAX_MESSAGES
    assert client.subscription.unsubscribed


async def test_sentinel_is_counted_but_not_yielded() -> None:
    client = FakeClient(_enqueue(Message("reply", b"one"), Message("reply", b"done")))
    responses = await request_many(
        client,
        "work",
        sentinel=lambda message: message.data == b"done",
        max_wait=None,
    )

    assert [message.data async for message in responses] == [b"one"]
    assert responses.messages_received == 2
    assert responses.termination_reason is TerminationReason.SENTINEL


async def test_no_responders_status_is_not_yielded() -> None:
    client = FakeClient(_enqueue(Message("reply", b"", status=Status("503", "No Responders"))))
    responses = await request_many(client, "work")

    assert [message async for message in responses] == []
    assert responses.messages_received == 0
    assert responses.termination_reason is TerminationReason.NO_RESPONDERS


async def test_stall_wait_terminates_idle_stream() -> None:
    client = FakeClient()
    responses = await request_many(client, "work", max_wait=None, stall_wait=0.001)

    assert [message async for message in responses] == []
    assert responses.termination_reason is TerminationReason.STALL_WAIT


async def test_max_wait_terminates_idle_stream() -> None:
    client = FakeClient()
    responses = await request_many(client, "work", max_wait=0.001)

    assert [message async for message in responses] == []
    assert responses.termination_reason is TerminationReason.MAX_WAIT


async def test_subscription_closure_terminates_stream() -> None:
    client = FakeClient()
    client.subscription.closed = True
    responses = await request_many(client, "work", max_wait=None)

    assert [message async for message in responses] == []
    assert responses.termination_reason is TerminationReason.SUBSCRIPTION_CLOSED


async def test_aclose_unsubscribes() -> None:
    client = FakeClient()
    responses = await request_many(client, "work", max_wait=None)

    await responses.aclose()

    assert responses.termination_reason is TerminationReason.SUBSCRIPTION_CLOSED
    assert client.subscription.unsubscribed


async def test_request_many_rejects_negative_limits() -> None:
    with pytest.raises(ValueError, match="max_wait"):
        await request_many(FakeClient(), "work", max_wait=-1)
    with pytest.raises(ValueError, match="stall_wait"):
        await request_many(FakeClient(), "work", stall_wait=-1)
    with pytest.raises(ValueError, match="max_messages"):
        await request_many(FakeClient(), "work", max_messages=-1)
