"""Request-many integration tests against a live nats-server."""

from __future__ import annotations

import asyncio

from nats.client import Client
from nats.extra import TerminationReason, request_many


async def test_request_many_collects_streamed_responses(client: Client) -> None:
    requests = await client.subscribe("work.stream")

    async def respond() -> None:
        request = await requests.next(timeout=1)
        assert request.reply is not None
        for payload in (b"one", b"two", b"three"):
            await client.publish(request.reply, payload)

    responder = asyncio.create_task(respond())
    responses = await request_many(client, "work.stream", b"go", max_messages=3)
    received = [message.data async for message in responses]
    await responder
    await requests.unsubscribe()

    assert received == [b"one", b"two", b"three"]
    assert responses.termination_reason is TerminationReason.MAX_MESSAGES


async def test_request_many_reports_no_responders(client: Client) -> None:
    responses = await request_many(client, "nobody.listens", max_wait=1)

    assert [message async for message in responses] == []
    assert responses.termination_reason is TerminationReason.NO_RESPONDERS
