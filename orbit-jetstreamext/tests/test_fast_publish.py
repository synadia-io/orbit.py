"""Live nats-server tests for fast-ingest batch publishing."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest
from nats.client.message import Message

from orbit.jetstreamext import (
    FastPublishClosedError,
    FastPublishEmptyBatchError,
    FastPublishFlowError,
    FastPublishNotEnabledError,
    GapMode,
    fast_publish,
)

if TYPE_CHECKING:
    from nats.jetstream import JetStream, Stream

pytestmark = pytest.mark.usefixtures("require_fast_publish_server")


async def _create_fast_stream(js: JetStream, name: str, subjects: list[str]) -> Stream:
    return await js.create_stream(name=name, subjects=subjects, allow_batched=True)


async def test_fast_publish_add_and_commit(jetstream: JetStream) -> None:
    stream = await _create_fast_stream(jetstream, "FP_BASIC", ["fp.basic.>"])
    publisher = fast_publish(jetstream)

    first = await publisher.add("fp.basic.1", b"one")
    second = await publisher.add_message(Message("fp.basic.2", b"two"))
    ack = await publisher.commit("fp.basic.3", b"three")

    assert first.batch_sequence == 1
    assert second.batch_sequence == 2
    assert ack.stream == "FP_BASIC"
    assert ack.batch_id == publisher.batch_id
    assert ack.batch_size == 3
    assert publisher.size == 3
    assert publisher.is_closed
    assert (await stream.get_info()).state.messages == 3

    with pytest.raises(FastPublishClosedError):
        await publisher.add("fp.basic.4", b"four")


async def test_fast_publish_close_does_not_store_eob(jetstream: JetStream) -> None:
    stream = await _create_fast_stream(jetstream, "FP_CLOSE", ["fp.close.>"])
    publisher = fast_publish(jetstream)
    await publisher.add("fp.close.1", b"one")
    await publisher.add("fp.close.2", b"two")

    ack = await publisher.close()

    assert ack.batch_size == 2
    assert publisher.size == 2
    assert (await stream.get_info()).state.messages == 2


async def test_fast_publish_close_empty_errors(jetstream: JetStream) -> None:
    await _create_fast_stream(jetstream, "FP_EMPTY", ["fp.empty.>"])
    publisher = fast_publish(jetstream)
    with pytest.raises(FastPublishEmptyBatchError):
        await publisher.close()
    assert not publisher.is_closed


async def test_fast_publish_single_message_commit(jetstream: JetStream) -> None:
    stream = await _create_fast_stream(jetstream, "FP_ONE", ["fp.one.>"])
    publisher = fast_publish(jetstream)

    ack = await publisher.commit("fp.one.only", b"one")

    assert ack.batch_size == 1
    assert publisher.size == 1
    assert (await stream.get_info()).state.messages == 1


async def test_fast_publish_stalls_at_each_flow_boundary(jetstream: JetStream) -> None:
    await _create_fast_stream(jetstream, "FP_FLOW", ["fp.flow.>"])
    publisher = fast_publish(jetstream, flow=1, max_outstanding_acks=1, ack_timeout=5)

    for index in range(25):
        progress = await publisher.add("fp.flow.msg", str(index).encode())
        assert progress.batch_sequence == index + 1

    ack = await publisher.commit("fp.flow.done", b"done")
    assert ack.batch_size == 26


async def test_fast_publish_gap_ok_mode(jetstream: JetStream) -> None:
    await _create_fast_stream(jetstream, "FP_GAP_OK", ["fp.gap.>"])
    publisher = fast_publish(jetstream, gap_mode=GapMode.OK)
    for index in range(5):
        await publisher.add("fp.gap.msg", str(index).encode())
    assert (await publisher.close()).batch_size == 5


async def test_fast_publish_large_batch_exceeds_atomic_limit(jetstream: JetStream) -> None:
    stream = await _create_fast_stream(jetstream, "FP_LARGE", ["fp.large.>"])
    publisher = fast_publish(jetstream, flow=100, max_outstanding_acks=2, ack_timeout=10)
    for index in range(1_500):
        await publisher.add("fp.large.msg", str(index).encode())

    ack = await publisher.close()

    assert ack.batch_size == 1_500
    assert publisher.size == 1_500
    assert (await stream.get_info()).state.messages == 1_500


async def test_fast_publish_not_enabled_is_typed(jetstream: JetStream) -> None:
    await jetstream.create_stream(name="FP_DISABLED", subjects=["fp.disabled.>"])
    publisher = fast_publish(jetstream, ack_timeout=2)
    with pytest.raises(FastPublishNotEnabledError):
        await publisher.add("fp.disabled.msg", b"data")


async def test_fast_publish_flow_error_is_typed(jetstream: JetStream) -> None:
    await _create_fast_stream(jetstream, "FP_ERROR", ["fp.error.>"])
    publisher = fast_publish(jetstream, flow=1, max_outstanding_acks=1)
    with pytest.raises(FastPublishFlowError) as raised:
        await publisher.add(
            "fp.error.bad",
            b"bad",
            headers={"Nats-Expected-Last-Sequence": "99"},
        )
    assert raised.value.error_code == 10071
    assert raised.value.publish_ack is None


async def test_fail_mode_flow_error_retains_terminal_publish_ack(jetstream: JetStream) -> None:
    stream = await _create_fast_stream(jetstream, "FP_ERROR_ACK", ["fp.error.ack.>"])
    publisher = fast_publish(jetstream, flow=1, max_outstanding_acks=1, ack_timeout=5)
    await publisher.add("fp.error.ack.first", b"first")

    with pytest.raises(FastPublishFlowError) as raised:
        await publisher.add(
            "fp.error.ack.bad",
            b"bad",
            headers={"Nats-Expected-Last-Sequence": "99"},
        )

    error = raised.value
    assert error.error_code == 10071
    assert error.batch_sequence == 2
    assert error.publish_ack is not None
    assert error.publish_ack.batch_size == 1
    assert (await stream.get_info()).state.messages == 1


async def test_concurrent_fast_publishers_have_independent_inboxes(jetstream: JetStream) -> None:
    stream = await _create_fast_stream(jetstream, "FP_CONCURRENT", ["fp.concurrent.>"])

    async def publish(worker: int) -> tuple[str, int | None]:
        publisher = fast_publish(jetstream, flow=5, max_outstanding_acks=2)
        for index in range(20):
            await publisher.add("fp.concurrent.msg", f"{worker}:{index}".encode())
        ack = await publisher.commit("fp.concurrent.done", str(worker).encode())
        return publisher.batch_id, ack.batch_size

    results = await asyncio.gather(*(publish(worker) for worker in range(4)))
    assert len({batch_id for batch_id, _ in results}) == 4
    assert {size for _, size in results} == {21}
    assert (await stream.get_info()).state.messages == 84
