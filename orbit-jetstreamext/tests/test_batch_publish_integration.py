"""Atomic batch publish tests against a live nats-server."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from orbit.jetstreamext import (
    AtomicPublishDuplicateMessageIDError,
    AtomicPublishNotEnabledError,
    BatchMessage,
    BatchPublishRequestError,
    BatchPublishServerError,
    batch_publish,
    get_batch,
    publish_batch,
)

if TYPE_CHECKING:
    from nats.jetstream import JetStream


async def test_atomic_batch_is_invisible_until_commit(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(
        name="ATOMIC1",
        subjects=["atomic1.>"],
        allow_atomic=True,
    )
    publisher = batch_publish(atomic_jetstream)

    await publisher.add("atomic1.one", b"one")
    await publisher.add("atomic1.two", b"two")
    assert (await stream.get_info()).state.messages == 0

    ack = await publisher.commit("atomic1.three", b"three")
    assert ack.stream == "ATOMIC1"
    assert ack.batch_id == publisher.batch_id
    assert ack.batch_size == 3
    assert ack.sequence == 3
    assert (await stream.get_info()).state.messages == 3


async def test_bulk_publish_preserves_order_and_headers(atomic_jetstream: JetStream) -> None:
    await atomic_jetstream.create_stream(
        name="ATOMIC2",
        subjects=["atomic2.>"],
        allow_atomic=True,
        allow_direct=True,
    )
    ack = await publish_batch(
        atomic_jetstream,
        [
            BatchMessage("atomic2.one", b"one", {"X-Test": "first"}),
            BatchMessage("atomic2.two", b"two"),
            BatchMessage("atomic2.three", b"three"),
        ],
        ack_every=2,
    )

    messages = [message async for message in get_batch(atomic_jetstream, "ATOMIC2", 10)]
    assert [message.data for message in messages] == [b"one", b"two", b"three"]
    assert messages[0].headers is not None
    assert messages[0].headers.get("X-Test") == "first"
    for sequence, message in enumerate(messages, 1):
        assert message.headers is not None
        assert message.headers.get("Nats-Batch-Id") == ack.batch_id
        assert message.headers.get("Nats-Batch-Sequence") == str(sequence)
    assert messages[-1].headers is not None
    assert messages[-1].headers.get("Nats-Batch-Commit") == "1"


async def test_failed_commit_stores_none_of_the_batch(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(
        name="ATOMIC3",
        subjects=["atomic3.>"],
        allow_atomic=True,
    )
    publisher = batch_publish(atomic_jetstream, ack_first=False)
    await publisher.add(
        "atomic3.one",
        b"one",
        headers={"Nats-Expected-Last-Sequence": "10"},
    )
    await publisher.add("atomic3.two", b"two")

    with pytest.raises(BatchPublishServerError):
        await publisher.commit("atomic3.three", b"three")

    assert (await stream.get_info()).state.messages == 0


async def test_atomic_publish_disabled_is_typed(atomic_jetstream: JetStream) -> None:
    await atomic_jetstream.create_stream(
        name="ATOMIC4",
        subjects=["atomic4.>"],
    )
    publisher = batch_publish(atomic_jetstream)

    with pytest.raises(AtomicPublishNotEnabledError) as raised:
        await publisher.add("atomic4.one", b"one")

    assert raised.value.error_code == 10174
    assert publisher.is_closed


async def test_discard_leaves_stream_empty(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(
        name="ATOMIC5",
        subjects=["atomic5.>"],
        allow_atomic=True,
    )
    publisher = batch_publish(atomic_jetstream)
    await publisher.add("atomic5.one", b"one")
    await publisher.add("atomic5.two", b"two")
    publisher.discard()

    assert publisher.is_closed
    assert (await stream.get_info()).state.messages == 0


async def test_flow_control_large_batch(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(
        name="ATOMIC6",
        subjects=["atomic6.>"],
        allow_atomic=True,
    )
    publisher = batch_publish(atomic_jetstream, ack_every=25)
    for sequence in range(100):
        await publisher.add("atomic6.data", str(sequence).encode())

    ack = await publisher.commit("atomic6.final", b"done")
    assert ack.batch_size == 101
    assert (await stream.get_info()).state.messages == 101


async def test_unique_message_ids_commit(atomic_message_id_jetstream: JetStream) -> None:
    stream = await atomic_message_id_jetstream.create_stream(
        name="ATOMIC7",
        subjects=["atomic7.>"],
        allow_atomic=True,
    )
    publisher = batch_publish(atomic_message_id_jetstream)
    await publisher.add("atomic7.one", b"one", headers={"Nats-Msg-Id": "message-1"})
    await publisher.add("atomic7.two", b"two", headers={"Nats-Msg-Id": "message-2"})
    ack = await publisher.commit("atomic7.three", b"three", headers={"Nats-Msg-Id": "message-3"})

    assert ack.batch_size == 3
    assert (await stream.get_info()).state.messages == 3


async def test_duplicate_message_ids_are_typed(atomic_message_id_jetstream: JetStream) -> None:
    stream = await atomic_message_id_jetstream.create_stream(
        name="ATOMIC8",
        subjects=["atomic8.>"],
        allow_atomic=True,
    )
    publisher = batch_publish(atomic_message_id_jetstream, ack_every=1)
    await publisher.add("atomic8.one", b"one", headers={"Nats-Msg-Id": "duplicate"})
    await publisher.add("atomic8.two", b"two", headers={"Nats-Msg-Id": "duplicate"})

    with pytest.raises(AtomicPublishDuplicateMessageIDError) as raised:
        await publisher.commit("atomic8.three", b"three", headers={"Nats-Msg-Id": "message-3"})

    assert raised.value.error_code == 10201
    assert publisher.is_closed
    assert (await stream.get_info()).state.messages == 0


async def test_no_matching_stream_status_is_typed(atomic_jetstream: JetStream) -> None:
    publisher = batch_publish(atomic_jetstream)

    with pytest.raises(BatchPublishRequestError) as raised:
        await publisher.add("atomic-unmatched.one", b"one")

    cause = raised.value.__cause__
    assert cause is not None
    assert getattr(cause, "status", None) == "503"
    assert publisher.is_closed
