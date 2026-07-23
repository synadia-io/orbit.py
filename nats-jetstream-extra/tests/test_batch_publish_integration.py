"""Atomic batch behavior against a real nats-server."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any, cast

import pytest
from nats.jetstream_extra import (
    AtomicPublishDuplicateMessageIDError,
    AtomicPublishNotEnabledError,
    AtomicPublishTooManyInflightError,
    AtomicPublishUnsupportedHeaderError,
    BatchClosedError,
    BatchMessage,
    BatchPublishRequestError,
    BatchPublishServerError,
    BatchTooLargeError,
    EmptyBatchError,
    batch_publish,
    publish_batch,
)

if TYPE_CHECKING:
    from nats.jetstream import JetStream
    from nats.jetstream.stream import Stream


async def _wait_for_messages(stream: Stream, expected: int, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        if (await stream.get_info()).state.messages == expected:
            return
        if asyncio.get_running_loop().time() >= deadline:
            pytest.fail(f"stream did not reach {expected} messages before timeout")
        await asyncio.sleep(0.1)


async def test_atomic_batch_is_invisible_until_commit_and_closes(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="ATOMIC1", subjects=["atomic1.>"], allow_atomic=True)
    publisher = batch_publish(atomic_jetstream)

    await publisher.add("atomic1.one", b"one")
    await publisher.add("atomic1.two", b"two")
    assert (await stream.get_info()).state.messages == 0

    ack = await publisher.commit("atomic1.three", b"three")
    assert ack.stream == "ATOMIC1"
    assert ack.batch_id == publisher.batch_id
    assert ack.batch_size == 3
    assert ack.sequence == 3
    assert ack.domain is None
    assert ack.value is None
    assert publisher.is_closed
    assert (await stream.get_info()).state.messages == 3
    with pytest.raises(BatchClosedError):
        await publisher.add("atomic1.four", b"four")
    with pytest.raises(BatchClosedError):
        await publisher.commit("atomic1.four", b"four")


async def test_bulk_publish_preserves_order_and_all_headers(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="ATOMIC2", subjects=["atomic2.>"], allow_atomic=True)
    source_headers: dict[str, str | list[str]] = {"X-Test": "first", "X-Multi": ["a", "b"]}
    ack = await publish_batch(
        atomic_jetstream,
        [
            BatchMessage("atomic2.one", b"one", source_headers),
            BatchMessage("atomic2.two", b"two"),
            BatchMessage("atomic2.three", b"three", {"X-Final": "yes"}),
        ],
        ack_every=2,
    )

    assert source_headers == {"X-Test": "first", "X-Multi": ["a", "b"]}
    messages = [await stream.get_message(sequence) for sequence in range(1, 4)]
    assert [message.data for message in messages] == [b"one", b"two", b"three"]
    assert messages[0].headers is not None
    assert messages[0].headers.get("X-Test") == "first"
    assert messages[0].headers.get_all("X-Multi") == ["a", "b"]
    assert messages[-1].headers is not None
    assert messages[-1].headers.get("X-Final") == "yes"
    for sequence, message in enumerate(messages, 1):
        assert message.headers is not None
        assert message.headers.get("Nats-Batch-Id") == ack.batch_id
        assert message.headers.get("Nats-Batch-Sequence") == str(sequence)
    assert messages[0].headers.get("Nats-Batch-Commit") is None
    assert messages[1].headers is not None
    assert messages[1].headers.get("Nats-Batch-Commit") is None
    assert messages[-1].headers.get("Nats-Batch-Commit") == "1"


async def test_message_ttl_expires_only_tagged_batch_message(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(
        name="ATOMIC_TTL",
        subjects=["atomic-ttl.>"],
        allow_atomic=True,
        allow_msg_ttl=True,
    )
    publisher = batch_publish(atomic_jetstream)
    await publisher.add("atomic-ttl.permanent", b"one")
    await publisher.add("atomic-ttl.expiring", b"two", headers={"Nats-TTL": "1s"})
    ack = await publisher.commit("atomic-ttl.final", b"three")

    assert ack.batch_size == 3
    assert (await stream.get_info()).state.messages == 3
    await _wait_for_messages(stream, 2, timeout=5.0)


async def test_supported_expectation_headers_commit(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="EXPECT", subjects=["expect.>"], allow_atomic=True)
    seed = await atomic_jetstream.publish("expect.seed", b"seed")
    assert seed.sequence == 1

    publisher = batch_publish(atomic_jetstream)
    await publisher.add(
        "expect.one",
        b"one",
        headers={"Nats-Expected-Stream": "EXPECT", "Nats-Expected-Last-Sequence": "1"},
    )
    await publisher.add(
        "expect.two",
        b"two",
        headers={"Nats-Expected-Last-Subject-Sequence": "0"},
    )
    ack = await publisher.commit(
        "expect.three",
        b"three",
        headers={
            "Nats-Expected-Last-Subject-Sequence": "1",
            "Nats-Expected-Last-Subject-Sequence-Subject": "expect.seed",
        },
    )

    assert ack.batch_size == 3
    assert (await stream.get_info()).state.messages == 4


@pytest.mark.parametrize(
    ("headers", "error_code"),
    [
        ({"Nats-Expected-Stream": "WRONG"}, 10060),
        ({"Nats-Expected-Last-Sequence": "5"}, 10071),
        ({"Nats-Expected-Last-Subject-Sequence": "5"}, 10071),
    ],
)
async def test_invalid_expectations_are_exact_and_atomic(
    atomic_jetstream: JetStream,
    headers: dict[str, str],
    error_code: int,
) -> None:
    stream = await atomic_jetstream.create_stream(name="EXPECT_BAD", subjects=["expect-bad.>"], allow_atomic=True)
    publisher = batch_publish(atomic_jetstream)

    with pytest.raises(BatchPublishServerError) as raised:
        await publisher.commit("expect-bad.one", b"one", headers=headers)

    assert raised.value.error_code == error_code
    assert publisher.is_closed
    assert (await stream.get_info()).state.messages == 0


async def test_expected_last_sequence_only_on_first_and_recovers(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="EXPECT_FIRST", subjects=["expect-first.>"], allow_atomic=True)
    publisher = batch_publish(atomic_jetstream)
    await publisher.add("expect-first.one", b"one", headers={"Nats-Expected-Last-Sequence": "0"})

    with pytest.raises(AtomicPublishUnsupportedHeaderError):
        await publisher.add("expect-first.two", b"two", headers={"Nats-Expected-Last-Sequence": "0"})

    assert not publisher.is_closed
    assert publisher.size == 1
    ack = await publisher.commit("expect-first.two", b"two")
    assert ack.batch_size == 2
    assert (await stream.get_info()).state.messages == 2


async def test_failed_commit_stores_none_and_closes(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="ATOMIC3", subjects=["atomic3.>"], allow_atomic=True)
    publisher = batch_publish(atomic_jetstream, ack_first=False)
    await publisher.add("atomic3.one", b"one", headers={"Nats-Expected-Last-Sequence": "10"})
    await publisher.add("atomic3.two", b"two")

    with pytest.raises(BatchPublishServerError) as raised:
        await publisher.commit("atomic3.three", b"three")

    assert raised.value.error_code == 10071
    assert publisher.is_closed
    assert (await stream.get_info()).state.messages == 0
    with pytest.raises(BatchClosedError):
        await publisher.add("atomic3.four", b"four")
    with pytest.raises(BatchClosedError):
        await publisher.commit("atomic3.four", b"four")


async def test_atomic_publish_disabled_is_typed_and_closed(atomic_jetstream: JetStream) -> None:
    await atomic_jetstream.create_stream(name="ATOMIC4", subjects=["atomic4.>"])
    publisher = batch_publish(atomic_jetstream)

    with pytest.raises(AtomicPublishNotEnabledError) as raised:
        await publisher.add("atomic4.one", b"one")

    assert raised.value.error_code == 10174
    assert publisher.is_closed
    with pytest.raises(BatchClosedError):
        await publisher.add("atomic4.two", b"two")
    with pytest.raises(BatchClosedError):
        await publisher.commit("atomic4.two", b"two")


async def test_ack_first_and_ack_every_surface_disabled_stream_errors(atomic_jetstream: JetStream) -> None:
    await atomic_jetstream.create_stream(name="FLOW_DISABLED", subjects=["flow-disabled.>"])

    first = batch_publish(atomic_jetstream)
    with pytest.raises(AtomicPublishNotEnabledError):
        await first.add("flow-disabled.one", b"one")

    periodic = batch_publish(atomic_jetstream, ack_first=False, ack_every=2)
    await periodic.add("flow-disabled.one", b"one")
    with pytest.raises(AtomicPublishNotEnabledError):
        await periodic.add("flow-disabled.two", b"two")
    assert periodic.is_closed


async def test_discard_closes_all_operations_and_leaves_stream_empty(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="ATOMIC5", subjects=["atomic5.>"], allow_atomic=True)
    publisher = batch_publish(atomic_jetstream)
    await publisher.add("atomic5.one", b"one")
    await publisher.add("atomic5.two", b"two")
    publisher.discard()

    assert publisher.is_closed
    assert (await stream.get_info()).state.messages == 0
    with pytest.raises(BatchClosedError):
        publisher.discard()
    with pytest.raises(BatchClosedError):
        await publisher.add("atomic5.three", b"three")
    with pytest.raises(BatchClosedError):
        await publisher.commit("atomic5.three", b"three")


async def test_flow_control_large_batch(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="ATOMIC6", subjects=["atomic6.>"], allow_atomic=True)
    publisher = batch_publish(atomic_jetstream, ack_every=25)
    for sequence in range(100):
        await publisher.add("atomic6.data", str(sequence).encode())

    ack = await publisher.commit("atomic6.final", b"done")
    assert ack.batch_size == 101
    assert (await stream.get_info()).state.messages == 101


async def test_incremental_1000_boundary_and_1001_rejection(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="LIMIT_INC", subjects=["limit-inc.>"], allow_atomic=True)
    publisher = batch_publish(atomic_jetstream, ack_first=False)
    for _ in range(999):
        await publisher.add("limit-inc.data", b"data")
    ack = await publisher.commit("limit-inc.final", b"final")
    assert ack.batch_size == 1000

    oversized = batch_publish(atomic_jetstream, ack_first=False)
    for _ in range(1000):
        await oversized.add("limit-inc.oversized", b"data")
    with pytest.raises(BatchTooLargeError):
        await oversized.add("limit-inc.oversized", b"too-many")
    with pytest.raises(BatchTooLargeError):
        await oversized.commit("limit-inc.final", b"too-many")
    assert not oversized.is_closed
    assert (await stream.get_info()).state.messages == 1000
    oversized.discard()


async def test_bulk_1000_boundary_and_1001_rejection(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="LIMIT_BULK", subjects=["limit-bulk.>"], allow_atomic=True)
    messages = [BatchMessage("limit-bulk.data", str(index).encode()) for index in range(1000)]
    ack = await publish_batch(atomic_jetstream, messages, ack_first=False)
    assert ack.batch_size == 1000

    oversized = [BatchMessage("limit-bulk.oversized", b"data") for _ in range(1001)]
    with pytest.raises(BatchTooLargeError):
        await publish_batch(atomic_jetstream, oversized, ack_first=False)
    assert (await stream.get_info()).state.messages == 1000


async def test_inflight_limit_and_timeout_reclaims_slot(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="INFLIGHT", subjects=["inflight.>"], allow_atomic=True)
    batches = []
    for _ in range(50):
        publisher = batch_publish(atomic_jetstream)
        await publisher.add("inflight.pending", b"data")
        batches.append(publisher)

    overflow = batch_publish(atomic_jetstream)
    with pytest.raises(AtomicPublishTooManyInflightError) as raised:
        await overflow.add("inflight.pending", b"overflow")
    assert raised.value.error_code == 10210
    assert overflow.is_closed

    for publisher in batches:
        publisher.discard()

    deadline = asyncio.get_running_loop().time() + 15.0
    while True:
        recovered = batch_publish(atomic_jetstream)
        try:
            await recovered.add("inflight.recovered", b"one")
        except AtomicPublishTooManyInflightError:
            if asyncio.get_running_loop().time() >= deadline:
                pytest.fail("server did not reclaim an expired atomic-batch slot")
            await asyncio.sleep(0.25)
            continue
        break
    ack = await recovered.commit("inflight.recovered", b"two")
    assert ack.batch_size == 2
    assert (await stream.get_info()).state.messages == 2


async def test_unique_message_ids_commit(atomic_message_id_jetstream: JetStream) -> None:
    stream = await atomic_message_id_jetstream.create_stream(name="ATOMIC7", subjects=["atomic7.>"], allow_atomic=True)
    publisher = batch_publish(atomic_message_id_jetstream)
    await publisher.add("atomic7.one", b"one", headers={"Nats-Msg-Id": "message-1"})
    await publisher.add("atomic7.two", b"two", headers={"Nats-Msg-Id": "message-2"})
    ack = await publisher.commit("atomic7.three", b"three", headers={"Nats-Msg-Id": "message-3"})

    assert ack.batch_size == 3
    assert (await stream.get_info()).state.messages == 3
    for sequence, message_id in enumerate(("message-1", "message-2", "message-3"), 1):
        message = await stream.get_message(sequence)
        assert message.headers is not None
        assert message.headers.get("Nats-Msg-Id") == message_id


async def test_duplicate_message_ids_are_typed(atomic_message_id_jetstream: JetStream) -> None:
    stream = await atomic_message_id_jetstream.create_stream(name="ATOMIC8", subjects=["atomic8.>"], allow_atomic=True)
    publisher = batch_publish(atomic_message_id_jetstream, ack_every=1)
    await publisher.add("atomic8.one", b"one", headers={"Nats-Msg-Id": "duplicate"})
    await publisher.add("atomic8.two", b"two", headers={"Nats-Msg-Id": "duplicate"})

    with pytest.raises(AtomicPublishDuplicateMessageIDError) as raised:
        await publisher.commit("atomic8.three", b"three", headers={"Nats-Msg-Id": "message-3"})

    assert raised.value.error_code == 10201
    assert publisher.is_closed
    assert (await stream.get_info()).state.messages == 0


async def test_batch_ids_are_unique(atomic_jetstream: JetStream) -> None:
    await atomic_jetstream.create_stream(name="UNIQUE", subjects=["unique.>"], allow_atomic=True)
    acknowledgements = []
    for index in range(3):
        acknowledgements.append(
            await publish_batch(atomic_jetstream, [BatchMessage(f"unique.{index}", str(index).encode())])
        )
    assert len({ack.batch_id for ack in acknowledgements}) == 3
    assert all(ack.batch_id for ack in acknowledgements)


async def test_bulk_accepts_sync_async_and_single_iterables(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="ITERABLES", subjects=["iterables.>"], allow_atomic=True)

    def sync_messages() -> Any:
        yield BatchMessage("iterables.sync-one", b"one")
        yield BatchMessage("iterables.sync-two", b"two")

    async def async_messages() -> AsyncIterator[BatchMessage]:
        yield BatchMessage("iterables.async-one", b"three")
        await asyncio.sleep(0)
        yield BatchMessage("iterables.async-two", b"four")

    sync_ack = await publish_batch(atomic_jetstream, sync_messages())
    async_ack = await publish_batch(atomic_jetstream, async_messages())
    single_ack = await publish_batch(atomic_jetstream, [BatchMessage("iterables.single", b"five")])

    assert (sync_ack.batch_size, async_ack.batch_size, single_ack.batch_size) == (2, 2, 1)
    assert (await stream.get_info()).state.messages == 5
    single = await stream.get_message(5)
    assert single.headers is not None
    assert single.headers.get("Nats-Batch-Sequence") == "1"
    assert single.headers.get("Nats-Batch-Commit") == "1"


async def test_bulk_rejects_empty_and_first_wrong_item(atomic_jetstream: JetStream) -> None:
    with pytest.raises(EmptyBatchError):
        await publish_batch(atomic_jetstream, [])
    with pytest.raises(TypeError, match="BatchMessage"):
        await publish_batch(atomic_jetstream, cast("Any", [("events.one", b"one")]))


async def test_bulk_late_invalid_and_raising_iterables_never_commit(atomic_jetstream: JetStream) -> None:
    stream = await atomic_jetstream.create_stream(name="ITER_ERRORS", subjects=["iter-errors.>"], allow_atomic=True)

    late_invalid = cast(
        "Any",
        [BatchMessage("iter-errors.one", b"one"), BatchMessage("iter-errors.two", b"two"), ("bad", b"item")],
    )
    with pytest.raises(TypeError, match="BatchMessage"):
        await publish_batch(atomic_jetstream, late_invalid)

    async def raising_messages() -> AsyncIterator[BatchMessage]:
        yield BatchMessage("iter-errors.three", b"three")
        yield BatchMessage("iter-errors.four", b"four")
        raise RuntimeError("source failed")

    with pytest.raises(RuntimeError, match="source failed"):
        await publish_batch(atomic_jetstream, raising_messages())

    assert (await stream.get_info()).state.messages == 0
    ack = await publish_batch(atomic_jetstream, [BatchMessage("iter-errors.recovery", b"ok")])
    assert ack.batch_size == 1
    assert (await stream.get_info()).state.messages == 1


async def test_no_matching_stream_status_is_typed(atomic_jetstream: JetStream) -> None:
    publisher = batch_publish(atomic_jetstream)

    with pytest.raises(BatchPublishRequestError) as raised:
        await publisher.add("atomic-unmatched.one", b"one")

    cause = raised.value.__cause__
    assert cause is not None
    assert getattr(cause, "status", None) == "503"
    assert publisher.is_closed
