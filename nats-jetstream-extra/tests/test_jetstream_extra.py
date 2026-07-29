"""Integration tests for nats.jetstream_extra against a live nats-server."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

import nats.jetstream_extra as jetstream_extra
from nats.jetstream_extra import (
    InvalidOptionError,
    NoMessagesError,
    RawStreamMsg,
    SubjectRequiredError,
)

if TYPE_CHECKING:
    from nats.jetstream import JetStream


async def _seed(js: JetStream, *, name: str, subjects: list[str], messages: list[tuple[str, bytes]]) -> None:
    await js.create_stream(name=name, subjects=subjects, allow_direct=True)
    for subject, data in messages:
        await js.publish(subject, data)


async def _collect(iterator) -> list[RawStreamMsg]:
    return [msg async for msg in iterator]


async def test_get_batch_returns_messages_in_order(jetstream: JetStream) -> None:
    await _seed(
        jetstream,
        name="B1",
        subjects=["b1.>"],
        messages=[("b1.a", b"1"), ("b1.b", b"2"), ("b1.a", b"3")],
    )
    msgs = await _collect(jetstream_extra.get_batch(jetstream, "B1", batch=10))
    assert [m.sequence for m in msgs] == [1, 2, 3]
    assert [m.data for m in msgs] == [b"1", b"2", b"3"]


async def test_get_batch_respects_batch_size(jetstream: JetStream) -> None:
    await _seed(
        jetstream,
        name="B2",
        subjects=["b2.>"],
        messages=[("b2.a", b"1"), ("b2.a", b"2"), ("b2.a", b"3")],
    )
    msgs = await _collect(jetstream_extra.get_batch(jetstream, "B2", batch=2))
    assert len(msgs) == 2
    assert [m.sequence for m in msgs] == [1, 2]


async def test_get_batch_starts_at_seq(jetstream: JetStream) -> None:
    await _seed(
        jetstream,
        name="B3",
        subjects=["b3.>"],
        messages=[("b3.a", b"1"), ("b3.a", b"2"), ("b3.a", b"3")],
    )
    msgs = await _collect(jetstream_extra.get_batch(jetstream, "B3", batch=10, seq=2))
    assert [m.sequence for m in msgs] == [2, 3]


async def test_get_batch_filters_by_subject(jetstream: JetStream) -> None:
    await _seed(
        jetstream,
        name="B4",
        subjects=["b4.>"],
        messages=[("b4.a", b"1"), ("b4.b", b"2"), ("b4.a", b"3")],
    )
    msgs = await _collect(jetstream_extra.get_batch(jetstream, "B4", batch=10, next_by_subject="b4.a"))
    assert [m.subject for m in msgs] == ["b4.a", "b4.a"]
    assert [m.data for m in msgs] == [b"1", b"3"]


async def test_get_batch_no_messages_raises(jetstream: JetStream) -> None:
    await _seed(jetstream, name="B5", subjects=["b5.>"], messages=[])
    with pytest.raises(NoMessagesError):
        await _collect(jetstream_extra.get_batch(jetstream, "B5", batch=10))


async def test_get_last_msgs_for_returns_latest_per_subject(jetstream: JetStream) -> None:
    await _seed(
        jetstream,
        name="L1",
        subjects=["l1.>"],
        messages=[("l1.a", b"a1"), ("l1.b", b"b1"), ("l1.a", b"a2")],
    )
    msgs = await _collect(jetstream_extra.get_last_msgs_for(jetstream, "L1", ["l1.a", "l1.b"]))
    by_subject = {m.subject: m for m in msgs}
    assert by_subject["l1.a"].data == b"a2"
    assert by_subject["l1.b"].data == b"b1"


async def test_get_last_msgs_for_supports_wildcards(jetstream: JetStream) -> None:
    await _seed(
        jetstream,
        name="L2",
        subjects=["l2.>"],
        messages=[("l2.a", b"a1"), ("l2.b", b"b1")],
    )
    msgs = await _collect(jetstream_extra.get_last_msgs_for(jetstream, "L2", ["l2.>"]))
    assert {m.subject for m in msgs} == {"l2.a", "l2.b"}


async def test_get_last_msgs_for_up_to_seq(jetstream: JetStream) -> None:
    await _seed(
        jetstream,
        name="L3",
        subjects=["l3.>"],
        messages=[("l3.a", b"a1"), ("l3.a", b"a2"), ("l3.a", b"a3")],
    )
    msgs = await _collect(jetstream_extra.get_last_msgs_for(jetstream, "L3", ["l3.a"], up_to_seq=2))
    assert len(msgs) == 1
    assert msgs[0].data == b"a2"


async def test_get_last_msgs_for_empty_subjects_raises(jetstream: JetStream) -> None:
    with pytest.raises(SubjectRequiredError):
        jetstream_extra.get_last_msgs_for(jetstream, "L1", [])


async def test_get_batch_invalid_batch_raises(jetstream: JetStream) -> None:
    with pytest.raises(InvalidOptionError):
        jetstream_extra.get_batch(jetstream, "B1", batch=0)


async def test_get_batch_conflicting_start_raises(jetstream: JetStream) -> None:
    from datetime import datetime, timezone

    with pytest.raises(InvalidOptionError):
        jetstream_extra.get_batch(jetstream, "B1", batch=1, seq=1, start_time=datetime.now(timezone.utc))
