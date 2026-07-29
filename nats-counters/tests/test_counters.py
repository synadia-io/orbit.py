"""Integration tests for nats.counters against a live nats-server."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from nats import counters
from nats.counters import (
    Counter,
    CounterNotEnabledError,
    CounterNotFoundError,
    Entry,
    NoCounterForSubjectError,
)
from nats.jetstream_extra import NoMessagesError

if TYPE_CHECKING:
    from nats.jetstream import JetStream


async def _make_counter(js: JetStream, *, name: str, subjects: list[str]) -> Counter:
    stream = await js.create_stream(name=name, subjects=subjects, allow_msg_counter=True, allow_direct=True)
    return counters.from_stream(js, stream)


async def _collect(counter: Counter, subjects: list[str]) -> dict[str, Entry]:
    return {entry.subject: entry async for entry in counter.get_multiple(subjects)}


async def test_add_returns_running_total(jetstream: JetStream) -> None:
    counter = await _make_counter(jetstream, name="C1", subjects=["c1.>"])
    assert await counter.add("c1.orders", 1) == 1
    assert await counter.add("c1.orders", 5) == 6
    assert await counter.add("c1.orders", -2) == 4


async def test_load_returns_current_value(jetstream: JetStream) -> None:
    counter = await _make_counter(jetstream, name="C2", subjects=["c2.>"])
    await counter.add("c2.clicks", 100)
    assert await counter.load("c2.clicks") == 100


async def test_get_returns_entry(jetstream: JetStream) -> None:
    counter = await _make_counter(jetstream, name="C3", subjects=["c3.>"])
    await counter.add("c3.items", 7)
    await counter.add("c3.items", 3)
    entry = await counter.get("c3.items")
    assert entry.subject == "c3.items"
    assert entry.value == 10
    assert entry.incr == 3  # most recent increment


async def test_add_handles_large_values(jetstream: JetStream) -> None:
    counter = await _make_counter(jetstream, name="C4", subjects=["c4.>"])
    big = 9999999999999999999999999999
    assert await counter.add("c4.big", big) == big


async def test_load_uninitialized_subject_raises(jetstream: JetStream) -> None:
    counter = await _make_counter(jetstream, name="C5", subjects=["c5.>"])
    with pytest.raises(NoCounterForSubjectError):
        await counter.load("c5.missing")


async def test_from_stream_requires_counter_enabled(jetstream: JetStream) -> None:
    stream = await jetstream.create_stream(name="PLAIN", subjects=["plain.>"])
    with pytest.raises(CounterNotEnabledError):
        counters.from_stream(jetstream, stream)


async def test_get_counter_missing_raises(jetstream: JetStream) -> None:
    with pytest.raises(CounterNotFoundError):
        await counters.get_counter(jetstream, "DOES_NOT_EXIST")


async def test_get_multiple_returns_entry_per_subject(jetstream: JetStream) -> None:
    counter = await _make_counter(jetstream, name="C6", subjects=["c6.>"])
    await counter.add("c6.a", 3)
    await counter.add("c6.a", 4)
    await counter.add("c6.b", 10)
    entries = await _collect(counter, ["c6.a", "c6.b"])
    assert entries["c6.a"].value == 7
    assert entries["c6.a"].incr == 4  # most recent increment, unlike orbit.go
    assert entries["c6.b"].value == 10


async def test_get_multiple_supports_wildcards(jetstream: JetStream) -> None:
    counter = await _make_counter(jetstream, name="C7", subjects=["c7.>"])
    await counter.add("c7.a", 1)
    await counter.add("c7.b", 2)
    entries = await _collect(counter, ["c7.>"])
    assert {s: e.value for s, e in entries.items()} == {"c7.a": 1, "c7.b": 2}


async def test_get_multiple_skips_missing_subjects(jetstream: JetStream) -> None:
    counter = await _make_counter(jetstream, name="C8", subjects=["c8.>"])
    await counter.add("c8.a", 5)
    entries = await _collect(counter, ["c8.a", "c8.missing"])
    assert set(entries) == {"c8.a"}


async def test_get_multiple_all_missing_raises(jetstream: JetStream) -> None:
    counter = await _make_counter(jetstream, name="C9", subjects=["c9.>"])
    with pytest.raises(NoMessagesError):
        await _collect(counter, ["c9.missing"])
