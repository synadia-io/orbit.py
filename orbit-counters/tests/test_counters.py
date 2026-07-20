"""Integration tests for orbit.counters against a live nats-server."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from orbit import counters
from orbit.counters import (
    Counter,
    CounterNotEnabledError,
    CounterNotFoundError,
    NoCounterForSubjectError,
)

if TYPE_CHECKING:
    from nats.jetstream import JetStream


async def _make_counter(js: JetStream, *, name: str, subjects: list[str]) -> Counter:
    stream = await js.create_stream(name=name, subjects=subjects, allow_msg_counter=True, allow_direct=True)
    return counters.from_stream(js, stream)


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


async def test_get_multiple_not_implemented(jetstream: JetStream) -> None:
    counter = await _make_counter(jetstream, name="C6", subjects=["c6.>"])
    with pytest.raises(NotImplementedError):
        counter.get_multiple(["c6.>"])
