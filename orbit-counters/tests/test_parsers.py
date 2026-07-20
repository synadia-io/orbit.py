from __future__ import annotations

import pytest
from nats.client.message import Headers

from orbit.counters import (
    COUNTER_SOURCES_HEADER,
    InvalidCounterValueError,
    _parse_counter_value,
    _parse_sources,
)


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (b'{"val":"42"}', 42),
        (b'{"val":"-10"}', -10),
        (b'{"val":"0"}', 0),
        (b'{"val":"123456789012345678901234567890"}', 123456789012345678901234567890),
    ],
)
def test_parse_counter_value_ok(data: bytes, expected: int) -> None:
    assert _parse_counter_value(data) == expected


@pytest.mark.parametrize(
    "data",
    [
        b'{"val": invalid}',  # invalid JSON
        b'{"other":"42"}',  # missing val field
        b'{"val":"not-a-number"}',  # invalid number format
        b"",  # empty data
    ],
)
def test_parse_counter_value_error(data: bytes) -> None:
    with pytest.raises(InvalidCounterValueError):
        _parse_counter_value(data)


def _sources_header(value: str) -> Headers:
    return Headers({COUNTER_SOURCES_HEADER: value})


def test_parse_sources_none_when_absent() -> None:
    assert _parse_sources(None) is None
    assert _parse_sources(Headers({})) is None


def test_parse_sources_single() -> None:
    result = _parse_sources(_sources_header('{"source1":{"subject1":"10"}}'))
    assert result == {"source1": {"subject1": 10}}


def test_parse_sources_single_multi_subject() -> None:
    result = _parse_sources(_sources_header('{"source1":{"subject1":"10","subject2":"20"}}'))
    assert result == {"source1": {"subject1": 10, "subject2": 20}}


def test_parse_sources_multiple() -> None:
    result = _parse_sources(
        _sources_header('{"source1":{"subject1":"10"},"source2":{"subject2":"20","subject3":"90"}}')
    )
    assert result == {
        "source1": {"subject1": 10},
        "source2": {"subject2": 20, "subject3": 90},
    }


@pytest.mark.parametrize(
    "value",
    [
        "{",  # invalid JSON
        '{"source1":{"subject1":"not-a-number"}}',  # invalid value format
    ],
)
def test_parse_sources_error(value: str) -> None:
    with pytest.raises(InvalidCounterValueError):
        _parse_sources(_sources_header(value))
