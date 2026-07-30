"""Unit tests for the pure response parsing in nats.jetstream_extra."""

from __future__ import annotations

from datetime import timezone

import pytest

from nats.client.message import Headers, Message, Status
from nats.jetstream_extra import (
    BatchUnsupportedError,
    InvalidResponseError,
    NoMessagesError,
    _convert,
    _is_eob,
    _parse_timestamp,
)


def _msg(*, data: bytes = b"", headers: dict | None = None, status: Status | None = None) -> Message:
    return Message(
        subject="_INBOX.x",
        data=data,
        headers=Headers(headers) if headers is not None else None,
        status=status,
    )


def _direct_headers(**overrides: str) -> dict[str, str]:
    headers = {
        "Nats-Stream": "EVENTS",
        "Nats-Subject": "events.a",
        "Nats-Sequence": "7",
        "Nats-Time-Stamp": "2026-07-14T12:00:00.123456789Z",
        "Nats-Num-Pending": "3",
        "Nats-Last-Sequence": "4",
    }
    headers.update(overrides)
    return headers


def test_parse_timestamp_truncates_nanoseconds() -> None:
    parsed = _parse_timestamp("2026-07-14T12:00:00.123456789Z")
    assert parsed.year == 2026
    assert parsed.microsecond == 123456
    assert parsed.tzinfo == timezone.utc


def test_parse_timestamp_without_fraction() -> None:
    parsed = _parse_timestamp("2026-07-14T12:00:00Z")
    assert parsed.microsecond == 0
    assert parsed.tzinfo == timezone.utc


def test_is_eob_true_for_204_eob() -> None:
    assert _is_eob(_msg(status=Status(code="204", description="EOB")))


def test_is_eob_false_for_data_message() -> None:
    assert not _is_eob(_msg(data=b"x", headers=_direct_headers()))


def test_is_eob_false_for_204_with_data() -> None:
    assert not _is_eob(_msg(data=b"x", status=Status(code="204", description="EOB")))


def test_convert_full_message() -> None:
    msg = _convert(_msg(data=b"payload", headers=_direct_headers()))
    assert msg.subject == "events.a"
    assert msg.sequence == 7
    assert msg.data == b"payload"
    assert msg.num_pending == 3
    assert msg.last_sequence == 4
    assert msg.time.microsecond == 123456


def test_convert_optional_last_sequence_absent() -> None:
    headers = _direct_headers()
    del headers["Nats-Last-Sequence"]
    assert _convert(_msg(data=b"payload", headers=headers)).last_sequence is None


def test_convert_no_messages_status_raises() -> None:
    with pytest.raises(NoMessagesError):
        _convert(_msg(status=Status(code="404", description="No Messages")))


def test_convert_missing_num_pending_is_unsupported() -> None:
    headers = _direct_headers()
    del headers["Nats-Num-Pending"]
    with pytest.raises(BatchUnsupportedError):
        _convert(_msg(data=b"payload", headers=headers))


def test_convert_without_headers_raises() -> None:
    with pytest.raises(InvalidResponseError):
        _convert(_msg(data=b"payload"))


@pytest.mark.parametrize("missing", ["Nats-Stream", "Nats-Subject", "Nats-Time-Stamp"])
def test_convert_missing_required_header_raises(missing: str) -> None:
    headers = _direct_headers()
    del headers[missing]
    with pytest.raises(InvalidResponseError):
        _convert(_msg(data=b"payload", headers=headers))


def test_convert_invalid_sequence_raises() -> None:
    with pytest.raises(InvalidResponseError):
        _convert(_msg(data=b"payload", headers=_direct_headers(**{"Nats-Sequence": "not-a-number"})))
