"""Smoke tests for the runnable atomic publishing example."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest
from nats.client import connect
from nats.jetstream import new as new_jetstream
from nats.jetstream.errors import StreamNotFoundError

_PACKAGE_ROOT = Path(__file__).parents[1]
_EXAMPLE = _PACKAGE_ROOT / "examples" / "atomic_batch.py"


async def test_atomic_example_runs_and_cleans_up(atomic_server_url: str) -> None:
    environment = os.environ.copy()
    environment["NATS_URL"] = atomic_server_url
    environment["PYTHONPATH"] = str(_PACKAGE_ROOT / "src")

    result = subprocess.run(
        [sys.executable, str(_EXAMPLE)],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
        env=environment,
    )

    assert result.returncode == 0, result.stderr
    output = re.fullmatch(
        r"committed 3 messages to (?P<stream>ATOMIC_ORDERS_[0-9a-f]{32}) "
        r"at sequence 3 \(batch [0-9a-f]{32}\)\n",
        result.stdout,
    )
    assert output is not None

    client = await connect(atomic_server_url)
    try:
        with pytest.raises(StreamNotFoundError):
            await new_jetstream(client).get_stream_info(output["stream"])
    finally:
        await client.close()
