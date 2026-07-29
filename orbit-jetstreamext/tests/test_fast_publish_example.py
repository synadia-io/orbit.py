"""Smoke-test the runnable fast-publish example against a real server."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from nats.jetstream.errors import StreamNotFoundError

if TYPE_CHECKING:
    from nats.jetstream import JetStream


async def test_fast_publish_example(fast_jetstream: JetStream, fast_server_url: str) -> None:
    sentinel = await fast_jetstream.create_stream(
        name="ORBIT_FAST_EXAMPLE",
        subjects=["orbit.example.fast.sentinel"],
    )
    package_root = Path(__file__).parents[1]
    environment = os.environ.copy()
    environment["NATS_URL"] = fast_server_url
    environment["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(package_root / "src"), environment.get("PYTHONPATH")) if part
    )

    result = subprocess.run(
        [sys.executable, str(package_root / "examples" / "fast_publish.py")],
        cwd=package_root.parent,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "published batch sequence 1; server acknowledged through 1" in result.stdout
    assert "published batch sequence 2; server acknowledged through 2" in result.stdout
    match = re.search(r"committed 3 messages to (ORBIT_FAST_EXAMPLE_[0-9A-F]{16});", result.stdout)
    assert match is not None, result.stdout
    generated_stream = match.group(1)

    with pytest.raises(StreamNotFoundError):
        await fast_jetstream.get_stream(generated_stream)
    assert (await sentinel.get_info()).config.name == "ORBIT_FAST_EXAMPLE"
