"""Integration fixtures: launch a local nats-server and connect.

The integration tests need a nats-server binary (2.11+ for batch direct get).
If none is available on PATH, the fixture skips the dependent tests so the unit
suite still runs everywhere.
"""

from __future__ import annotations

import asyncio
import re
import shutil
import socket
import subprocess
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from nats.client import connect
from nats.jetstream import JetStream
from nats.jetstream import new as new_jetstream

_NATS_SERVER = shutil.which("nats-server")


def _server_version() -> tuple[int, int, int] | None:
    if _NATS_SERVER is None:
        return None
    out = subprocess.run([_NATS_SERVER, "--version"], capture_output=True, text=True, check=False).stdout
    match = re.search(r"v(\d+)\.(\d+)\.(\d+)", out)
    if match is None:
        return None
    major, minor, patch = (int(g) for g in match.groups())
    return major, minor, patch


# Batch direct get requires nats-server 2.11+.
_VERSION = _server_version()
_BATCH_SUPPORTED = _VERSION is not None and _VERSION >= (2, 11, 0)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest_asyncio.fixture
async def jetstream(tmp_path: Path) -> AsyncIterator[JetStream]:
    server_bin = _NATS_SERVER
    if server_bin is None or not _BATCH_SUPPORTED:
        pytest.skip("nats-server 2.11+ is required for batch direct get integration tests")

    port = _free_port()
    proc = subprocess.Popen(
        [server_bin, "-js", "-p", str(port), "-sd", str(tmp_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"nats://127.0.0.1:{port}"
    client = None
    try:
        for _ in range(50):
            try:
                client = await connect(url)
                break
            except Exception:
                await asyncio.sleep(0.1)
        if client is None:
            pytest.skip("could not connect to nats-server")
        yield new_jetstream(client, strict=True)
    finally:
        if client is not None:
            await client.close()
        proc.terminate()
        proc.wait()
