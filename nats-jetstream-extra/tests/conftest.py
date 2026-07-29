"""Integration fixtures: launch a local nats-server and connect."""

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
_ATOMIC_PUBLISH_SUPPORTED = _VERSION is not None and _VERSION >= (2, 12, 0)
_ATOMIC_MESSAGE_ID_SUPPORTED = _VERSION is not None and _VERSION >= (2, 12, 1)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest_asyncio.fixture
async def nats_server_url(tmp_path: Path) -> AsyncIterator[str]:
    server_bin = _NATS_SERVER
    if server_bin is None or not _BATCH_SUPPORTED:
        pytest.fail("nats-server 2.11+ is required for integration tests")

    port = _free_port()
    proc = subprocess.Popen(
        [server_bin, "-js", "-p", str(port), "-sd", str(tmp_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"nats://127.0.0.1:{port}"
    probe = None
    try:
        for _ in range(50):
            try:
                probe = await connect(url)
                break
            except Exception:
                await asyncio.sleep(0.1)
        if probe is None:
            pytest.fail("could not connect to nats-server")
        await probe.close()
        yield url
    finally:
        if probe is not None:
            await probe.close()
        proc.terminate()
        proc.wait()


@pytest_asyncio.fixture
async def jetstream(nats_server_url: str) -> AsyncIterator[JetStream]:
    client = await connect(nats_server_url)
    try:
        yield new_jetstream(client, strict=True)
    finally:
        await client.close()


@pytest_asyncio.fixture
async def atomic_server_url(nats_server_url: str) -> str:
    """A live nats-server URL with atomic publish support."""
    if not _ATOMIC_PUBLISH_SUPPORTED:
        pytest.fail("nats-server 2.12+ is required for atomic batch publishing")
    return nats_server_url


@pytest_asyncio.fixture
async def atomic_jetstream(jetstream: JetStream) -> JetStream:
    """A live JetStream context on a server with atomic publish support."""
    if not _ATOMIC_PUBLISH_SUPPORTED:
        pytest.fail("nats-server 2.12+ is required for atomic batch publishing")
    return jetstream


@pytest_asyncio.fixture
async def atomic_message_id_jetstream(atomic_jetstream: JetStream) -> JetStream:
    """A live context supporting message IDs inside atomic batches."""
    if not _ATOMIC_MESSAGE_ID_SUPPORTED:
        pytest.skip("nats-server 2.12.1+ is required for message IDs in atomic batches")
    return atomic_jetstream
