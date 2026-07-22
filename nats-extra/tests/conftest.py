"""Integration fixtures for nats.extra."""

from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from nats.client import Client, connect

_NATS_SERVER = shutil.which("nats-server")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest_asyncio.fixture
async def client() -> AsyncIterator[Client]:
    server_bin = _NATS_SERVER
    if server_bin is None:
        pytest.skip("nats-server is required for integration tests")

    port = _free_port()
    proc = subprocess.Popen(
        [server_bin, "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    nc = None
    try:
        for _ in range(50):
            try:
                nc = await connect(f"nats://127.0.0.1:{port}")
                break
            except Exception:
                await asyncio.sleep(0.1)
        if nc is None:
            pytest.skip("could not connect to nats-server")
        yield nc
    finally:
        if nc is not None:
            await nc.close()
        proc.terminate()
        proc.wait()
