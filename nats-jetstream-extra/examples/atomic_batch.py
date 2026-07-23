"""Publish a small all-or-nothing batch to JetStream."""

from __future__ import annotations

import asyncio
import os
import uuid

import nats.jetstream_extra as jetstream_extra
from nats.client import connect
from nats.jetstream import JetStream
from nats.jetstream import new as new_jetstream
from nats.jetstream.errors import StreamNotFoundError


async def publish_order(js: JetStream) -> None:
    run_id = uuid.uuid4().hex
    stream_name = f"ATOMIC_ORDERS_{run_id}"
    subject_prefix = f"orders.atomic.{run_id}"

    try:
        await js.create_stream(
            name=stream_name,
            subjects=[f"{subject_prefix}.>"],
            allow_atomic=True,
        )

        batch = jetstream_extra.batch_publish(js)
        await batch.add(f"{subject_prefix}.created", b'{"order_id":"1001"}')
        await batch.add(f"{subject_prefix}.reserved", b'{"order_id":"1001","items":2}')
        ack = await batch.commit(f"{subject_prefix}.confirmed", b'{"order_id":"1001"}')

        print(f"committed {ack.batch_size} messages to {ack.stream} at sequence {ack.sequence} (batch {ack.batch_id})")
    except BaseException as error:
        try:
            await js.delete_stream(stream_name)
        except StreamNotFoundError:
            pass
        except BaseException as cleanup_error:
            error.add_note(f"stream cleanup also failed: {cleanup_error}")
        raise
    else:
        await js.delete_stream(stream_name)


async def main() -> None:
    client = await connect(os.environ.get("NATS_URL", "nats://127.0.0.1:4222"))
    try:
        await publish_order(new_jetstream(client))
    except BaseException as error:
        try:
            await client.close()
        except BaseException as cleanup_error:
            error.add_note(f"client cleanup also failed: {cleanup_error}")
        raise
    else:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
