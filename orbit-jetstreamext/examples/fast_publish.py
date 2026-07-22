"""Publish a small fast-ingest batch to a real nats-server 2.14+."""

from __future__ import annotations

import asyncio
import os
import secrets
import sys

from nats.client import connect
from nats.jetstream import new as new_jetstream
from nats.jetstream.errors import StreamNotFoundError

from orbit.jetstreamext import fast_publish


async def main() -> None:
    server = os.environ.get("NATS_URL", "nats://127.0.0.1:4222")
    run_id = secrets.token_hex(8)
    stream_name = f"ORBIT_FAST_EXAMPLE_{run_id.upper()}"
    subject = f"orbit.example.fast.{run_id}"
    client = await connect(server)
    js = new_jetstream(client, strict=True)
    try:
        await js.create_stream(name=stream_name, subjects=[subject], allow_batched=True)

        # The context owns the publisher's inbox and aborts it if this block
        # exits before commit. A successful commit closes it normally.
        async with fast_publish(js, flow=1, max_outstanding_acks=1) as batch:
            for item in ("one", "two"):
                progress = await batch.add(subject, item.encode())
                print(
                    f"published batch sequence {progress.batch_sequence}; "
                    f"server acknowledged through {progress.ack_sequence}"
                )

            ack = await batch.commit(subject, b"three")
            print(
                f"committed {ack.batch_size} messages to {ack.stream}; "
                f"batch={ack.batch_id}, stream sequence={ack.sequence}"
            )
    finally:
        original_error = sys.exception()
        cleanup_errors: list[BaseException] = []
        try:
            await js.delete_stream(stream_name)
        except StreamNotFoundError:
            pass
        except BaseException as error:
            cleanup_errors.append(error)
        try:
            await client.close()
        except BaseException as error:
            cleanup_errors.append(error)

        if original_error is not None:
            for error in cleanup_errors:
                original_error.add_note(f"example cleanup also failed: {error!r}")
        elif cleanup_errors:
            first_error = cleanup_errors[0]
            for error in cleanup_errors[1:]:
                first_error.add_note(f"additional example cleanup failure: {error!r}")
            raise first_error


if __name__ == "__main__":
    asyncio.run(main())
