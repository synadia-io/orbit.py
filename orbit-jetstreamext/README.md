# orbit-jetstreamext

JetStream extensions for NATS: atomic batch publishing and batch direct get.

Atomic batch publishing sends up to 1,000 messages that become visible in a
stream together. If validation or commit fails, none of the messages are
stored. It requires `allow_atomic` on the stream and nats-server 2.12+.

Batch direct get fetches several stored messages with a single request instead
of one round-trip per message. The server streams the matching messages back on
a reply inbox and ends the stream with an end-of-batch sentinel.

Batch direct get requires `allow_direct` and nats-server 2.11+.

## Install

```sh
pip install orbit-jetstreamext
```

## Usage

```python
from nats.client import connect
from nats.jetstream import new as jetstream
from orbit import jetstreamext

nc = await connect("nats://localhost:4222")
js = jetstream(nc)

# Atomic all-or-nothing publishing.
await js.create_stream(name="EVENTS", subjects=["events.>"], allow_atomic=True)
batch = jetstreamext.batch_publish(js, ack_every=100)
await batch.add("events.order", b"order-123")
await batch.add("events.payment", b"payment-456")
ack = await batch.commit("events.complete", b"done")
print(ack.batch_id, ack.batch_size)

# Last message for each of several subjects (wildcards allowed).
async for msg in jetstreamext.get_last_msgs_for(js, "EVENTS", ["events.a", "events.b"]):
    print(msg.subject, msg.sequence, msg.data)

# A batch of messages from a starting point.
async for msg in jetstreamext.get_batch(js, "EVENTS", batch=100, seq=1):
    print(msg.sequence, msg.data)
```

### Runnable atomic example

[`examples/atomic_batch.py`](examples/atomic_batch.py) creates a uniquely named,
atomic-enabled stream, commits a three-message order batch against a real
nats-server 2.12+, and removes the example stream afterward. Start the server:

```sh
nats-server -js
```

Then run the example from the repository root:

```sh
uv run --package orbit-jetstreamext python orbit-jetstreamext/examples/atomic_batch.py
```

It connects to `nats://127.0.0.1:4222` by default. Set `NATS_URL` to use a
different server.

## API

### `batch_publish(js, *, ack_first=True, ack_every=None, timeout=5.0)`

Create a `BatchPublisher` for one atomic batch. `add(subject, data, headers=...)`
sends non-final messages, `commit(subject, data, headers=...)` sends and stores
the final message, and `discard()` closes the publisher without committing.
`size`, `batch_id`, and `is_closed` expose its current state.

The first message requests a flow-control acknowledgement by default so stream
configuration errors surface early. `ack_every=N` additionally waits after
every Nth message. `timeout` applies to flow-control and commit acks. A failure
after I/O begins closes the publisher because its server-side state is no
longer certain; validation failures leave it usable.

The publisher preserves custom headers but manages `Nats-Batch-*` itself.
`Nats-Msg-Id` is allowed, but every message ID in a batch must be unique
(supported by nats-server 2.12.1+). `Nats-Expected-Last-Msg-Id` is unsupported.
`Nats-Expected-Last-Sequence` may be supplied only on the first message.

### `publish_batch(js, messages, *, ack_first=True, ack_every=None, timeout=5.0)`

Publish a regular or async iterable of `BatchMessage` values as one atomic
batch. The helper buffers one item so the last input message carries the commit
marker; it does not add a synthetic message.

```python
ack = await jetstreamext.publish_batch(
    js,
    [
        jetstreamext.BatchMessage("events.a", b"one"),
        jetstreamext.BatchMessage("events.b", b"two", {"X-Source": "import"}),
    ],
)
```

### `BatchAck`

A validated commit acknowledgement: `stream`, `sequence`, `batch_id`,
`batch_size`, optional `domain`, and optional counter-stream `value`.

### `get_batch(js, stream, batch, *, seq=None, next_by_subject=None, start_time=None, max_bytes=None, timeout=5.0)`

Fetch up to `batch` messages from `stream`. Fetching starts at the first message
unless `seq` or `start_time` (mutually exclusive) is given. `next_by_subject`
restricts matches to a subject (wildcards allowed) and `max_bytes` caps the total
size the server returns. Returns an async iterator of `RawStreamMsg`.

### `get_last_msgs_for(js, stream, subjects, *, batch=None, up_to_seq=None, up_to_time=None, timeout=5.0)`

Fetch the last message for each subject in `subjects` (wildcards allowed; the
server matches at most 1024). `up_to_seq` or `up_to_time` (mutually exclusive)
fetches the last message at or before a point rather than the latest; `batch`
caps how many messages are returned. Returns an async iterator of `RawStreamMsg`.

### `RawStreamMsg`

A stored message: `subject`, `sequence`, `data`, `time`, `headers`,
`num_pending`, `last_sequence`.

## Errors

Atomic publishing has a `BatchPublishError` hierarchy. Common client-side
errors are `BatchClosedError`, `BatchTooLargeError`, `EmptyBatchError`,
`BatchPublishRequestError`, and `InvalidBatchAckError`. Server protocol errors
map to specific types such as `AtomicPublishNotEnabledError`,
`AtomicPublishUnsupportedHeaderError`, `AtomicPublishDuplicateMessageIDError`, and
`AtomicPublishTooManyInflightError`; unknown server errors use
`BatchPublishServerError` and retain `code`, `error_code`, and `description`.

- `SubjectRequiredError` — no subjects passed to `get_last_msgs_for`.
- `InvalidOptionError` — an invalid or conflicting option.
- `NoMessagesError` — no messages matched the request.
- `BatchUnsupportedError` — the server predates batch direct get (2.11+).
- `InvalidResponseError` — a server response that could not be parsed.

All inherit from `JetStreamExtError`.
