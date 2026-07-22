# orbit-jetstreamext

JetStream batch retrieval and publishing extensions for NATS.

Batch direct get fetches several stored messages with a single request instead
of one round-trip per message. The server streams the matching messages back on
a reply inbox and ends the stream with an end-of-batch sentinel.

Requires a stream configured with `allow_direct` and nats-server 2.11+.

Fast-ingest publishing sends non-atomic, immediately stored batches using one
persistent inbox, server-driven flow control, and flow-ack ping recovery. It
requires a stream configured with `allow_batched` and nats-server 2.14+.

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

# Last message for each of several subjects (wildcards allowed).
async for msg in jetstreamext.get_last_msgs_for(js, "EVENTS", ["events.a", "events.b"]):
    print(msg.subject, msg.sequence, msg.data)

# A batch of messages from a starting point.
async for msg in jetstreamext.get_batch(js, "EVENTS", batch=100, seq=1):
    print(msg.sequence, msg.data)

# Non-atomic high-throughput publishing. Each add is stored immediately.
batch = jetstreamext.fast_publish(js, flow=100, max_outstanding_acks=2)
await batch.add("events.a", b"one")
await batch.add("events.b", b"two")

# Commit stores this final message and returns the batch publish ack.
ack = await batch.commit("events.c", b"three")
print(ack.stream, ack.batch_id, ack.batch_size)

# To end without storing a final message, use `await batch.close()` instead.
# To abandon an unfinished publisher while keeping already stored messages,
# use `await batch.abort()` or own it with `async with`.
```

## API

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

### `fast_publish(js, *, flow=100, max_outstanding_acks=2, ack_timeout=5.0, gap_mode=GapMode.FAIL, on_error=None)`

Create a `FastPublisher` for one non-atomic batch. The publisher lazily opens a
persistent wildcard subscription on its first operation. The server can lower
the effective flow interval; up to `max_outstanding_acks` intervals may be in
flight before `add` waits. Protocol pings recover lost flow acknowledgements
and provide progress/liveness while a commit is pending. The server does not
replay a lost terminal acknowledgement in response to a ping, so the commit
still fails—either with an unknown-batch response after server cleanup or when
`ack_timeout` expires without that final response.

`max_outstanding_acks` must be from 1 through 3. `GapMode.FAIL` abandons the
batch on a missing message; `GapMode.OK` reports the gap through `on_error` and
continues. A publisher owns mutable protocol state and must be used from one
asyncio task at a time. Create separate publishers for concurrent batches.

### `FastPublisher`

- `await add(subject, data, *, headers=None)` and `await add_message(message)`
  store a message immediately and return `FastPubAck(batch_sequence,
  ack_sequence)`.
- `await commit(subject, data, *, headers=None)` and `await
  commit_message(message)` store a final message and return the native
  `nats.jetstream.PublishAck` with `batch_id` and `batch_size` populated.
- `await close()` sends an unstored end-of-batch marker and returns the same
  final ack shape. Closing an empty publisher is an error.
- `await abort()` (also available as `await aclose()`) releases an unfinished
  publisher's inbox without sending a commit marker. Already stored messages
  remain stored. `async with fast_publish(js) as batch:` aborts on exit unless
  the batch was already committed or closed. Garbage-collection cleanup is
  best-effort; use one of these explicit forms for reliable cleanup.
- `size`, `is_closed`, `batch_id`, `inbox`, `gap_mode`, and
  `last_ack_sequence` expose publisher state. `size` counts messages
  successfully handed to the core client; an asynchronously server-rejected
  message may therefore still be included, while a synchronous publish failure
  or cancellation is not.

## Errors

- `SubjectRequiredError` — no subjects passed to `get_last_msgs_for`.
- `InvalidOptionError` — an invalid or conflicting option.
- `NoMessagesError` — no messages matched the request.
- `BatchUnsupportedError` — the server predates batch direct get (2.11+).
- `InvalidResponseError` — a server response that could not be parsed.

All inherit from `JetStreamExtError`.

Fast-ingest failures inherit from `FastPublishError` (and therefore
`JetStreamExtError`). Typed variants distinguish invalid configuration, closed
or empty batches, timeout, subscribe/publish/response failures, gaps, generic
per-message flow failures, and the server's not-enabled, invalid-pattern,
invalid-batch-id, unknown-batch-id, and too-many-inflight errors. API errors
retain `code`, `error_code`, `description`, and the batch sequence when the
server provides it. In fail-on-gap mode, gap and flow errors also retain the
following terminal acknowledgement as `publish_ack` when it arrives before the
ack deadline, exposing the server's terminal batch count. Successful terminal
acknowledgements are checked against the publisher's batch ID and message count;
malformed or mismatched terminal responses fail the batch and release its inbox.
