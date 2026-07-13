# orbit-jetstreamext

JetStream extensions for NATS, built on the batch direct get API.

Batch direct get fetches several stored messages with a single request instead
of one round-trip per message. The server streams the matching messages back on
a reply inbox and ends the stream with an end-of-batch sentinel.

Requires a stream configured with `allow_direct` and nats-server 2.11+.

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

## Errors

- `SubjectRequiredError` — no subjects passed to `get_last_msgs_for`.
- `InvalidOptionError` — an invalid or conflicting option.
- `NoMessagesError` — no messages matched the request.
- `BatchUnsupportedError` — the server predates batch direct get (2.11+).
- `InvalidResponseError` — a server response that could not be parsed.

All inherit from `JetStreamExtError`.
