# orbit-counters

Distributed counters built on NATS JetStream streams.

A counter is a JetStream stream configured with `allow_msg_counter` (ADR-49,
**requires nats-server 2.12+**). Each subject in the stream is an independent,
arbitrary-precision counter, and increments are atomic.

## Install

```sh
uv add orbit-counters
```

## Usage

```python
from nats.client import connect
from nats.jetstream import new as jetstream
from orbit import counters

nc = await connect("nats://localhost:4222")
js = jetstream(nc)

stream = await js.create_stream(
    name="COUNTERS",
    subjects=["events.>"],
    allow_msg_counter=True,   # enable counters
    allow_direct=True,        # required for reads
)
counter = counters.from_stream(js, stream)

# Increment / decrement; returns the new total.
await counter.add("events.orders", 1)    # -> 1
await counter.add("events.orders", 10)   # -> 11
await counter.add("events.orders", -1)   # -> 10

# Read the current value.
await counter.load("events.orders")      # -> 10

# Full entry with the last increment and source history.
entry = await counter.get("events.orders")
entry.value    # 10
entry.incr     # -1
entry.sources  # {source_id: {subject: value}} or None
```

Or wrap an existing stream by name:

```python
counter = await counters.get_counter(js, "COUNTERS")
```

## Status

Single-subject operations (`add`, `load`, `get`) and source tracking are
implemented. `get_multiple` (batch / wildcard queries) is **not yet
implemented** — it needs batch direct get, which arrives with
`nats-jetstream-extra`.

## License

Apache 2.0
