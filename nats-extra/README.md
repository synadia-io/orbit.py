# nats-extra

Core NATS extensions. The first utility is request-many, a streaming
request/reply and scatter-gather pattern matching `nats-extra` in orbit.rs.

## Install

```sh
pip install nats-extra
```

## Usage

```python
import nats.extra
from nats.client import connect

nc = await connect("nats://localhost:4222")

responses = await nats.extra.request_many(
    nc,
    "services.ping",
    b"ping",
    stall_wait=0.25,  # stop after 250 ms without another response
)
async for response in responses:
    print(response.data)

print(responses.termination_reason)
```

Request streams can terminate on `max_messages`, `max_wait`, `stall_wait`, a
sentinel predicate, a no-responders status, or subscription closure. The reason
is available as `responses.termination_reason`.

The orbit.rs-style builder is also available:

```python
responses = await (
    nats.extra.RequestMany(nc)
    .max_messages(10)
    .stall_wait(0.25)
    .sentinel(lambda message: message.data == b"done")
    .send("services.work", b"start")
)
```

Natural termination unsubscribes automatically. When abandoning a response
stream early, call `await responses.aclose()` or use it as an async context
manager.

## License

Apache 2.0
