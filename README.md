# Orbit

A set of independent utilities around the NATS ecosystem that aim to boost
productivity and provide a higher abstraction layer for NATS clients — the
Python counterpart to [orbit.go](https://github.com/synadia-io/orbit.go).

Each utility is published as its own distribution, so you only depend on the
pieces you use.

## Layout

This is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/).
The root is a virtual workspace (tooling only); utilities are added as members
under the `orbit-*` convention as they are built. Each member ships under the
shared `orbit` namespace package:

```
distribution   orbit-<utility>      ->   import   orbit.<utility>
```

Names map 1:1 to orbit.go's modules and stay flat (e.g. `orbit-counters`, not
`orbit-jetstream-counters`); a utility's relationship to JetStream is expressed
as a dependency, not a name hierarchy.

## Utilities

Mirroring the feature set of orbit.go. None implemented yet — this is the
planned set.

| Distribution          | Module                | Status     | Description                                                 |
| --------------------- | --------------------- | ---------- | ----------------------------------------------------------- |
| `orbit-natsext`       | `orbit.natsext`       | ⬜ planned | Core NATS extensions                                        |
| `orbit-jetstreamext`  | `orbit.jetstreamext`  | ⬜ planned | JetStream extensions                                        |
| `orbit-natscontext`   | `orbit.natscontext`   | ⬜ planned | Connect to NATS using NATS CLI contexts                     |
| `orbit-natssysclient` | `orbit.natssysclient` | ⬜ planned | Client for the NATS monitoring / system APIs                |
| `orbit-pcgroups`      | `orbit.pcgroups`      | ⬜ planned | Client-side partitioned consumer groups                     |
| `orbit-kvcodec`       | `orbit.kvcodec`       | ⬜ planned | Transparent encoding/decoding for JetStream KeyValue stores |
| `orbit-counters`      | `orbit.counters`      | ⬜ planned | Distributed counters built on JetStream streams             |

## Development

Requires [uv](https://docs.astral.sh/uv/) and Python 3.13+.

```sh
make deps     # uv sync the workspace
make test     # ruff + codespell + ty + pytest
make format   # ruff format
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
