# Orbit

A set of independent utilities around the NATS ecosystem, each published as its
own distribution so you depend only on what you use.

## Packages

All packages require Python 3.13 or newer. The current releases are:

| Package | Import | What it provides | Resources |
| --- | --- | --- | --- |
| [`nats-extra` 0.1.0](https://pypi.org/project/nats-extra/0.1.0/) | `nats.extra` | Streaming request-many and scatter-gather for Core NATS | [Guide](nats-extra/README.md) · [Changelog](nats-extra/CHANGELOG.md) |
| [`nats-jetstream-extra` 0.1.0](https://pypi.org/project/nats-jetstream-extra/0.1.0/) | `nats.jetstream_extra` | Atomic and fast-ingest publishing, plus batch direct-get extensions for JetStream | [Guide](nats-jetstream-extra/README.md) · [Changelog](nats-jetstream-extra/CHANGELOG.md) |
| [`nats-counters` 0.1.0](https://pypi.org/project/nats-counters/0.1.0/) | `nats.counters` | Distributed counters built on JetStream counter streams | [Guide](nats-counters/README.md) · [Changelog](nats-counters/CHANGELOG.md) |

## Installation

Choose only the distributions your application needs:

```sh
# uv
uv add nats-extra
uv add nats-jetstream-extra
uv add nats-counters

# pip
python -m pip install nats-extra
python -m pip install nats-jetstream-extra
python -m pip install nats-counters
```

Each package guide linked above includes usage examples and feature-specific
server requirements.

## Layout

A [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): each
utility is an independently installable member under the shared `nats`
namespace.

## Development

```sh
uv sync                # set up the workspace
uv run pytest          # tests
uv run ruff format     # format
uv run ruff check .    # lint
uv run ty check        # type-check
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
