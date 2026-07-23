# Orbit

A set of independent utilities around the NATS ecosystem, each published as its
own distribution so you depend only on what you use.

Current utilities:

- `nats-extra` — streaming request-many and scatter-gather for Core NATS.
- `nats-jetstream-extra` — atomic publishing and batch direct-get extensions for JetStream.
- `orbit-counters` — distributed JetStream counters.

## Layout

A [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): each
utility is an independently installable member. Orbit utilities import under
the shared `orbit` namespace
(`orbit-<name>` → `import orbit.<name>`), while extensions to the NATS Python
API import under `nats` (`nats-extra` → `import nats.extra`;
`nats-jetstream-extra` → `import nats.jetstream_extra`).

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
