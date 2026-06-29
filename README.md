# Orbit

A set of independent utilities around the NATS ecosystem, each published as its
own `orbit-*` distribution so you depend only on what you use.

## Layout

A [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/): each
utility is a member that imports under the shared `orbit` namespace —
`orbit-<name>` → `import orbit.<name>`.

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
