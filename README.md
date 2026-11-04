# sqlload

SQL data loading tooling.

> **Pre-release.** sqlload is under active development and is not yet intended for general use.
> Commands, configuration and behaviour can change without notice between releases, and there is
> no support or stability guarantee.

## Install

```sh
uv tool install sqlload
```

The `sqlload` package is a native Python extension (CPython 3.10+) for Linux (x86_64, aarch64)
and macOS (arm64): its `sqlload` command runs the loader in-process, so Python sources can run.
`sqlload-cli` holds the same command as a standalone executable, without Python.

## Development

```sh
make check            # fmt, clippy, fensu
make test-unit
make test-containers  # needs Docker (Redpanda, Postgres, SeaweedFS)
make test-python      # builds the sqlload extension into .venv and runs its tests
```

## License

Apache-2.0
