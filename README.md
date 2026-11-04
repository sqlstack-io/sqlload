# sqlload

SQL data loading tooling.

> **Pre-release.** sqlload is under active development and is not yet intended for general use.
> Commands, configuration and behaviour can change without notice between releases, and there is
> no support or stability guarantee.

## Install

```sh
uv tool install sqlload
```

The `sqlload` package depends on `sqlload-cli`, which holds the native executable for Linux
(x86_64, aarch64) and macOS (arm64).

## Development

```sh
make check            # fmt, clippy, fensu
make test-unit
make test-containers  # needs Docker (Redpanda, Postgres, SeaweedFS)
```

## License

Apache-2.0
