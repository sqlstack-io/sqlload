# sqlload

SQL data loading tooling.

> Commands, configuration, the library API and behaviour can change without notice between
> releases, and there is no support or stability guarantee.

```sh
cargo install sqlload
```

The library is the engine behind the `sqlload` command; build without default features to leave
out the command-line dependencies.

| Feature | Default | What it adds |
|---------|---------|--------------|
| `cli` | yes | The `sqlload` executable and its command-line parsing. |
| `duckdb` | yes | DuckDB as a destination, compiled from its bundled source. |
| `kafka` | yes | Exporter configurations, which read Kafka topics, through a bundled librdkafka. Without it nothing links librdkafka, and an exporter configuration fails to parse, saying so. |
| `ssl`, `ssl-vendored`, `sasl` | no | TLS (with system or built-in OpenSSL) and SASL in librdkafka, when `kafka` is on. |
| `odbc` | no | ODBC sources, through the system driver manager. |
