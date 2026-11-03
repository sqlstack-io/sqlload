# sqlload

Export Kafka topics to Snowflake tables. One process watches a set of topics, creates the tables it needs, and keeps each table up to date with the records in its topics, using as few connections and as little warehouse time as the chosen delivery mode allows.

sqlload is a pre-release. The configuration format may still change.

## Install

```sh
uv tool install sqlload    # or: pip install sqlload, or: uvx sqlload ...
```

The `sqlload` package depends on `sqlload-cli`, a wheel holding the native executable (Linux x86_64 and aarch64, macOS arm64); no Python runs while exporting. Kafka TLS uses a built-in OpenSSL, and SASL PLAIN and SCRAM work without system libraries; GSSAPI/Kerberos is not included.

## How it works

- **Discovery.** Topics matching `source.include` and not `source.exclude` are listed at start and every `discovery_interval`. New topics start exporting without a restart.
- **Naming.** Each topic maps to a table through `[naming.map]`, then `[[naming.rules]]` in order, then the default rule. Several topics can share a table on purpose through a rule; topics that collide only through the default rule are rejected.
- **Streams.** All topics of one table form a stream with one consumer group. Run more processes with the same configuration to share partitions.
- **Envelope.** Every row has the same columns, set by `[[envelope.columns]]`. The default is `TOPIC, KAFKA_TIMESTAMP, KAFKA_TIMESTAMP_TYPE, KAFKA_OFFSET, KAFKA_PARTITION, KEY, VALUE, EXPORTED_AT, DATE`. A `VARIANT` value column holds parsed JSON; text columns hold the payload as text. sqlload keeps no schema of its own and stores no state.
- **Objects.** `plan` shows what each stream needs and `apply` creates what is missing. sqlload only ever creates; it never alters or drops. An existing table missing a configured column, or with a different type family, stops the plan.

## Delivery modes

| `staging.kind` | How rows arrive | Resume point | Warehouse needed | Connections |
| --- | --- | --- | --- | --- |
| `none` | Snowpipe Streaming REST, one channel per partition | Snowflake's committed offset per channel | No | A few, shared by all streams |
| `internal` | gzip files on a table stage, loaded by Snowpipe (key pair) or `COPY` | Kafka consumer group | Only for `COPY` | A few to Snowflake and S3 |
| `s3` | gzip files in your bucket, loaded by an auto-ingest pipe or `COPY` | Kafka consumer group | Only for `COPY` | A few to S3 |

Streaming is the cheapest and lowest-latency mode, and the only one with exactly-once delivery: rows count as delivered only after Snowflake reports them committed, and after a crash, restart or rebalance sqlload resumes from each channel's committed offset, so nothing is lost or duplicated.

The staged modes are at-least-once. A batch's offsets are committed to Kafka only after its load is accepted, and a retried batch reuses the same file, so ordinary retries do not duplicate rows. A crash between the load and the Kafka commit, or a rebalance in that window, loads the batch's rows again. Rows Snowflake rejects go to the table's error table (`ERROR_LOGGING = TRUE`) and are counted in `sqlload_error_rows_total`.

`internal` mode supports S3-backed accounts with the default client-side encryption only. Snowpipe's `insertFiles` endpoint does not accept access tokens, so with a token `load = "auto"` uses `COPY`, which needs a running warehouse.

## Commands

```
sqlload -c sqlload.toml check    # validate, connect to Kafka and Snowflake
sqlload -c sqlload.toml plan     # topic → table mapping and objects to create
sqlload -c sqlload.toml tables   # topic, table, stream and group per topic; no Snowflake
sqlload -c sqlload.toml apply    # create missing tables, stages and pipes
sqlload -c sqlload.toml run      # export until SIGINT or SIGTERM
sqlload -c sqlload.toml status   # per-partition lag and committed offsets
```

`run` creates missing objects itself before starting a stream; a stream whose existing table does not fit is logged and skipped, and checked again hourly. `--max-duration 10m` stops it after a while; `--shard N` (or `SQLLOAD_SHARD`) runs only the streams shard `N` of `source.shards` owns. `tables --topics-file FILE` maps names from a file instead of Kafka. Logs go to stderr; `--log-format json` writes JSON lines and `RUST_LOG` sets the level.

## Configuration

See [`sqlload.example.toml`](sqlload.example.toml) for every setting. Unknown keys are errors. Secrets come from the environment with `${NAME}`.

Authentication is a programmatic access token (`method = "pat"`) or a key pair (`method = "key_pair"`, an unencrypted PKCS#8 or PKCS#1 PEM file). The role needs `CREATE TABLE`, plus `CREATE STAGE` and `CREATE PIPE` for staged modes, on the schema, and `USAGE` on the warehouse when one is used.

Each stream has its own Kafka consumer. To keep many streams in one process cheap, `run` raises the open-file soft limit to the hard limit, consumers share one `prefetch_budget`, idle fetches wait 2 s, and streams start in waves of `startup_batch`. [`deploy/sqlload.service`](deploy/sqlload.service) is an example systemd unit with suitable limits.

Kafka TLS and SASL need the `ssl` and `sasl` build features, which the release binaries include.

In `s3` mode, AWS credentials come from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` at start, else from the shared credentials file (`AWS_SHARED_CREDENTIALS_FILE`, default `~/.aws/credentials`) under `AWS_PROFILE` or `default`. Temporary credentials that expire stop the exporter after the retry window; restart it with fresh ones, or use long-lived keys.

Topics that share a table through a naming rule must not differ only in case or punctuation, because they would share streaming channel names; `plan` rejects such groups.

A delivery that keeps failing for 4 minutes stops the exporter rather than letting its consumer drop out of the group; errors that cannot succeed on retry, such as a rejected `COPY`, stop it at once.

## Metrics

With `[metrics] listen` set, `/metrics` serves Prometheus text and `/healthz` answers `ok`. Per stream: `sqlload_rows_total`, `sqlload_uncompressed_bytes_total`, `sqlload_sent_bytes_total`, `sqlload_requests_total`, `sqlload_batches_total`, `sqlload_delivery_errors_total`, `sqlload_skipped_rows_total`, `sqlload_error_rows_total`, `sqlload_last_delivery_timestamp_seconds`. Process-wide: `sqlload_connections_opened`, `sqlload_buffered_bytes`.

## Development

```
make check            # fmt, clippy, fensu
make test-unit
make test-containers  # needs Docker: Redpanda containers and a local fake Snowflake
```

Code layout follows [fensu](https://pypi.org/project/fensu-cli/)'s Rust rules: each domain has `main/` entry modules with one public function, `_helpers/`, `models.rs`, `constants.rs`, `errors.rs` and table-driven tests.
