# Spike: Snowpipe Streaming REST API (Named Channels) from Rust

A small binary that talks to the Snowpipe Streaming high-performance REST API with
`reqwest` only (no Snowflake SDK, no driver crate). It shows:

1. authentication with a programmatic access token (PAT), used directly or exchanged for a
   scoped token;
2. ingest host discovery;
3. opening a Named Channel on a pipe;
4. appending gzip'd NDJSON with `offsetToken` set to a fake source (Kafka-style) offset;
5. polling channel status until committed, and resuming from `last_committed_offset_token`
   after a restart without duplicates;
6. one pooled HTTPS client for everything, with a count of the new connections it opened.

## Setup SQL

```sql
CREATE TABLE MY_DB.MY_SCHEMA.STREAM_ORDERS (
    kafka_offset NUMBER NOT NULL,
    order_id     NUMBER,
    customer     STRING,
    amount       NUMBER(10, 2),
    channel      STRING,
    sent_at_ms   NUMBER,
    payload      VARIANT
) ERROR_LOGGING = TRUE;

-- Optional: an explicit pipe. Without it, the default pipe "STREAM_ORDERS-STREAMING" is
-- created on demand by the first open-channel call and behaves the same for this spike.
CREATE PIPE MY_DB.MY_SCHEMA.STREAM_ORDERS_PIPE AS
    COPY INTO STREAM_ORDERS
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

The role needs USAGE on the database and schema, INSERT on the table and, for an explicit
pipe, OPERATE on the pipe (ownership covers all of these).

## Running

Configuration comes from the environment only:

| Variable | Meaning |
| --- | --- |
| `SQLLOAD_SNOWFLAKE_ACCOUNT` | Account identifier, `<org>-<account>` |
| `SQLLOAD_SNOWFLAKE_PAT` | Programmatic access token |
| `SQLLOAD_SNOWFLAKE_ROLE` | Role for SQL API verification queries (optional) |
| `SQLLOAD_SNOWFLAKE_WAREHOUSE` | Warehouse for SQL API verification queries |
| `SQLLOAD_SNOWFLAKE_DATABASE`, `SQLLOAD_SNOWFLAKE_SCHEMA`, `SQLLOAD_SNOWFLAKE_TABLE` | Target |
| `SQLLOAD_SNOWFLAKE_PIPE` | Pipe name (default `<TABLE>-STREAMING`) |
| `SQLLOAD_CHANNEL` | Channel name (default `spike_channel_0`) |
| `SQLLOAD_AUTH` | `pat` (default) or `scoped` |

```sh
spike-streaming run --appends 20 --batch 50   # open, resume, append, wait for commit, verify
spike-streaming run --no-wait                 # append and exit before commit (simulated crash)
spike-streaming large-row                     # ~9 MiB single rows vs the 4 MB request limit
spike-streaming bad-row                       # rows that fail server-side processing
```

Verification queries go through the SQL API (`POST /api/v2/statements`) on the same
client, so the binary has no driver dependency.

## Endpoints and request shapes

All requests use `Authorization: Bearer <token>`. With a PAT, also send
`X-Snowflake-Authorization-Token-Type: PROGRAMMATIC_ACCESS_TOKEN`.

| Step | Request |
| --- | --- |
| Discover ingest host | `GET https://<account>.snowflakecomputing.com/v2/streaming/hostname` → plain-text host (replace `_` with `-`) |
| Scoped token (optional) | `POST https://<account>.snowflakecomputing.com/oauth/token`, form: `grant_type=urn:ietf:params:oauth:grant-type:token-exchange`, `scope=<ingest host>`, `subject_token=<PAT>`, `subject_token_type=programmatic_access_token` (no `Authorization` header) |
| Open channel | `PUT https://<ingest>/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}?requestId=<uuid>`, JSON `{"fail_on_uncommitted_rows": true}` → `next_continuation_token`, `channel_status.last_committed_offset_token` |
| Append | `POST https://<ingest>/v2/streaming/data/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}/rows?continuationToken=..&offsetToken=..&requestId=..`, `Content-Type: application/x-ndjson`, `Content-Encoding: gzip` → `next_continuation_token` |
| Status | `POST https://<ingest>/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}:bulk-channel-status`, JSON `{"channel_names": ["<channel>"]}` → `channel_statuses.<CHANNEL>` (key comes back upper-cased) |

The pipe name is used verbatim in the path, including the `-` of a default pipe name.

## Findings

Measured from a Linux host against a test schema; small volumes only.

### Authentication

- The PAT works directly against the ingest host for open, append and status (with the
  `PROGRAMMATIC_ACCESS_TOKEN` token-type header), and against the SQL API.
- The token-exchange grant above returns a scoped token (JSON body) that also works for
  open, append and status, sent without a token-type header. The documented `jwt-bearer`
  grant does not accept a PAT.
- Using the PAT directly saves one round trip and avoids scoped-token expiry handling.

### Connections

- reqwest dials only when its pool has no usable connection, so a counting connector layer
  (`src/conn_count.rs`) gives the exact number of new TCP+TLS connections.
- Every run opened **2 connections** (account host + ingest host), for 1, 20 and 500
  appends alike. Both hosts negotiate HTTP/2, so all requests multiplex over one
  connection per host. A rejected 413 request did not cost a new connection either.
- `/proc/net/snmp` `ActiveOpens` is system-wide and too noisy on a shared host to be useful.

### Latency

- Append requests take about 6–18 ms each (20 appends of 50 rows in 125–356 ms; 500
  appends of 4 rows in about 4 s). No HTTP 429 was seen at roughly 125 requests per second
  on one channel, despite the documented 10 requests per second default.
- Time from the last append response to `last_committed_offset_token` reaching the final
  offset: about 19 s on the first ever use of a new pipe, then 1.5–7 s.
  `snowflake_avg_processing_latency_ms` reported 4.7–6.7 s.
- Rows were queryable through SQL 0.2–0.55 s after the status poll first reported the
  commit, so a commit in the status endpoint is a good proxy for visibility.

### Restart and exactly-once

- Run, then run again: the second open returns the previous final offset
  (for example `1000`) and the binary resumes at the next offset. After repeated runs the
  table held exactly one row per offset (`COUNT(*) = COUNT(DISTINCT kafka_offset) = MAX`).
- Simulated crash (`--no-wait`, 1000 rows appended, exit immediately) followed by an
  immediate reopen with `fail_on_uncommitted_rows: true`: the open did **not** return 409.
  It succeeded with the last committed offset from before the crash, and the 1000
  in-flight rows were discarded (never appeared). Re-sending them from the committed
  offset gave no duplicates and no gaps. Correctness relies on resuming from
  `last_committed_offset_token`, not on the uncommitted-rows check.

### Large rows (4 MB per request limit)

- The limit applies to the compressed bytes on the wire, so row size alone does not decide.
- A 9.0 MiB realistic JSON row (repeated order-line objects) gzips to 675 KiB (16.5 % of the
  limit). It was accepted in 193 ms, committed about 5 s later and stored as a 9.4 MB
  VARIANT.
- A 9.0 MiB row of random hex text gzips to 5.1 MiB and is rejected with
  `413 REQUEST_BODY_TOO_LARGE` ("The request body cannot exceed 4MB (4210688 received
  bytes / 4194304 allowed bytes)"). The server stops reading at the limit. The channel
  stays usable.
- A loader therefore needs to compress first, then split batches by compressed size, and
  route single rows that do not fit compressed to another path (for example a stage load).

### Bad rows (ON_ERROR = CONTINUE)

Each test batch was good row, bad row, good row:

| Bad row | HTTP | Channel status | Error table `error_data_content_type` |
| --- | --- | --- | --- |
| `"amount": "not-a-number"` into NUMBER | 200 | `SUCCESS`, `rows_error_count` +1, message "Failed to cast variant value \<redacted\> to \<redacted\>" | `json` |
| missing NOT NULL `kafka_offset` | 200 | `SUCCESS`, +1, "NULL result in a non-nullable column" | `json` |
| malformed JSON line | 200 | `SUCCESS`, +1, "Error parsing JSON: \<redacted\>" | `json-invalid` |

- The good rows around each bad row loaded, and the offset still committed.
- `last_error_offset_upper_bound` points at the batch's offset token.
- With `ERROR_LOGGING = TRUE`, `ERROR_TABLE(<table>)` held the full original line, the
  unredacted message and `offset_token_upper_bound`, as soon as the batch committed.
- Bad rows never fail the request, so a loader has to read `rows_error_count` from status
  or the error table to detect them.
