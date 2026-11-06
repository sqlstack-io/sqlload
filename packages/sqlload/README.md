# sqlload

SQL data loading tooling: the `sqlload` command line and the Python API for custom sources.

> **Pre-release.** Not yet intended for general use; anything can change between releases.

```sh
uv tool install sqlload
```

The package is a native extension (CPython 3.10+, one abi3 wheel per platform for Linux x86_64 and
aarch64 and macOS arm64). Its `sqlload` command runs in-process, so Python sources defined with
`sqlload.source` and `sqlload.table` run in the same process as the loader. The standalone
executable without Python is the `sqlload-cli` package.

## Table data types

A table function's dicts are typed like JSON: numbers, booleans and text keep their types, and
nested dicts and lists become JSON. `Decimal` becomes a decimal with room for at least 19 digits
before the point (more digits than 38 in all become text, NaN and infinity NULL), `date` becomes
`date`, a naive `datetime` becomes `timestamp_ntz` and one with a UTC offset `timestamp_tz` (a
column mixing both becomes text); subclasses such as `pandas.Timestamp` and other values become
text. Enums give their values,
dataclasses become JSON objects, and numpy numbers count as numbers. Arrow input (pyarrow, polars,
pandas) keeps its own types.

### Flattening objects

Nested values stay one JSON column (`VARIANT` on Snowflake) unless the table lists top-level
objects in `flatten`:

```python
@shop.table(description="Orders", flatten=["customer"])
def orders(ctx):
    yield {"order_id": 7, "customer": {"country": "GB", "address": {"city": "Leeds"}}, "line_totals": {"sku-14": 12.5}}
```

Each listed object is expanded exactly one level, into a column per key named `<object>__<key>`
(`customer__country`, with `customer__address` staying JSON). The key columns are typed like
top-level values: a `Decimal`, `date` or `datetime` inside a listed dict, or a typed child of an
Arrow struct, keeps its type. Objects not listed, such as `line_totals` above, stay JSON; maps whose
keys are data should never be flattened. Lists are never expanded. A missing or null object leaves
its columns NULL, and new keys are new columns under `on_schema_change`. Dicts, `sl.json` bytes and
Arrow structs flatten the same way.

The load fails when a listed value arrives as a scalar or a list, or when a key's column name is
also a field of the rows or comes from flattening another object. `plan` refuses `flatten` on other
sources than Python tables, a dotted path (`customer.address`), and an object that
`exclude_columns`, `mask` or `columns` also names: after flattening no column carries the object's
own name, so name the `<object>__<key>` columns instead. Turning `flatten` on for a table that
already has the object as a JSON column leaves that column NULL from then on, next to the new key
columns.

## HTTP APIs

`sl.http` builds HTTP jobs that run entirely in Rust: requests, pages, fan-out, retries and parsing.
A table function returns or yields the job; Python's part ends once it is built.

```python
shop = sl.source("shop", connection="shop")
api = sl.http(shop, auth=sl.bearer(), rate_limit="10/s", retries=5)


@shop.table(description="Orders, one row per order.", unique_key="order_id")
def orders(ctx):
    return api.paginate("/orders", params={"updated_after": ctx.cursor_value},
                        paginator=sl.next_link("$.next"), select="$.items")


@shop.table(description="Order lines.", unique_key=["order_id", "line_no"])
def order_items(ctx):
    order_ids = api.paginate("/orders", paginator=sl.next_link("$.next"), select="$.items[*].order_id")
    return api.fan_out("/orders/{order_id}/items", over=order_ids, concurrency=8,
                       select="$.items", on_status={404: "skip"})
```

- The connection is `kind = "http"` with `base_url`, and `token` (for `sl.bearer()` and
  `sl.api_key(header=...)` or `sl.api_key(query=...)`) or `username` and `password` (for
  `sl.basic()`), plus optional `headers`. Credentials go only to the connection's own scheme,
  host and port, never to a next-page link elsewhere.
- Every `paginate` names its paginator: `sl.single()`, `sl.next_link(pointer)`,
  `sl.link_header()`, `sl.offset(param, limit=...)`, `sl.page_number(param, start=1)` or
  `sl.cursor(pointer, param)`. Nothing is guessed.
- `select` is `$`, a dotted path such as `$.data.items`, or, for fan-out keys, `$.items[*].id`.
- `fan_out` requests its path once per key, up to `concurrency` at a time, while the keys still
  arrive: keys from a paginate fill the placeholder named after their field; listed values fill
  the path's one placeholder, and dicts fill placeholders by name.
- `rate_limit` (`"10/s"`, `"600/m"`, `"1000/h"`) is shared by every job on the connection with
  the same limit. Transport errors, 429 and 5xx are retried with backoff and jitter, honouring
  `Retry-After`; another status fails unless `on_status` skips it.
- The definition is checked before any request. Errors name the request, the status and the start
  of the body, with the connection's secrets replaced by `***`.
- A job's rows never come back into Python: iterating a job hands the job itself to Rust, and
  reading a row from it fails (rule `python_row_loop`). Filter rows downstream.

## Upgrading from 0.4

Both this package and `sqlload-cli` install a `sqlload` command. Remove the old one first:

```sh
pip uninstall sqlload-cli
```

## Selecting tables

`plan`, `apply` and `load` take selectors positionally or with `--select`/`-s`, and
`--exclude` to leave tables out: each is a source name, a destination table name or a source's
`schema.table`. `--start-cursor`/`--end-cursor` backfill a cursor range, typed from each selected
table's cursor (an integer, an RFC 3339 timestamp or a date).

## Copying one table

`sqlload copy SOURCE DESTINATION` copies one table once, through the same plan and load as a
project, and needs no `sqlload.toml`. Each end is a connection named in `sqlload.toml`
(`app:public.accounts`, `warehouse:RAW.ACCOUNTS`) or a URI: `postgresql://user:${ENV:PGPASSWORD}@host:5432/db#schema.table`,
`duckdb://file.duckdb#SCHEMA.TABLE`, `s3://bucket/prefix/?region=eu-west-2`, `file:///folder` or a
folder. Snowflake needs auth and a `load_method`, so it is always a named connection.

```sh
sqlload copy app:public.accounts duckdb://wh.duckdb#RAW.ACCOUNTS --write-strategy merge --unique-key id
sqlload copy s3://example-bucket/exports/orders/ warehouse:RAW.ORDERS --format ndjson
```

`--write-strategy`, `--unique-key`, `--cursor-column` and `--format` (a files source's format)
set the table; `--print-toml` prints the copy as a project to paste into `sqlload.toml` instead of
running it. A password written into a URI never reaches the printed project, plans or errors:
the project refers to it as `${ENV:SQLLOAD_COPY_SOURCE_PASSWORD}` (or `..._DESTINATION_...`) and
`copy` warns that `${ENV:NAME}` in the URI keeps it out of shell history.

## Checking declarations

`sqlload contract diff [SELECTOR ...]` compares each selected table's declared `columns` with
its live table, in SQLLoad's portable types, and changes nothing. It reads the warehouse's
catalog and lists sources to find their tables (CSV and JSON files are sampled for their columns,
as `plan` does); no rows are loaded. A type agrees when the warehouse stores it the same way, so
`varchar(50)` matches DuckDB's `VARCHAR`; a wider live type is a difference. It reports a declared column whose live
type differs (`type_mismatch`), a declared column the table lacks (`missing_physical_column`), a
table that does not exist yet (`relation_missing`) and, on `contract = "enforced"` tables, live
columns nothing declares (`missing_declaration`); SQLLoad's own `_sqlload_*` columns never count.
It exits 0 when every table agrees and 1 otherwise, including when a source cannot be listed or
a selector matches nothing. `--from TARGET` reads every table as if its source loaded into that
target, in that target's database and schema (declared types are read for its warehouse), and `--json` prints the report as one document with
`sqb contract diff --json`'s field names. A table on a files target is compared with the schema
`plan` uses for it: the footers of the newest load in its newest partition. `--all-files` also reads
every file's footer under the table's folder, ignoring `max_footer_reads`, and reports each
column whose files differ from that load: another type (`files_type_mismatch`), absent
(`files_missing_column`), or held only by older files (`files_extra_column`), naming the files.
A file it cannot read, or a store error, makes that table `inspection_unavailable`. It changes
nothing; on warehouse tables it adds nothing.

`--source` also compares what each table's source would load, from the catalog or file footers
listed as `plan` lists them, with the live table, for the columns no declaration names: a column the
source has that the table lacks (`source_new_column`), one it would load as another type
(`source_type_mismatch`), and a live column the source no longer has (`source_missing_column`).
A type counts only when the live column does not hold it as it is. These findings carry a
`source_type` field in `--json`. Tables under `contract = "enforced"` add none, since their
declaration decides what loads. A Python table's columns are inferred only when it runs, so it is
`inspection_unavailable` under `--source` and makes the command exit 1: declare its columns, leave
it out with `--exclude`, or compare after a load.

`sqlload contract generate [SELECTOR ...]` takes the same selection and options and proposes the
declarations that adopt each live table: every live column nothing declares, with its live type.
`--write` writes them into the table's `columns` in `sqlload.toml`, creating
`[sources.app.tables."public.orders"]` when needed and keeping the rest of the file (comments,
layout) as it was; the edited file must still parse, or nothing is written. Declared columns are
left alone unless `--overwrite` is also given, which retypes them to their live type and removes
declared columns the table lacks. A database or files table declares only columns its source
has, in the source's spelling; masked columns stay under `mask`. Python tables are never edited:
the report prints the whole `columns={...}` to paste into the table's declaration. A table found
by an `include` pattern with no description is not written, since its new entry would then need
one. The file is replaced atomically with its permissions kept, and only if nobody changed it
while the command ran. Adopted columns become pinned types, so a CSV or JSON source converts
them while reading. With `--from`, a table whose own target is another kind of warehouse is not
adopted (its types could be ones its own warehouse cannot take). The exit code is that of
`diff`, after the write.

## Descriptions

A table someone writes needs a description, which becomes the destination table's comment;
`plan` fails for such a table without one. That covers Python tables (`description="..."` or the
function's docstring), files sources (the source's own `description`, as a files source is one
table), `[sources.app.tables."public.orders"]` entries (`description = "..."` there), and tables
named exactly in `include`. A table found by an `include` pattern needs none: it takes the
source's own table comment (Postgres `COMMENT ON TABLE`) when there is one, and none otherwise.

## Schema changes

A column whose type SQLLoad infers (Python tables, JSON and CSV files) does not change type on its
own: when values outgrow the table's column, the table fails with the fix. With
`auto_widen = true` on the table or source, it widens along bool → int → decimal and
timestamp_ntz → timestamp_tz instead, shown in `plan`, and never to float, text or JSON; a change
that rebuilds the table still needs `--allow-type-change-rebuild`. A type pinned in `columns`
widens the table as before.

## New tables

With `new_tables = "skip"`, a table that appears after the source's first load loads only once
`include` names it exactly (`include = ["public.*", "public.refunds"]`, with a description under
`[sources.app.tables."public.refunds"]`); a `[sources.*.tables]` entry alone only sets its options.
`new_tables = "fail"` stops the plan instead. The first load is the one before any of the
source's tables exists in the destination, whichever tables are selected; tables present then
count as approved. A table that did not load then (blocked by a problem, say) counts as new
afterwards, so name it in `include`.

## Postgres change data capture

`kind = "postgres_cdc"` streams a Postgres database's changes from a logical replication slot
into DuckDB, Snowflake or Databricks (not files targets yet). The server needs
`wal_level = logical`, and the connection's user needs the `REPLICATION` attribute.

```toml
[sources.app_cdc]
kind = "postgres_cdc"
connection = "app_pg"
include = ["public.*"]
cdc_mode = "merge"        # merge (default) | changelog
deletes = "soft"          # soft (default) | hard; merge only
# slot = "sqlload_app_cdc"         # the default: sqlload_<source>
# publication = "sqlload_app_cdc"  # created for the selected tables when missing
```

- The first `sqlload load` creates the slot with an exported snapshot and copies every table
  under it. Each later load reads the changes committed since the last one, up to the moment it
  started, then stops.
- Every row carries `_sqlload_lsn` (its transaction's commit LSN), `_sqlload_op` (`snapshot`,
  `insert`, `update`, `delete` or `truncate`) and `_sqlload_committed_at`.
  - `merge` keeps one row per key. A soft delete sets `_sqlload_deleted_at`, and a truncate
    soft-deletes every older row. A TOASTed value the change left out keeps its current value.
  - `changelog` appends every change, with `_sqlload_seq` as its position in the transaction
    and `_sqlload_unchanged` flagging the values the change left out.
- A batch always holds whole transactions. The slot confirms a batch only after every table it
  touched has committed, and a change whose `_sqlload_lsn` the table already holds is skipped.
  A load that stops part-way is safe to run again: nothing is lost or applied twice.
- Tables are not promised to be consistent with each other at every moment. Each one converges
  to the source as of the last confirmed position.
- A captured table takes no `write_strategy`, `cursor_column`, `batch_size` or `state`. `merge`
  needs a primary key or a `unique_key`.
- When the slot is lost, or a snapshot stopped part-way, the load stops and names the fix:
  `sqlload load --resnapshot` drops and recreates the slot and copies every table again. `plan
  --resnapshot` shows it first.
- Schema changes while streaming are not handled yet; a table that changes shape stops the load
  with the same fix.

## Python sources

List the folders holding them in `sqlload.toml` (`python_paths = ["sources"]`); every
`sqlload.source` and `@sqlload.table` found there joins `plan`, `apply` and `load`:

```python
import sqlload as sl

shop = sl.source("shop", connection="shop", write_strategy="merge")


@shop.table(unique_key="order_id", cursor_column="updated_at", lookback="1h")
def orders(ctx: sl.Context):
    """Orders from the shop API, one row per order."""
    yield from fetch_orders(ctx.connection["base_url"], updated_after=ctx.cursor_value)
```

This loads into the table `shop__orders`: like `[sources]` entries, tables are named
`{source}__{table}` unless the source or the table sets `table_name` (with the same `{source}`,
`{schema}` and `{table}` placeholders). A `@sqlload.table` declared without a source keeps its
own name.

Columns are inferred from what a table yields (declare types with `columns={...}`, which take
precedence). A table is created by its first load, never by `apply`; a first load that yields
no rows creates nothing unless columns are declared. Under `replace` (and `--reload`), a load
that yields no rows into a table that has rows fails rather than emptying it; set
`allow_empty=True` on the table or source when an empty result is real. Rows outside the cursor window are skipped,
so a function that ignores `ctx.cursor_value` still loads correctly.

`ctx.connection` holds the source's connection settings; `ctx.connection("name")` gives another
connection's, except connections that a `[targets]` entry writes to, which table functions never
get.

### Batches

A large load commits along the way instead of all at the end, so a failure keeps what committed
and the next run carries on from there. Tables load at the same time, up to `concurrency` in
`sqlload.toml` or `--concurrency N` (default 4); one table's batches commit one at a time.

How a table commits, as `plan` shows on its `batch` line:

- `batch_size = "1d"` (a duration, for date and timestamp cursors) or `batch_size = 1000` (a step, for integer
  cursors) reads and commits one cursor window at a time, up to 100,000 windows. A Python function
  is called once per window, with `ctx.cursor_value` and `ctx.cursor_end` set to it. A Postgres
  integer cursor without `--end-cursor` ends at its maximum when `plan` ran.
- Without `batch_size`, a Postgres table whose cursor leads an index is read in cursor order (NULLs
  first) and commits about every 1 GB, only between distinct cursor values. `batch_size = "none"`
  opts out, for tables where the ordered read is slower than a scan.
- Any other cursor table commits once per read, as do text cursors and `append` without a cursor
  or logged state, since a rerun could otherwise skip or duplicate rows.
- Files sources commit at 1,000 files or about 1 GB; under `delete_insert` a partition is never split.
- `replace` and `--reload` swap the whole table in once, at the end. `load --reload` shows the
  plan and asks first; off a terminal it needs `--yes`.

Windows on a Postgres table are read in one `REPEATABLE READ` snapshot, so a row moving between
windows is read once. That snapshot stays open on the source for the whole table load: it counts
towards `idle_in_transaction_session_timeout`, and vacuum cannot remove rows it can still see. Use
a larger `batch_size` or a narrower range if either matters.

Tables of a Python source with a `@source.client` run one at a time, because clients are rarely
thread-safe; set `parallel_tables=True` on the source when its client is.

### How rows reach Snowflake

A Snowflake connection needs `load_method`, with no default: it is how data gets into Snowflake.
A method the role cannot use fails at `plan`, and never falls back to another:

```toml
[connections.wh]
kind = "snowflake"
# ...
load_method = { type = "external_stage", url = "s3://lake/sqlload/", storage_integration = "LAKE_INT" }
# or an existing stage: name = "DB.SCHEMA.STAGE" instead of storage_integration
# or { type = "internal_user_stage" }, { type = "internal_table_stage" },
#    { type = "internal_named_stage", name = "DB.SCHEMA.STAGE" }, { type = "streaming" },
#    { type = "insert" } (very slow, warned in plan)
```

An `external_stage` needs SQLLoad's own write access to the bucket. `apply` creates `_SQLLOAD_STAGE` in the
loader schema when given a `storage_integration`. Staged files are deleted once their batch commits,
and the next load removes what failed runs left after a day; `keep_staged_files = true` on the
stage keeps them and leaves that stage out of the cleanup.

Files of a files source (Parquet, JSON, NDJSON or CSV) under that URL load directly: Snowflake
reads them itself and nothing passes through SQLLoad, with the same values SQLLoad's own reader
gives, which live tests check. A column the direct `COPY` cannot load exactly (`Float16`, a decimal
over 38 digits, a nested type) fails `plan`, or the load for a later batch, naming the column: files
never fall back to SQLLoad. Files no external stage covers fail `plan` too, unless the source sets
`copy_to_internal_stage = true`.

A `hash`-masked column on this route reaches Snowflake in plain text, in a temporary table gone with
its session, and is hashed there. `hmac` is the one exception: its key must never appear in SQL, so
those files go through SQLLoad, which `plan` warns is much slower.

Files sources add `_sqlload_source_row`, a row's number in its file from 1.

### How rows reach Databricks

A Databricks target loads into a Unity Catalog SQL warehouse through a volume it stages files in.
It signs in with a service principal's OAuth machine-to-machine credentials:

```toml
[connections.lakehouse]
kind = "databricks"
host = "${ENV:DATABRICKS_HOST}"
client_id = "${ENV:DATABRICKS_CLIENT_ID}"
client_secret = "${ENV:DATABRICKS_CLIENT_SECRET}"
warehouse_id = "${ENV:DATABRICKS_WAREHOUSE_ID}"
catalog = "main"
volume = "main.raw.sqlload_stage"   # catalog.schema.volume
```

The service principal needs `USE CATALOG`, `USE SCHEMA` and `CREATE TABLE` where it loads
(`CREATE SCHEMA` too when the schema does not exist yet), ownership of or `MANAGE` on the tables it
replaces or rewrites, and `READ VOLUME` and `WRITE VOLUME` on the volume. `debug` checks that the
volume lists.

Each batch is written as Parquet to `<volume>/sqlload/<load id>/<batch>/` and loaded with one
`COPY INTO`. The batch's folder is removed once the `COPY` commits, and when it fails; the next load
removes what failed runs left after a day, deleting only files it listed as old and folders that
are empty, so a load writing meanwhile keeps its files. This is the only route: there is no `INSERT`
fallback. Files of a files source pass through SQLLoad on the way to the volume.

The Statement Execution API runs every statement in its own transaction, so each write is one
statement: `append` is the `COPY` into the table (into a staging table, then one `INSERT`, for a
logged table), `delete_insert` is one `MERGE` that inserts the
batch and deletes the rows it replaces, and `merge` is one `MERGE` on the key. The run record
commits after it.

`replace` loads into a build table, which then replaces the table with
`CREATE OR REPLACE TABLE ... AS SELECT`. That keeps the table's history and grants, but Delta has no
metadata-only swap, so the rows are copied once more. That copy is the expected cost of `replace`
on Databricks.

Types without an exact Databricks equivalent are lossy, and `plan` says so: times and UUIDs are
stored as text, and `TIMESTAMP` keeps the instant but not the offset. JSON loads as `VARIANT`.
Added columns use one `ADD COLUMNS`. Widening a type uses Delta's type widening where it applies
and rewrites the table where it does not, such as an integer becoming a float.

### How files reach DuckDB

Parquet files in S3 or a local folder load into DuckDB directly: DuckDB reads them with
`read_parquet`, with the same values SQLLoad's own reader gives. `plan` names the route per table.
Tables DuckDB cannot load exactly go through SQLLoad's reader instead: other formats, a custom S3
endpoint (in the connection or `AWS_ENDPOINT`/`AWS_ENDPOINT_URL`), masks, a file column named
`filename` or `file_row_number`, nanosecond or epoch timestamps and other types DuckDB would not
read exactly, a column loaded as a type other than the files hold, or glob characters in a path. A
later batch whose files hold a column differently fails the load, naming the column.

The first S3 load installs DuckDB's `httpfs` extension into `~/.duckdb`, and `aws` too when
credentials come from the AWS credential chain rather than `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. That needs the network once. Local folders need nothing extra.

### Logged state

A table whose position the data cannot carry (an API sync token, say) sets `state="logged"` and
keeps it in `ctx.state`:

```python
@shop.table(write_strategy="append", state="logged")
def events(ctx: sl.Context):
    """Shop events, read page by page with a sync token."""
    page = fetch_events(since=ctx.state)
    yield from page.items
    ctx.state = page.next_token
```

A large run commits along the way, each commit with the value `ctx.state` had one assignment
before its current one, so whether a function sets the state before or after the rows it covers,
a run that fails part-way never loses rows; under `append`, the next run may load up to about
two pages again.
Assign `ctx.state` once per page; assigning more than once per page can lose rows after a crash.

For an exact position, yield `sl.checkpoint(...)` after the rows it covers instead:

```python
@shop.table(write_strategy="append", state="logged")
def events(ctx: sl.Context):
    """Shop events, read page by page with a sync token."""
    token = ctx.state
    while True:
        page = fetch_events(since=token)
        yield from page.items
        token = page.next_token or token
        yield sl.checkpoint(token)
        if not page.has_more:
            break
```

A checkpoint covers exactly the rows yielded before it: commits record the latest checkpoint
whose rows they hold, so a run that fails part-way resumes from it without loading any row twice,
and the next run reads it as `ctx.state`. Rows yielded after the last checkpoint are loaded again by
the next run unless the function leaves the final position in `ctx.state`. Once a function yields a checkpoint, only checkpoints move the logged position during
the run; a value it leaves in `ctx.state` when it finishes still becomes the final one.
`sl.checkpoint(None)` clears the position, so the next run starts from the beginning; an API whose
last page has no next token should keep the last real one. `sl.checkpoint` needs
`state="logged"` and a JSON value.

Each successful run appends a record to `_sqlload_runs` in the table's schema (or one object under
`<table folder>/_sqlload/runs/` in a files target), in the same transaction as the data where the
warehouse allows it, so a failed load never moves the position. A table with a `cursor_column`
also logs the data's cursor; when the log and the data disagree, the data wins and the load appends
a visible reconciliation. `sqlload state show`, `verify`, `reconcile --yes` and
`reset --to VALUE --yes` (or `--clear`) inspect and correct the log; `--reload` starts over.
`reset` needs the tables or sources to reset named. On a table with a `cursor_column`,
`reset --to` sets the cursor and keeps `ctx.state`; on one without, it sets `ctx.state` (JSON).
`--clear` wipes both. A record SQLLoad cannot read (corrupt, or written by a newer version)
blocks only its table; `state show` lists it and a `reset` starts a new position after it.
Deleting the log is safe: the next run reads as a stateless one would.

