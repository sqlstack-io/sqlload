# Contributing

## Checks

```sh
make check            # fmt, clippy, fensu
make test-unit
make test-containers  # needs Docker (Redpanda, Postgres, SeaweedFS)
make test-python      # builds the sqlload extension into .venv and runs its tests
```

`make check-ci` is what CI runs, and must be clean, including fensu with 0 faults.

The unit and container tests link the ODBC source against unixODBC's `libodbc.so`, and the
container tests also need the PostgreSQL ODBC driver: `unixodbc-dev odbc-postgresql` on Debian and Ubuntu, `unixodbc psqlodbc`
with Homebrew.

## Files a warehouse reads itself never pass through SQLLoad

When a files source loads into a warehouse that reads files itself (Snowflake), the warehouse
reads the files: in place under an external stage, or uploaded unchanged to its internal stage
with `copy_to_internal_stage = true`. SQLLoad never downloads, decodes and re-uploads them.

The one exception is a column masked with `hmac`: its key must never appear in SQL, so SQLLoad
reads those files and masks the column itself. The plan warns when this happens. Files on an SFTP
server are not an exception: no warehouse reads SFTP, so they never had a direct route, and
`plan` shows them read over SFTP.

Anything else fails rather than falling back to SQLLoad:

- Files no external stage covers fail the plan, and the error says how to fix it.
- Files the warehouse cannot load exactly fail the plan.
- A later batch the warehouse cannot load exactly fails the load.

These checks enforce the rule:

- The fensu rule `XSL002` (`fensu_rules/loader_reads.py`) checks three things:
  - `read_files` is called only from `loader/_helpers/sources.rs`.
  - `Step::FileReader` is built only in routing's `loader_reads`.
  - `plan_route` sends a warehouse destination without `hmac` to `warehouse_reads`, and that function never reaches the reader.
- The route test `given_files_a_warehouse_reads_itself_when_planning_every_format_type_and_strategy_then_only_hmac_reaches_sqlloads_reader`
  plans every format, type and write strategy, unmasked, hashed and `hmac`-masked, and checks
  that only `hmac` reaches SQLLoad's reader.

Warehouse-specific code lives in `crates/sqlload/src/adapters` (fensu rule `XSL001`).
