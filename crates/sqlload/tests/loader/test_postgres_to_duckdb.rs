use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::adapters::dispatch::models::Destination;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::PlanOptions;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{described_project, duckdb_project, load_all, load_selected, run_async};
use crate::test_types::{
    ConstantLoadTestCase, LoadRunTestCase, PinnedRunTestCase, SettingsChangeTestCase,
    TenantStepTestCase,
};

const SETUP: &str = "
CREATE TABLE public.accounts (id int8 PRIMARY KEY, name text, email text, balance numeric(12,2), updated_at timestamptz NOT NULL);
CREATE TABLE public.events (event_id int8, kind text, at timestamptz NOT NULL);
CREATE TABLE public.stores (code text, city text);
INSERT INTO public.accounts VALUES
  (1, 'Acme Ltd', 'a@example.com', 10.50, '2026-11-01 10:00:00+00'),
  (2, 'Beta Co', 'b@example.com', 20.00, '2026-11-02 10:00:00+00');
INSERT INTO public.events VALUES (1, 'open', '2026-11-01 09:00:00+00'), (2, 'close', '2026-11-02 09:00:00+00');
INSERT INTO public.stores VALUES ('L1', 'Leeds'), ('Y1', 'York');
";

const TABLES: &str = r#"
[sources.app.tables."public.accounts"]
write_strategy = "merge"
unique_key = "{primary_key}"
cursor_column = "updated_at"
lookback = "1h"

[sources.app.tables."public.events"]
write_strategy = "delete_insert"
cursor_column = "at"

[sources.app.tables."public.stores"]
write_strategy = "replace"
mask = { city = "hash" }
"#;

const PINNED_SETUP: &str = "
CREATE TABLE public.orders (id int8 PRIMARY KEY, label text, amount numeric(10,2), placed date, updated_at timestamptz NOT NULL);
INSERT INTO public.orders VALUES
  (1, 'first', 1.50, '2026-11-01', '2026-11-01 10:00:00+00'),
  (2, 'second', 2.25, '2026-11-02', '2026-11-02 10:00:00+00');
";

const PINNED_TABLES: &str = r#"include = ["public.orders"]
[sources.app.tables."public.orders"]
write_strategy = "merge"
unique_key = "{primary_key}"
cursor_column = "updated_at"
"#;

const PINNED_DESCRIBE_QUERY: &str = "SELECT string_agg(column_name || ' ' || column_type || ' ' || \"null\", '|') FROM (DESCRIBE raw.app__orders)";

const PINNED_ROWS_QUERY: &str = "SELECT string_agg(CAST(o AS VARCHAR), '|' ORDER BY o.id) FROM (SELECT * EXCLUDE (_sqlload_load_id, _sqlload_loaded_at) FROM raw.app__orders) AS o";

const PINNED_METADATA_QUERY: &str = "SELECT CAST(count(*) FILTER (WHERE _sqlload_load_id IS NULL OR _sqlload_loaded_at IS NULL) AS VARCHAR) FROM raw.app__orders";

const PINNED_FIRST_PLAN: &str = "Plan ready  1 selected, 1 object to create, 1 to load

Objects (1)                                         sqlload apply
└── raw.app__orders                                 create table
    ├── id         int             -> BIGINT
    ├── label      text            -> VARCHAR
    ├── amount     decimal(10,2)   -> DECIMAL(10,2)
    ├── placed     date            -> DATE
    ├── updated_at timestamp_tz    -> TIMESTAMP WITH TIME ZONE  (stored as a UTC instant; the offset is not kept)
    └── + _sqlload_load_id, _loaded_at

Loads (1)                                           sqlload load
└── raw.app__orders                                 merge (unique_key: id ← {primary_key})
    ├── source  postgres  app  public.orders
    ├── route   read as Arrow -> DuckDB takes Arrow
    ├── key     id, the primary key the source reports (unique_key = \"{primary_key}\")
    ├── reads   updated_at: first run, everything
    ├── columns 5
    └── commits once, at the end of the read (updated_at isn't read in order)

✓ Plan complete  1 selected, 1 object to create, 1 to load";

const PINNED_DESCRIBE: &str = "id BIGINT YES|label VARCHAR YES|amount DECIMAL(10,2) YES|placed DATE YES|updated_at TIMESTAMP WITH TIME ZONE YES|_sqlload_load_id VARCHAR YES|_sqlload_loaded_at TIMESTAMP WITH TIME ZONE YES";

const PINNED_SECOND_PLAN: &str = "Plan ready  1 selected, 1 to load

Loads (1)                                           sqlload load
└── raw.app__orders                                 merge (unique_key: id ← {primary_key})
    ├── source  postgres  app  public.orders
    ├── route   read as Arrow -> DuckDB takes Arrow
    ├── key     id, the primary key the source reports (unique_key = \"{primary_key}\")
    ├── reads   updated_at >= 2026-11-02 10:00:00.000+00 (warehouse maximum)
    ├── columns 5
    └── commits once, at the end of the read (updated_at isn't read in order)

✓ Plan complete  1 selected, 1 to load";

const NO_MISSING_METADATA: &str = "0";

/// Two tenants' copies of one table, tenant A's rows newer than all of tenant B's, and both with id 1.
const TENANTS_SETUP: &str = "
CREATE SCHEMA a;
CREATE SCHEMA b;
CREATE TABLE a.orders (id int8 PRIMARY KEY, label text, updated_at timestamptz NOT NULL);
CREATE TABLE b.orders (id int8 PRIMARY KEY, label text, updated_at timestamptz NOT NULL);
INSERT INTO a.orders VALUES (1, 'a-one', '2026-11-05 10:00:00+00'), (2, 'a-two', '2026-11-06 10:00:00+00');
INSERT INTO b.orders VALUES (1, 'b-one', '2026-11-01 10:00:00+00'), (3, 'b-three', '2026-11-02 10:00:00+00');
";

/// Source `app` is tenant A, `tenant_b` tenant B: one destination table, told apart by `tenant_id`.
const TENANTS_TABLES: &str = r#"include = ["a.orders"]
table_name = "{table}"
constant_columns = { tenant_id = "a" }

[sources.app.tables."a.orders"]
write_strategy = "merge"
unique_key = ["tenant_id", "id"]
cursor_column = "updated_at"

[sources.tenant_b]
kind = "postgres"
connection = "app_pg"
include = ["b.orders"]
table_name = "{table}"
constant_columns = { tenant_id = "b" }

[sources.tenant_b.tables."b.orders"]
write_strategy = "merge"
unique_key = ["tenant_id", "id"]
cursor_column = "updated_at"
"#;

/// A Postgres connection nothing listens on, for a source that cannot be read.
const DOWN_CONNECTION: &str = r#"[connections.down_pg]
kind = "postgres"
host = "127.0.0.1"
port = 1
user = "sqlload"
password = "sqlload"
database = "app"
"#;

const TENANT_VALUES_QUERY: &str = "SELECT string_agg(tenant_id || ':' || id || ':' || label, ',' ORDER BY tenant_id, id) FROM raw.orders";

const TENANT_ROWS_QUERY: &str = "SELECT string_agg(id || ':' || label || ':' || _sqlload_load_id || ':' || CAST(_sqlload_loaded_at AS VARCHAR), ',' ORDER BY id) FROM raw.orders WHERE tenant_id = 'TENANT'";

const CONSTANTS_SETUP: &str = "
CREATE TABLE public.items (id int8 PRIMARY KEY, label text);
CREATE TABLE public.clashing (id int8 PRIMARY KEY, \"Tenant_ID\" text);
CREATE TABLE public.other_items (id int8 PRIMARY KEY, label text);
INSERT INTO public.items VALUES (1, 'one'), (2, 'two');
INSERT INTO public.clashing VALUES (1, 'x');
INSERT INTO public.other_items VALUES (1, 'other');
";

const NO_SUCH_TABLE: &str = "0";

const ADDED_CONSTANT_BEFORE: &str = r#"include = ["public.items"]
[sources.app.tables."public.items"]
write_strategy = "append"
cursor_column = "id"
"#;

const ADDED_CONSTANT_AFTER: &str = r#"include = ["public.items"]
[sources.app.tables."public.items"]
write_strategy = "append"
cursor_column = "id"
constant_columns = { run_tag = "second" }
"#;

const ADDED_CONSTANT_ROWS_QUERY: &str = "SELECT string_agg(id || ':' || label || ':' || coalesce(run_tag, ''), ',' ORDER BY id) FROM raw.app__items";

/// A table whose quoted names dlt's naming renames, read in integer windows of one key each.
const DLT_SETUP: &str = r#"
CREATE TABLE public."Orders-V2" ("OrderID" int8 PRIMARY KEY, "Seq" int8 NOT NULL, "Unit Price" numeric(10,2), "UpdatedAt" timestamptz NOT NULL);
INSERT INTO public."Orders-V2" VALUES (1, 1, 1.50, '2026-11-01 10:00:00+00'), (2, 2, 2.25, '2026-11-02 10:00:00+00');
"#;

/// The key named as the source spells it, the cursor as it loads.
const DLT_TABLES: &str = r#"include = ["public.Orders-V2"]
[sources.app.tables."public.Orders-V2"]
naming = "dlt"
write_strategy = "merge"
unique_key = ["OrderID"]
cursor_column = "seq"
cursor_start = "0"
batch_size = 1
"#;

const DLT_DESCRIBE_QUERY: &str =
    "SELECT string_agg(column_name || ' ' || column_type, '|') FROM (DESCRIBE raw.app__orders_v2)";

const DLT_ROWS_QUERY: &str = "SELECT string_agg(order_id || ':' || seq || ':' || unit_price || ':' || CAST(updated_at AS VARCHAR), '|' ORDER BY order_id) FROM raw.app__orders_v2";

const DLT_FIRST_PLAN: &str = "Plan ready  1 selected, 1 object to create, 1 to load

Objects (1)                                         sqlload apply
└── raw.app__orders_v2                              create table
    ├── order_id   int             -> BIGINT
    ├── seq        int             -> BIGINT
    ├── unit_price decimal(10,2)   -> DECIMAL(10,2)
    ├── updated_at timestamp_tz    -> TIMESTAMP WITH TIME ZONE  (stored as a UTC instant; the offset is not kept)
    └── + _dlt_load_id, _loaded_at, _dlt_id

Loads (1)                                           sqlload load
└── raw.app__orders_v2                              merge (unique_key: order_id)
    ├── source  postgres  app  public.Orders-V2
    ├── route   read as Arrow -> DuckDB takes Arrow
    ├── key     from unique_key; the source reports the same primary key
    ├── reads   seq >= 0 (first run, from cursor_start)
    ├── columns 4, named by naming = \"dlt\": OrderID -> order_id, Seq -> seq, ...
    ├── commits 3 windows of 1, one commit each, up to the source's maximum 2 (read at plan)
    └── note    windows are read in one REPEATABLE READ snapshot, held open on the source for the whole table load (mind idle_in_transaction_session_timeout and vacuum)

✓ Plan complete  1 selected, 1 object to create, 1 to load";

const DLT_SECOND_PLAN: &str = "Plan ready  1 selected, 1 to load

Loads (1)                                           sqlload load
└── raw.app__orders_v2                              merge (unique_key: order_id)
    ├── source  postgres  app  public.Orders-V2
    ├── route   read as Arrow -> DuckDB takes Arrow
    ├── key     from unique_key; the source reports the same primary key
    ├── reads   seq >= 2 (warehouse maximum)
    ├── columns 4, named by naming = \"dlt\": OrderID -> order_id, Seq -> seq, ...
    ├── commits 2 windows of 1, one commit each, up to the source's maximum 3 (read at plan)
    └── note    windows are read in one REPEATABLE READ snapshot, held open on the source for the whole table load (mind idle_in_transaction_session_timeout and vacuum)

✓ Plan complete  1 selected, 1 to load";

const DLT_DESCRIBE: &str = "order_id BIGINT|seq BIGINT|unit_price DECIMAL(10,2)|updated_at TIMESTAMP WITH TIME ZONE|_dlt_load_id VARCHAR|_sqlload_loaded_at TIMESTAMP WITH TIME ZONE|_dlt_id VARCHAR";

#[test]
fn given_postgres_tables_when_loading_repeatedly_into_duckdb_then_each_strategy_keeps_the_table_right()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let text = duckdb_project(postgres.port, TABLES);
        let project = described_project(&text).await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
            return Err("no DuckDB destination".to_owned());
        };
        let test_cases = [
            LoadRunTestCase {
                description: "the first load creates every table without excluded columns",
                source_change: "",
                check_query: "SELECT string_agg(table_name || '.' || column_name, ',' ORDER BY table_name, ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'app__accounts'",
                expected_result: "app__accounts.id,app__accounts.name,app__accounts.balance,app__accounts.updated_at,app__accounts._sqlload_load_id,app__accounts._sqlload_loaded_at",
            },
            LoadRunTestCase {
                description: "merge with a cursor upserts changed rows and keeps the rest",
                source_change: "UPDATE public.accounts SET balance = 99.99, updated_at = '2026-11-03 10:00:00+00' WHERE id = 2; INSERT INTO public.accounts VALUES (3, 'Gamma', 'g@example.com', 1.00, '2026-11-03 11:00:00+00');",
                check_query: "SELECT string_agg(id || '=' || balance, ',' ORDER BY id) FROM raw.app__accounts",
                expected_result: "1=10.50,2=99.99,3=1.00",
            },
            LoadRunTestCase {
                description: "a rerun with no changes leaves exactly one row per key",
                source_change: "",
                check_query: "SELECT count(*) || '/' || count(DISTINCT id) FROM raw.app__accounts",
                expected_result: "3/3",
            },
            LoadRunTestCase {
                description: "delete_insert replaces the rows from the cursor onwards without duplicates",
                source_change: "INSERT INTO public.events VALUES (3, 'reopen', '2026-11-02 09:00:00+00');",
                check_query: "SELECT string_agg(event_id || ':' || kind, ',' ORDER BY event_id) FROM raw.app__events",
                expected_result: "1:open,2:close,3:reopen",
            },
            LoadRunTestCase {
                description: "replace without a cursor mirrors deletes in the source",
                source_change: "DELETE FROM public.stores WHERE code = 'Y1';",
                check_query: "SELECT string_agg(code, ',' ORDER BY code) FROM raw.app__stores",
                expected_result: "L1",
            },
            LoadRunTestCase {
                description: "a masked column holds the SHA-256 hex of each value",
                source_change: "",
                check_query: "SELECT string_agg(code || '@' || city, ',' ORDER BY code) FROM raw.app__stores",
                expected_result: "L1@8593b9db65212160979d946950601c26622a219c80f1f122679eb69ec5b96600",
            },
            LoadRunTestCase {
                description: "a new source column is added to the table and filled",
                source_change: "ALTER TABLE public.stores ADD COLUMN opened date; UPDATE public.stores SET opened = '2020-01-02';",
                check_query: "SELECT string_agg(code || '@' || CAST(opened AS VARCHAR), ',') FROM raw.app__stores",
                expected_result: "L1@2020-01-02",
            },
            LoadRunTestCase {
                description: "every row carries the metadata of the load that wrote it",
                source_change: "",
                check_query: "SELECT CAST(count(*) FILTER (WHERE _sqlload_load_id IS NULL OR _sqlload_loaded_at IS NULL) AS VARCHAR) FROM raw.app__stores",
                expected_result: "0",
            },
        ];
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| error.to_string())?;
            let plan = plan_project(&project, &destinations, &PlanOptions::default())
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            let (lines, ok) = render_plan(&plan);
            assert!(ok, "{}:\n{}", test_case.description, lines.join("\n"));
            apply_project(&plan, &destinations)
                .await
                .map_err(|error| error.to_string())?;
            load_all(&project, &plan, &destinations)
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            let result = single_text(duckdb, test_case.check_query.to_owned())
                .await
                .map_err(|error| error.to_string())?;
            assert_eq!(
                result.as_deref(),
                Some(test_case.expected_result),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_table_without_constant_columns_when_planning_and_loading_twice_then_its_plan_columns_and_rows_stay_byte_identical()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(PINNED_SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let text = duckdb_project(postgres.port, PINNED_TABLES);
        let project = described_project(&text).await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
            return Err("no DuckDB destination".to_owned());
        };
        let test_cases = [
            PinnedRunTestCase {
                description: "the first load creates the table from the source's columns",
                source_change: "",
                expected_plan: PINNED_FIRST_PLAN,
                expected_describe: PINNED_DESCRIBE,
                expected_rows: "{'id': 1, 'label': first, 'amount': 1.50, 'placed': 2026-11-01, 'updated_at': '2026-11-01 10:00:00+00'}|{'id': 2, 'label': second, 'amount': 2.25, 'placed': 2026-11-02, 'updated_at': '2026-11-02 10:00:00+00'}",
            },
            PinnedRunTestCase {
                description: "an incremental merge reads from the destination maximum",
                source_change: "UPDATE public.orders SET label = 'changed', updated_at = '2026-11-03 10:00:00+00' WHERE id = 1;",
                expected_plan: PINNED_SECOND_PLAN,
                expected_describe: PINNED_DESCRIBE,
                expected_rows: "{'id': 1, 'label': changed, 'amount': 1.50, 'placed': 2026-11-01, 'updated_at': '2026-11-03 10:00:00+00'}|{'id': 2, 'label': second, 'amount': 2.25, 'placed': 2026-11-02, 'updated_at': '2026-11-02 10:00:00+00'}",
            },
        ];
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| error.to_string())?;
            let plan = plan_project(&project, &destinations, &PlanOptions::default())
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            let (lines, ok) = render_plan(&plan);
            assert!(ok, "{}:\n{}", test_case.description, lines.join("\n"));
            assert_eq!(
                lines.join("\n"),
                test_case.expected_plan,
                "{}",
                test_case.description
            );
            apply_project(&plan, &destinations)
                .await
                .map_err(|error| error.to_string())?;
            load_all(&project, &plan, &destinations)
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            let checks = [
                (PINNED_DESCRIBE_QUERY, test_case.expected_describe),
                (PINNED_ROWS_QUERY, test_case.expected_rows),
                (PINNED_METADATA_QUERY, NO_MISSING_METADATA),
            ];
            for (query, expected) in checks {
                let result = single_text(duckdb, query.to_owned())
                    .await
                    .map_err(|error| error.to_string())?;
                assert_eq!(
                    result.as_deref(),
                    Some(expected),
                    "{}: {query}",
                    test_case.description
                );
            }
        }
        Ok(())
    })
}

#[test]
fn given_two_tenants_merging_into_one_table_when_each_loads_then_each_reads_from_its_own_maximum_and_never_touches_the_others_rows()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(TENANTS_SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let text = duckdb_project(postgres.port, TENANTS_TABLES);
        let project = described_project(&text).await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
            return Err("no DuckDB destination".to_owned());
        };
        load_selected(&project, &destinations, &["app"]).await?;
        let test_cases = [
            TenantStepTestCase {
                description: "tenant B's first run reads everything, not from tenant A's newer maximum, and loads beside A's rows of the same id",
                source_change: "",
                select: "tenant_b",
                untouched_tenant: "a",
                unreadable: "",
                expected_cursor: "reads   updated_at: empty table, everything",
                expected_warning: "",
                expected_values: "a:1:a-one,a:2:a-two,b:1:b-one,b:3:b-three",
            },
            TenantStepTestCase {
                description: "tenant A's update merges into A's id 1 only",
                source_change: "UPDATE a.orders SET label = 'a-one-changed', updated_at = '2026-11-07 10:00:00+00' WHERE id = 1;",
                select: "app",
                untouched_tenant: "b",
                unreadable: "",
                expected_cursor: "reads   updated_at >= 2026-11-06 10:00:00.000+00 (warehouse maximum)",
                expected_warning: "",
                expected_values: "a:1:a-one-changed,a:2:a-two,b:1:b-one,b:3:b-three",
            },
            TenantStepTestCase {
                description: "tenant B's incremental read starts from B's own maximum, so its new row older than A's loads",
                source_change: "INSERT INTO b.orders VALUES (4, 'b-four', '2026-11-03 10:00:00+00');",
                select: "tenant_b",
                untouched_tenant: "a",
                unreadable: "",
                expected_cursor: "reads   updated_at >= 2026-11-02 10:00:00.000+00 (warehouse maximum)",
                expected_warning: "",
                expected_values: "a:1:a-one-changed,a:2:a-two,b:1:b-one,b:3:b-three,b:4:b-four",
            },
            TenantStepTestCase {
                description: "with tenant A's source unreadable, tenant B still reads from B's own maximum, not the whole table's",
                source_change: "INSERT INTO b.orders VALUES (5, 'b-five', '2026-11-03 12:00:00+00');",
                select: "tenant_b",
                untouched_tenant: "a",
                unreadable: "app",
                expected_cursor: "reads   updated_at >= 2026-11-03 10:00:00.000+00 (warehouse maximum)",
                expected_warning: "app could not be read, so tenant_b.b.orders may share a destination table with it",
                expected_values: "a:1:a-one-changed,a:2:a-two,b:1:b-one,b:3:b-three,b:4:b-four,b:5:b-five",
            },
        ];
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| error.to_string())?;
            let untouched = TENANT_ROWS_QUERY.replace("TENANT", test_case.untouched_tenant);
            let before = single_text(duckdb, untouched.clone())
                .await
                .map_err(|error| error.to_string())?;
            let source = format!(
                "[sources.{}]\nkind = \"postgres\"\nconnection = \"app_pg\"",
                test_case.unreadable
            );
            let down = format!(
                "[sources.{}]\nkind = \"postgres\"\nconnection = \"down_pg\"",
                test_case.unreadable
            );
            let stepped = described_project(&format!(
                "{}\n{DOWN_CONNECTION}",
                text.replacen(&source, &down, 1)
            ))
            .await?;
            let plan = load_selected(&stepped, &destinations, &[test_case.select])
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            assert!(
                plan.contains(test_case.expected_cursor)
                    && plan.contains(test_case.expected_warning),
                "{}:\n{plan}",
                test_case.description
            );
            let values = single_text(duckdb, TENANT_VALUES_QUERY.to_owned())
                .await
                .map_err(|error| error.to_string())?;
            assert_eq!(
                values.as_deref(),
                Some(test_case.expected_values),
                "{}",
                test_case.description
            );
            let after = single_text(duckdb, untouched)
                .await
                .map_err(|error| error.to_string())?;
            assert_eq!(
                after, before,
                "{}: the other tenant's rows, load id and load time are unchanged",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_table_of_its_own_with_rows_when_a_constant_is_added_then_its_cursor_keeps_the_destination_maximum()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(CONSTANTS_SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let test_cases = [SettingsChangeTestCase {
            description: "a constant added to an append table of its own: its NULLs in earlier rows don't restart the cursor, so no row loads twice",
            before: ADDED_CONSTANT_BEFORE,
            after: ADDED_CONSTANT_AFTER,
            expected_cursor: "reads   id > 2 (warehouse maximum)",
            check_query: ADDED_CONSTANT_ROWS_QUERY,
            expected_result: "1:one:,2:two:",
        }];
        for test_case in test_cases {
            let before = duckdb_project(postgres.port, test_case.before);
            let project = described_project(&before).await?;
            let destinations = connect_targets(&project)
                .await
                .map_err(|error| error.to_string())?;
            let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
                return Err("no DuckDB destination".to_owned());
            };
            load_selected(&project, &destinations, &[]).await?;
            let after = duckdb_project(postgres.port, test_case.after);
            let project = described_project(&after).await?;
            let plan = load_selected(&project, &destinations, &[]).await?;
            assert!(
                plan.contains(test_case.expected_cursor),
                "{}:\n{plan}",
                test_case.description
            );
            let result = single_text(duckdb, test_case.check_query.to_owned())
                .await
                .map_err(|error| error.to_string())?;
            assert_eq!(
                result.as_deref(),
                Some(test_case.expected_result),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_constant_columns_when_planning_and_loading_then_they_are_typed_and_never_replace_or_remove_other_rows()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(CONSTANTS_SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let test_cases = [
            ConstantLoadTestCase {
                description: "a typed constant loads as its type, in every row",
                settings: "include = [\"public.item*\"]\nwrite_strategy = \"replace\"\nconstant_columns = { shard = { value = \"7\", type = \"int\" }, tenant_id = \"a\" }\n",
                expected_error: "",
                check_query: "SELECT (SELECT string_agg(column_name || ' ' || column_type, ',') FROM (DESCRIBE raw.app__items)) || '|' || (SELECT string_agg(id || ':' || shard || ':' || tenant_id, ',' ORDER BY id) FROM raw.app__items)",
                expected_result: "id BIGINT,label VARCHAR,shard BIGINT,tenant_id VARCHAR,_sqlload_load_id VARCHAR,_sqlload_loaded_at TIMESTAMP WITH TIME ZONE|1:7:a,2:7:a",
            },
            ConstantLoadTestCase {
                description: "a table's own constant_columns replace its source's",
                settings: "include = [\"public.item*\"]\nwrite_strategy = \"replace\"\nconstant_columns = { tenant_id = \"source\" }\n[sources.app.tables.\"public.items\"]\nconstant_columns = { region = \"eu\" }\n",
                expected_error: "",
                check_query: "SELECT string_agg(column_name, ',') FROM (DESCRIBE raw.app__items)",
                expected_result: "id,label,region,_sqlload_load_id,_sqlload_loaded_at",
            },
            ConstantLoadTestCase {
                description: "a source column named like a constant, in another case, fails the plan, naming it",
                settings: "include = [\"public.clash*\"]\nwrite_strategy = \"replace\"\nconstant_columns = { tenant_id = \"a\" }\n",
                expected_error: "constant column tenant_id: the source already has a column Tenant_ID, and a constant never replaces it",
                check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'app__clashing'",
                expected_result: NO_SUCH_TABLE,
            },
            ConstantLoadTestCase {
                description: "excluding the source's column of the constant's name loads the constant instead",
                settings: "include = [\"public.clash*\"]\nwrite_strategy = \"replace\"\nconstant_columns = { tenant_id = \"a\" }\n[sources.app.tables.\"public.clashing\"]\nexclude_columns = [\"Tenant_ID\"]\n",
                expected_error: "",
                check_query: "SELECT string_agg(id || ':' || tenant_id, ',') FROM raw.app__clashing",
                expected_result: "1:a",
            },
            ConstantLoadTestCase {
                description: "a constant cannot be a database table's cursor",
                settings: "include = [\"public.item*\"]\nwrite_strategy = \"replace\"\nconstant_columns = { tenant_id = \"a\" }\n[sources.app.tables.\"public.items\"]\nwrite_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = \"tenant_id\"\n",
                expected_error: "constant column tenant_id cannot be the cursor_column",
                check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'app__items'",
                expected_result: NO_SUCH_TABLE,
            },
            ConstantLoadTestCase {
                description: "tables with constant columns sharing a destination cannot replace it, which would remove the other's rows",
                settings: "include = [\"public.item*\"]\nwrite_strategy = \"replace\"\ntable_name = \"{table}\"\nconstant_columns = { tenant_id = \"a\" }\n[sources.other]\nkind = \"postgres\"\nconnection = \"app_pg\"\ninclude = [\"public.other_item*\"]\nconstant_columns = { tenant_id = \"b\" }\nwrite_strategy = \"replace\"\n[sources.other.tables.\"public.other_items\"]\nname = \"items\"\n",
                expected_error: "items is also loaded by other.public.other_items, and constant_columns tell their rows apart; app.public.items uses write_strategy = \"replace\"",
                check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'items'",
                expected_result: NO_SUCH_TABLE,
            },
            ConstantLoadTestCase {
                description: "tables with constant columns sharing a destination cannot merge on a key without a constant",
                settings: "include = [\"public.item*\"]\ntable_name = \"{table}\"\nconstant_columns = { tenant_id = \"a\" }\nwrite_strategy = \"merge\"\nunique_key = \"{primary_key}\"\n[sources.other]\nkind = \"postgres\"\nconnection = \"app_pg\"\ninclude = [\"public.other_item*\"]\nconstant_columns = { tenant_id = \"b\" }\nwrite_strategy = \"merge\"\nunique_key = [\"tenant_id\", \"id\"]\n[sources.other.tables.\"public.other_items\"]\nname = \"items\"\n",
                expected_error: "its unique_key (id) has no constant column, so its merge could overwrite their rows",
                check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'items'",
                expected_result: NO_SUCH_TABLE,
            },
            ConstantLoadTestCase {
                description: "tables sharing a destination with the same constant values cannot merge, as their rows and cursors can't be told apart",
                settings: "include = [\"public.item*\"]\ntable_name = \"{table}\"\nconstant_columns = { tenant_id = \"a\", region = \"eu\" }\nwrite_strategy = \"merge\"\nunique_key = [\"region\", \"id\"]\n[sources.other]\nkind = \"postgres\"\nconnection = \"app_pg\"\ninclude = [\"public.other_item*\"]\nconstant_columns = { tenant_id = \"b\", region = \"eu\" }\nwrite_strategy = \"merge\"\nunique_key = [\"region\", \"id\"]\n[sources.other.tables.\"public.other_items\"]\nname = \"items\"\n",
                expected_error: "has the same values for the constant columns in the unique_key, so their rows and incremental cursors can't be told apart",
                check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'items'",
                expected_result: NO_SUCH_TABLE,
            },
        ];
        for test_case in test_cases {
            let text = duckdb_project(postgres.port, test_case.settings);
            let project = described_project(&text).await?;
            let destinations = connect_targets(&project)
                .await
                .map_err(|error| error.to_string())?;
            let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
                return Err("no DuckDB destination".to_owned());
            };
            let outcome = load_selected(&project, &destinations, &[]).await;
            let error = outcome.err().unwrap_or_default();
            let words = error
                .split_whitespace()
                .filter(|word| *word != "│")
                .collect::<Vec<_>>()
                .join(" ");
            assert!(
                words.contains(test_case.expected_error)
                    && error.is_empty() == test_case.expected_error.is_empty(),
                "{}: {error}",
                test_case.description
            );
            let result = single_text(duckdb, test_case.check_query.to_owned())
                .await
                .map_err(|error| error.to_string())?;
            assert_eq!(
                result.as_deref(),
                Some(test_case.expected_result),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_quoted_source_names_under_dlt_naming_when_loading_twice_then_the_table_and_columns_load_snake_cased_and_merge_by_the_renamed_key()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(DLT_SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let text = duckdb_project(postgres.port, DLT_TABLES);
        let project = described_project(&text).await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let Some(duckdb @ Destination::DuckDb(_)) = destinations.get("dev") else {
            return Err("no DuckDB destination".to_owned());
        };
        let test_cases = [
            PinnedRunTestCase {
                description: "the first load names the table and columns as dlt does, reading each window by the source's names",
                source_change: "",
                expected_plan: DLT_FIRST_PLAN,
                expected_describe: DLT_DESCRIBE,
                expected_rows: "1:1:1.50:2026-11-01 10:00:00+00|2:2:2.25:2026-11-02 10:00:00+00",
            },
            PinnedRunTestCase {
                description: "an incremental merge reads after the destination maximum and replaces the row by its renamed key",
                source_change: r#"UPDATE public."Orders-V2" SET "Seq" = 3, "Unit Price" = 9.99 WHERE "OrderID" = 1;"#,
                expected_plan: DLT_SECOND_PLAN,
                expected_describe: DLT_DESCRIBE,
                expected_rows: "1:3:9.99:2026-11-01 10:00:00+00|2:2:2.25:2026-11-02 10:00:00+00",
            },
        ];
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| error.to_string())?;
            let plan = plan_project(&project, &destinations, &PlanOptions::default())
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            let (lines, ok) = render_plan(&plan);
            assert!(ok, "{}:\n{}", test_case.description, lines.join("\n"));
            assert_eq!(
                lines.join("\n"),
                test_case.expected_plan,
                "{}",
                test_case.description
            );
            apply_project(&plan, &destinations)
                .await
                .map_err(|error| error.to_string())?;
            load_all(&project, &plan, &destinations)
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            let checks = [
                (DLT_DESCRIBE_QUERY, test_case.expected_describe),
                (DLT_ROWS_QUERY, test_case.expected_rows),
            ];
            for (query, expected) in checks {
                let result = single_text(duckdb, query.to_owned())
                    .await
                    .map_err(|error| error.to_string())?;
                assert_eq!(
                    result.as_deref(),
                    Some(expected),
                    "{}: {query}",
                    test_case.description
                );
            }
        }
        Ok(())
    })
}
