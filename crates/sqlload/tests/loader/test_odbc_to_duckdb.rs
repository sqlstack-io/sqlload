use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{duckdb_text, run_async, run_sqlload};
use crate::odbc_helpers::{
    filtered_project, null_cursor_project, odbc_connection, postgres_connection, project,
    text_cursor_project,
};
use crate::test_types::{OdbcLoadTestCase, OdbcNullCursorTestCase, RowFilterTestCase};

const SETUP: &str = r#"
CREATE SCHEMA erp;
CREATE TABLE erp.items (id bigint PRIMARY KEY, name text, price numeric(10,2), active boolean, opened date, updated_at timestamp NOT NULL, note text);
INSERT INTO erp.items SELECT g, 'item ' || g, g * 1.25, g % 2 = 0, DATE '2026-01-01' + g, TIMESTAMP '2026-01-01' + g * INTERVAL '1 hour', 'short' FROM generate_series(1, 3000) g;
CREATE TABLE erp."Mixed Case" ("Id" int PRIMARY KEY, "Label" varchar(20));
INSERT INTO erp."Mixed Case" VALUES (1, 'one'), (2, 'two');
CREATE TABLE erp.amounts (id int PRIMARY KEY, n numeric);
INSERT INTO erp.amounts VALUES (1, 1.5), (2, 2.25);
CREATE TABLE erp.ignored (id int);
"#;

const ITEMS: &str = "SELECT count(*) || ':' || sum(price) || ':' || count(*) FILTER (WHERE active) || ':' || max(opened) || ':' || max(updated_at) || ':' || count(DISTINCT _sqlload_load_id) FROM raw.erp__items";

const LABELS: &str = "SELECT (SELECT string_agg(label, ',' ORDER BY label) FROM raw.erp__notes) || '|' || (SELECT string_agg(label, ',' ORDER BY label) FROM raw.erp__codes)";

const ORDERS_FILTER: &str = "where = \"status <> 'X' AND site = 'BE01' -- open orders\"";

const LOAD: &[&str] = &["load", "--yes"];

const FILTERED: &str = "SELECT (SELECT string_agg(id || ':' || status, ',' ORDER BY id) || '|' || count(DISTINCT _sqlload_load_id) FROM raw.erp__orders) || '|' || (SELECT string_agg(code, ',' ORDER BY code) FROM raw.erp__sites)";

#[test]
fn given_an_odbc_source_when_loading_through_its_driver_then_tables_and_queries_load_typed_exactly_and_merge_on_their_cursor()
-> Result<(), String> {
    let test_cases = [
        OdbcLoadTestCase {
            description: "the first load discovers the matching tables, types them from the driver's description, and loads a query table",
            source_change: "",
            max_text_size: 1000,
            expected_output: (true, "4 loaded"),
            check_query: "SELECT (SELECT string_agg(table_name || '.' || column_name || ' ' || data_type, ',' ORDER BY table_name, ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND column_name NOT LIKE '\\_sqlload%' ESCAPE '\\') || '|' || (SELECT string_agg(id || '=' || label, ',' ORDER BY id) FROM raw.erp__mixed_case) || '|' || (SELECT count(*) || ':' || min(id) FROM raw.erp__recent)",
            expected_result: "erp__amounts.id BIGINT,erp__amounts.n DECIMAL(28,6),erp__items.id BIGINT,erp__items.name VARCHAR,erp__items.price DECIMAL(10,2),erp__items.active BOOLEAN,erp__items.opened DATE,erp__items.updated_at TIMESTAMP,erp__items.note VARCHAR,erp__mixed_case.id BIGINT,erp__mixed_case.label VARCHAR,erp__recent.id BIGINT,erp__recent.name VARCHAR|1=one,2=two|10:2991",
        },
        OdbcLoadTestCase {
            description: "a cursor merge reads the changed rows and upserts them",
            source_change: "UPDATE erp.items SET name = 'changed', updated_at = TIMESTAMP '2027-01-01' WHERE id IN (5, 6); INSERT INTO erp.items VALUES (9001, 'new', 1, true, DATE '2026-02-02', TIMESTAMP '2027-01-02', 'n');",
            max_text_size: 1000,
            expected_output: (true, "4 loaded"),
            check_query: ITEMS,
            expected_result: "3001:5626876.00:1501:2034-03-20:2027-01-02 00:00:00:2",
        },
        OdbcLoadTestCase {
            description: "a value longer than max_text_size fails the read instead of being cut, and the table keeps its rows",
            source_change: "UPDATE erp.items SET note = repeat('y', 5000), updated_at = TIMESTAMP '2027-02-01' WHERE id = 7;",
            max_text_size: 1000,
            expected_output: (false, "raise max_text_size"),
            check_query: "SELECT count(*) || ':' || max(length(note)) FROM raw.erp__items",
            expected_result: "3001:5",
        },
        OdbcLoadTestCase {
            description: "with a larger cap the long value loads whole",
            source_change: "",
            max_text_size: 8000,
            expected_output: (true, "4 loaded"),
            check_query: "SELECT count(*) || ':' || max(length(note)) FROM raw.erp__items",
            expected_result: "3001:5000",
        },
        OdbcLoadTestCase {
            description: "a 2 MB value over a cap that makes the column long (read value by value) fails as soon as the read passes the cap",
            source_change: "INSERT INTO erp.items VALUES (9002, 'big', 1, true, DATE '2026-02-02', TIMESTAMP '2027-03-01', repeat('z', 2000000));",
            max_text_size: 8000,
            expected_output: (false, "bytes, longer than max_text_size = 8000"),
            check_query: "SELECT count(*) || ':' || max(length(note)) FROM raw.erp__items",
            expected_result: "3001:5000",
        },
        OdbcLoadTestCase {
            description: "an unbounded numeric, which the driver describes as numeric(28,6), fails on a value with more places instead of rounding it",
            source_change: "DELETE FROM erp.items WHERE id = 9002; INSERT INTO erp.amounts VALUES (3, 1.123456789);",
            max_text_size: 8000,
            expected_output: (false, "n holds 1.123456789"),
            check_query: "SELECT count(*) || ':' || sum(n) FROM raw.erp__amounts",
            expected_result: "2:3.750000",
        },
    ];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| error.to_string())?;
            std::fs::write(
                &config,
                project(postgres.port, &database, test_case.max_text_size),
            )
            .map_err(|error| error.to_string())?;
            let (succeeded, output) = run_sqlload(&config, &["load", "--yes"], ("", ""))?;
            let (expected_success, expected_text) = test_case.expected_output;
            assert!(
                succeeded == expected_success && output.contains(expected_text),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(&database, test_case.check_query)?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_an_odbc_database_that_reads_backslash_escapes_when_a_text_cursor_maximum_holds_a_quote_and_a_backslash_then_the_next_load_reads_exactly_the_rows_after_it()
-> Result<(), String> {
    let test_cases = [
        OdbcLoadTestCase {
            description: "the first load reads every row; the maxima are o'brien\\ and zz\\'x",
            source_change: r"INSERT INTO erp.notes VALUES ('a'), (E'o\'brien\\'); INSERT INTO erp.codes VALUES ('a'), (E'zz\\\'x');",
            max_text_size: 1000,
            expected_output: (true, "2 loaded"),
            check_query: LABELS,
            expected_result: r"a,o'brien\|a,zz\'x",
        },
        OdbcLoadTestCase {
            description: "the next load reads only the rows after each maximum: its bounds are bound parameters, which the database cannot misread as escapes",
            source_change: r"INSERT INTO erp.notes VALUES ('p'), (E'o\'brien'); INSERT INTO erp.codes VALUES (E'zz\\\'y'), (E'zz\\\'');",
            max_text_size: 1000,
            expected_output: (true, "2 loaded"),
            check_query: LABELS,
            expected_result: r"a,o'brien\,p|a,zz\'x,zz\'y",
        },
    ];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(
                "ALTER DATABASE app SET standard_conforming_strings = off; CREATE SCHEMA erp; CREATE TABLE erp.notes (label text); CREATE TABLE erp.codes (label text);",
            )
            .await
            .map_err(|error| error.to_string())?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        std::fs::write(&config, text_cursor_project(postgres.port, &database))
            .map_err(|error| error.to_string())?;
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| error.to_string())?;
            let (succeeded, output) = run_sqlload(&config, &["load", "--yes"], ("", ""))?;
            let (expected_success, expected_text) = test_case.expected_output;
            assert!(
                succeeded == expected_success && output.contains(expected_text),
                "{} (max_text_size {}):\n{output}",
                test_case.description,
                test_case.max_text_size
            );
            assert_eq!(
                duckdb_text(&database, test_case.check_query)?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_an_odbc_source_with_rows_whose_cursor_is_null_when_loading_incrementally_then_the_load_fails_unless_null_cursor_or_a_coalesced_cursor_reads_them()
-> Result<(), String> {
    let test_cases = [
        OdbcNullCursorTestCase {
            description: "the first load reads every row, a NULL cursor too",
            source_change: "",
            settings: "cursor_column = \"updated_at\"",
            expected_output: (true, "1 loaded"),
            expected_result: "1:a,2:b,3:c",
        },
        OdbcNullCursorTestCase {
            description: "the next load counts the NULL-cursor rows its bounds would skip and fails, naming them, and merges nothing",
            source_change: "UPDATE erp.t SET v = 'b2' WHERE id = 2; INSERT INTO erp.t VALUES (4, 'd', NULL, TIMESTAMP '2026-11-05 10:00:00'); UPDATE erp.t SET v = 'a2', updated_at = TIMESTAMP '2026-11-03 10:00:00' WHERE id = 1;",
            settings: "cursor_column = \"updated_at\"",
            expected_output: (false, "2 source rows have a NULL updated_at"),
            expected_result: "1:a,2:b,3:c",
        },
        OdbcNullCursorTestCase {
            description: "null_cursor = \"include\" with the bounds written as ODBC escapes reads them beside the changed rows",
            source_change: "",
            settings: "cursor_column = \"updated_at\"\nnull_cursor = \"include\"\ncursor_bounds = \"literals\"",
            expected_output: (true, "1 loaded"),
            expected_result: "1:a2,2:b2,3:c,4:d",
        },
        OdbcNullCursorTestCase {
            description: "null_cursor = \"include\" with bound parameters reads a new NULL-cursor row too",
            source_change: "INSERT INTO erp.t VALUES (5, 'e', NULL, TIMESTAMP '2026-11-06 10:00:00');",
            settings: "cursor_column = \"updated_at\"\nnull_cursor = \"include\"",
            expected_output: (true, "1 loaded"),
            expected_result: "1:a2,2:b2,3:c,4:d,5:e",
        },
        OdbcNullCursorTestCase {
            description: "a coalesced cursor with ODBC escapes reads a row by its created_at while its updated_at is NULL",
            source_change: "INSERT INTO erp.t VALUES (6, 'f', NULL, TIMESTAMP '2026-11-07 10:00:00');",
            settings: "cursor_column = [\"updated_at\", \"created_at\"]\ncursor_bounds = \"literals\"",
            expected_output: (true, "1 loaded"),
            expected_result: "1:a2,2:b2,3:c,4:d,5:e,6:f",
        },
    ];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(
                "CREATE SCHEMA erp; CREATE TABLE erp.t (id bigint PRIMARY KEY, v text, updated_at timestamp, created_at timestamp NOT NULL);
                INSERT INTO erp.t VALUES (1, 'a', TIMESTAMP '2026-11-01 10:00:00', TIMESTAMP '2026-10-01 10:00:00'), (2, 'b', NULL, TIMESTAMP '2026-10-02 10:00:00'), (3, 'c', TIMESTAMP '2026-11-02 10:00:00', TIMESTAMP '2026-10-03 10:00:00');",
            )
            .await
            .map_err(|error| error.to_string())?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| error.to_string())?;
            std::fs::write(
                &config,
                null_cursor_project(postgres.port, &database, test_case.settings),
            )
            .map_err(|error| error.to_string())?;
            let (succeeded, output) = run_sqlload(&config, &["load", "--yes"], ("", ""))?;
            let (expected_success, expected_text) = test_case.expected_output;
            assert!(
                succeeded == expected_success && output.contains(expected_text),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(
                    &database,
                    "SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM raw.erp__t"
                )?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_where_on_a_database_source_and_its_table_when_loading_then_every_read_selects_only_matching_rows_and_the_table_keeps_its_primary_key()
-> Result<(), String> {
    let test_cases = [
        RowFilterTestCase {
            description: "the first ODBC load reads the orders matching their own where (replacing the source's) and the include-matched sites matching the source's, and plan shows the filter",
            source_kind: "odbc",
            connection: odbc_connection,
            source_change: "",
            orders_filter: ORDERS_FILTER,
            load: LOAD,
            expected_plan: "filter  status <> 'X' AND site = 'BE01' -- open orders",
            expected_output: (true, "2 loaded"),
            expected_result: "1:open,4:open|1|a",
        },
        RowFilterTestCase {
            description: "the next ODBC merge reads only changed rows that match: the cursor filter and the where both narrow its SELECT, and the merge keys on the reported primary key",
            source_change: "UPDATE erp.orders SET updated_at = TIMESTAMP '2026-03-01' WHERE id IN (2, 3, 4); UPDATE erp.orders SET status = 'done' WHERE id = 4; INSERT INTO erp.orders VALUES (5, 'BE01', 'open', TIMESTAMP '2026-03-02'), (6, 'NL01', 'open', TIMESTAMP '2026-03-02');",
            source_kind: "odbc",
            connection: odbc_connection,
            orders_filter: ORDERS_FILTER,
            load: LOAD,
            expected_plan: "filter  status <> 'X' AND site = 'BE01' -- open orders",
            expected_output: (true, "2 loaded"),
            expected_result: "1:open,4:done,5:open|2|a",
        },
        RowFilterTestCase {
            description: "a where the database can't run fails plan, naming the table, and loads nothing",
            source_kind: "odbc",
            connection: odbc_connection,
            source_change: "UPDATE erp.orders SET updated_at = TIMESTAMP '2026-04-01' WHERE id = 1;",
            orders_filter: "where = \"no_such_column = 1\"",
            load: LOAD,
            expected_plan: "where \"no_such_column = 1\" fails on the source",
            expected_output: (false, "where \"no_such_column = 1\" fails on the source"),
            expected_result: "1:open,4:done,5:open|2|a",
        },
        RowFilterTestCase {
            description: "a Postgres source narrows its COPY reads the same way",
            source_kind: "postgres",
            connection: postgres_connection,
            source_change: "",
            orders_filter: ORDERS_FILTER,
            load: LOAD,
            expected_plan: "filter  status <> 'X' AND site = 'BE01' -- open orders",
            expected_output: (true, "2 loaded"),
            expected_result: "1:open,4:done,5:open|1|a",
        },
        RowFilterTestCase {
            description: "the --sync-deletes key scan reads only matching keys too, so a deleted order and one that stopped matching both leave",
            source_kind: "odbc",
            connection: odbc_connection,
            source_change: "DELETE FROM erp.orders WHERE id = 5; UPDATE erp.orders SET status = 'X' WHERE id = 4;",
            orders_filter: "where = \"status <> 'X' AND site = 'BE01'\"\ndeletes = \"hard\"\nchecks = { max_row_drop = \"100%\" }",
            load: &["load", "--yes", "--sync-deletes"],
            expected_plan: "filter  status <> 'X' AND site = 'BE01'",
            expected_output: (true, "2 loaded"),
            expected_result: "1:open|1|a",
        },
    ];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(
                "CREATE SCHEMA erp; CREATE TABLE erp.orders (id int PRIMARY KEY, site text, status text, updated_at timestamp NOT NULL);
                INSERT INTO erp.orders VALUES (1, 'BE01', 'open', TIMESTAMP '2026-01-01'), (2, 'BE01', 'X', TIMESTAMP '2026-01-01'), (3, 'NL01', 'open', TIMESTAMP '2026-01-01'), (4, 'BE01', 'open', TIMESTAMP '2026-01-02');
                CREATE TABLE erp.sites (code text, site text); INSERT INTO erp.sites VALUES ('a', 'BE01'), ('b', 'NL01');",
            )
            .await
            .map_err(|error| error.to_string())?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let config = folder.path().join("sqlload.toml");
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| error.to_string())?;
            let database = folder
                .path()
                .join(format!("{}.duckdb", test_case.source_kind));
            std::fs::write(
                &config,
                filtered_project(
                    (
                        &(test_case.connection)(postgres.port),
                        test_case.source_kind,
                    ),
                    &database,
                    test_case.orders_filter,
                ),
            )
            .map_err(|error| error.to_string())?;
            let (_, plan) = run_sqlload(&config, &["plan"], ("", ""))?;
            assert!(
                plan.contains(test_case.expected_plan),
                "{}:\n{plan}",
                test_case.description
            );
            let (succeeded, output) = run_sqlload(&config, test_case.load, ("", ""))?;
            let (expected_success, expected_text) = test_case.expected_output;
            assert!(
                succeeded == expected_success
                    && output.contains(expected_text)
                    && (succeeded || output.contains("erp__orders")),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(&database, FILTERED)?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
