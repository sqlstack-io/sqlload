use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{duckdb_text, run_async, run_sqlload};
use crate::odbc_helpers::{project, text_cursor_project};
use crate::test_types::OdbcLoadTestCase;

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

#[test]
fn given_an_odbc_source_when_loading_through_its_driver_then_tables_and_queries_load_typed_exactly_and_merge_on_their_cursor()
-> Result<(), String> {
    let test_cases = [
        OdbcLoadTestCase {
            description: "the first load discovers the matching tables, types them from the driver's description, and loads a query table",
            source_change: "",
            max_text_size: 1000,
            expected_output: (true, "4 loaded"),
            check_query: "SELECT (SELECT string_agg(table_name || '.' || column_name || ' ' || data_type, ',' ORDER BY table_name, ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND column_name NOT LIKE '\\_sqlload%' ESCAPE '\\') || '|' || (SELECT string_agg(\"Id\" || '=' || \"Label\", ',' ORDER BY \"Id\") FROM raw.\"erp__mixed case\") || '|' || (SELECT count(*) || ':' || min(id) FROM raw.erp__recent)",
            expected_result: "erp__amounts.id BIGINT,erp__amounts.n DECIMAL(28,6),erp__items.id BIGINT,erp__items.name VARCHAR,erp__items.price DECIMAL(10,2),erp__items.active BOOLEAN,erp__items.opened DATE,erp__items.updated_at TIMESTAMP,erp__items.note VARCHAR,erp__mixed case.Id BIGINT,erp__mixed case.Label VARCHAR,erp__recent.id BIGINT,erp__recent.name VARCHAR|1=one,2=two|10:2991",
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
            description: "an unbounded numeric, which the driver describes as numeric(28,6), fails on a value with more places instead of rounding it",
            source_change: "INSERT INTO erp.amounts VALUES (3, 1.123456789);",
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
