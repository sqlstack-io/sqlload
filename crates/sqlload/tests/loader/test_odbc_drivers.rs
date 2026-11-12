use sqlload_testkit::odbc_servers::main::start_odbc_server::start_odbc_server;
use sqlload_testkit::odbc_servers::models::OdbcDatabase;

use crate::helpers::{duckdb_text, run_async, run_sqlload};
use crate::odbc_helpers::{driver_project, odbc_execute};
use crate::test_types::OdbcDriverTestCase;

const TABLES: &str = "SELECT (SELECT string_agg(id::VARCHAR, ',' ORDER BY id) || ' to ' || strftime(max(happened_at), '%Y-%m-%d %H:%M:%S.%g') FROM raw.erp__events) || ' | ' || (SELECT string_agg(id::VARCHAR, ',' ORDER BY id) || ' to ' || max(opened)::VARCHAR FROM raw.erp__days)";

const FIRST: &str = "1,2 to 2026-01-31 23:59:59.997 | 1,2 to 2026-01-31";

const SECOND: &str = "1,2,3,4 to 2026-02-01 00:00:00.000 | 1,2,3,4 to 2026-02-01";

#[test]
fn given_each_odbc_driver_when_a_millisecond_timestamp_cursor_and_a_date_cursor_resume_then_the_next_load_reads_exactly_the_rows_after_each_boundary()
-> Result<(), String> {
    let test_cases = [
        OdbcDriverTestCase {
            description: "SQL Server through Microsoft's ODBC Driver 18: {ts} and {d} bounds on datetime2(3) and date",
            database: OdbcDatabase::SqlServer,
            tables: ("erp.events", "erp.days"),
            setup: &[
                "CREATE SCHEMA erp",
                "CREATE TABLE erp.events (id int PRIMARY KEY, name nvarchar(40), happened_at datetime2(3) NOT NULL)",
                "CREATE TABLE erp.days (id int PRIMARY KEY, opened date NOT NULL)",
                "INSERT INTO erp.events VALUES (1, 'a', '2026-01-31 10:00:00'), (2, 'b', '2026-01-31 23:59:59.997')",
                "INSERT INTO erp.days VALUES (1, '2026-01-30'), (2, '2026-01-31')",
            ],
            change: &[
                "INSERT INTO erp.events VALUES (0, 'early', '2026-01-31 23:59:59.996'), (3, 'next', '2026-01-31 23:59:59.998'), (4, 'later', '2026-02-01 00:00:00')",
                "INSERT INTO erp.days VALUES (3, '2026-01-31'), (4, '2026-02-01')",
            ],
            expected_tables: (FIRST, SECOND),
        },
        OdbcDriverTestCase {
            description: "MySQL through Connector/ODBC, a driver without schemas, in an ANSI_QUOTES session: {ts} and {d} bounds on DATETIME(3) and DATE",
            database: OdbcDatabase::MySql,
            tables: ("events", "days"),
            setup: &[
                "CREATE TABLE events (id int PRIMARY KEY, name varchar(40), happened_at DATETIME(3) NOT NULL)",
                "CREATE TABLE days (id int PRIMARY KEY, opened DATE NOT NULL)",
                "INSERT INTO events VALUES (1, 'a', '2026-01-31 10:00:00'), (2, 'b', '2026-01-31 23:59:59.997')",
                "INSERT INTO days VALUES (1, '2026-01-30'), (2, '2026-01-31')",
            ],
            change: &[
                "INSERT INTO events VALUES (0, 'early', '2026-01-31 23:59:59.996'), (3, 'next', '2026-01-31 23:59:59.998'), (4, 'later', '2026-02-01 00:00:00')",
                "INSERT INTO days VALUES (3, '2026-01-31'), (4, '2026-02-01')",
            ],
            expected_tables: (FIRST, SECOND),
        },
        OdbcDriverTestCase {
            description: "Postgres through psqlODBC: {ts} and {d} bounds on timestamp(3) and date",
            database: OdbcDatabase::Postgres,
            tables: ("erp.events", "erp.days"),
            setup: &[
                "CREATE SCHEMA erp",
                "CREATE TABLE erp.events (id int PRIMARY KEY, name varchar(40), happened_at timestamp(3) NOT NULL)",
                "CREATE TABLE erp.days (id int PRIMARY KEY, opened date NOT NULL)",
                "INSERT INTO erp.events VALUES (1, 'a', '2026-01-31 10:00:00'), (2, 'b', '2026-01-31 23:59:59.997')",
                "INSERT INTO erp.days VALUES (1, '2026-01-30'), (2, '2026-01-31')",
            ],
            change: &[
                "INSERT INTO erp.events VALUES (0, 'early', '2026-01-31 23:59:59.996'), (3, 'next', '2026-01-31 23:59:59.998'), (4, 'later', '2026-02-01 00:00:00')",
                "INSERT INTO erp.days VALUES (3, '2026-01-31'), (4, '2026-02-01')",
            ],
            expected_tables: (FIRST, SECOND),
        },
    ];
    for test_case in test_cases {
        run_async(async {
            let server = start_odbc_server(test_case.database).await?;
            odbc_execute(&server.connection_string, test_case.setup)?;
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let database = folder.path().join("out.duckdb");
            let config = folder.path().join("sqlload.toml");
            std::fs::write(
                &config,
                driver_project(&server.connection_string, test_case.tables, &database),
            )
            .map_err(|error| error.to_string())?;
            let (succeeded, output) = run_sqlload(&config, &["load", "--yes"], ("", ""))?;
            assert!(
                succeeded && output.contains("2 loaded"),
                "{}, first load:\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(&database, TABLES)?,
                test_case.expected_tables.0,
                "{}, first load",
                test_case.description
            );
            odbc_execute(&server.connection_string, test_case.change)?;
            let (succeeded, output) = run_sqlload(&config, &["load", "--yes"], ("", ""))?;
            assert!(
                succeeded && output.contains("2 loaded"),
                "{}, second load:\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(&database, TABLES)?,
                test_case.expected_tables.1,
                "{}, second load: the event at .996 is before the maximum and the day 2026-01-31 is read again",
                test_case.description
            );
            Ok(())
        })?;
    }
    Ok(())
}
