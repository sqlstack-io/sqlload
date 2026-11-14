use sqlload_testkit::odbc_servers::main::start_odbc_server::start_odbc_server;
use sqlload_testkit::odbc_servers::models::OdbcDatabase;

use crate::helpers::{duckdb_text, run_async, run_sqlload, words};
use crate::odbc_helpers::{driver_project, keyless_project, odbc_execute};
use crate::test_types::{OdbcDriverTestCase, OdbcKeylessTestCase};

const TABLES: &str = "SELECT (SELECT string_agg(id::VARCHAR, ',' ORDER BY id) || ' to ' || strftime(max(happened_at), '%Y-%m-%d %H:%M:%S.%g') FROM raw.erp__events) || ' | ' || (SELECT string_agg(id::VARCHAR, ',' ORDER BY id) || ' to ' || max(opened)::VARCHAR FROM raw.erp__days)";

const FIRST: &str = "1,2 to 2026-01-31 23:59:59.997 | 1,2 to 2026-01-31";

const SECOND: &str = "1,2,3,4 to 2026-02-01 00:00:00.000 | 1,2,3,4 to 2026-02-01";

const LEGACY_FIRST: &str = "1,2 to 2026-01-05 23:59:59.123 | 1,2 to 2026-01-31";

#[test]
fn given_each_odbc_driver_when_a_millisecond_timestamp_cursor_and_a_date_cursor_resume_then_the_next_load_reads_exactly_the_rows_after_each_boundary()
-> Result<(), String> {
    let test_cases = [
        OdbcDriverTestCase {
            description: "SQL Server through Microsoft's ODBC Driver 18: millisecond and date cursors on datetime2(3) and date",
            connection_options: "",
            source_settings: "",
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
            description: "SQL Server's legacy datetime, whose 1/300 s ticks the driver reads rounded to milliseconds (.123 is .12333): a datetime2 bound would read the .123 row again, so it is bound as text in the column's own type, in ISO 8601 form, which a british session (day before month) reads alike: 2026-01-05 with a space would be 1 May",
            connection_options: "Language=British;",
            source_settings: "",
            database: OdbcDatabase::SqlServer,
            tables: ("erp.events", "erp.days"),
            setup: &[
                "CREATE SCHEMA erp",
                "CREATE TABLE erp.events (id int PRIMARY KEY, name nvarchar(40), happened_at datetime NOT NULL)",
                "CREATE TABLE erp.days (id int PRIMARY KEY, opened date NOT NULL)",
                "INSERT INTO erp.events VALUES (1, 'a', '2026-01-05 10:00:00'), (2, 'b', '2026-01-05 23:59:59.123')",
                "INSERT INTO erp.days VALUES (1, '2026-01-30'), (2, '2026-01-31')",
            ],
            change: &[
                "INSERT INTO erp.events VALUES (0, 'early', '2026-01-05 23:59:59.120'), (3, 'next', '2026-01-05 23:59:59.127'), (4, 'later', '2026-02-01 00:00:00')",
                "INSERT INTO erp.days VALUES (3, '2026-01-31'), (4, '2026-02-01')",
            ],
            expected_tables: (LEGACY_FIRST, SECOND),
        },
        OdbcDriverTestCase {
            description: "MySQL through Connector/ODBC, a driver without schemas, in an ANSI_QUOTES session: millisecond and date cursors on DATETIME(3) and DATE",
            connection_options: "",
            source_settings: "",
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
            description: "Postgres through psqlODBC: millisecond and date cursors on timestamp(3) and date",
            connection_options: "",
            source_settings: "",
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
        OdbcDriverTestCase {
            description: "MySQL through Connector/ODBC under cursor_bounds = \"literals\": {ts} and {d} escapes resume exactly from DATETIME(3), which the driver describes without fractional digits, so the escape keeps the value's own",
            connection_options: "",
            source_settings: "cursor_bounds = \"literals\"",
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
            description: "Postgres through psqlODBC under cursor_bounds = \"literals\": {ts} escapes at the column's three digits and {d} escapes resume exactly",
            connection_options: "",
            source_settings: "cursor_bounds = \"literals\"",
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
                driver_project(
                    (
                        &format!(
                            "{}{}",
                            server.connection_string, test_case.connection_options
                        ),
                        test_case.source_settings,
                    ),
                    test_case.tables,
                    &database,
                ),
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

#[test]
fn given_each_odbc_driver_when_tables_merge_on_a_primary_key_it_does_not_report_then_plan_refuses_them_and_names_the_keys_the_database_lists()
-> Result<(), String> {
    let test_cases = [
        OdbcKeylessTestCase {
            description: "SQL Server through Microsoft's ODBC Driver 18: a table without a key, and one with only a unique constraint on a NOT NULL column, suggested as its unique_key",
            database: OdbcDatabase::SqlServer,
            driver: ("", ""),
            tables: ("erp.logs", "erp.codes"),
            setup: &[
                "CREATE SCHEMA erp",
                "CREATE TABLE erp.logs (id int, at datetime2(3) NOT NULL)",
                "CREATE TABLE erp.codes (code varchar(10) NOT NULL UNIQUE, at datetime2(3) NOT NULL)",
            ],
            expected_reasons: &[
                "erp.logs (kind = \"odbc\"): unique_key = \"{primary_key}\", but the source reports no primary key for the table\n",
                "erp.codes (kind = \"odbc\"): unique_key = \"{primary_key}\", but the source reports no primary key for the table. The ODBC driver reported no primary key, but the database's INFORMATION_SCHEMA lists UNIQUE (code)\n",
                "fix     sqlload.toml:26 name the key columns, such as unique_key = [\"code\"]",
            ],
            absent: &[],
        },
        OdbcKeylessTestCase {
            description: "Postgres through psqlODBC: a table without a key, and one with only a unique constraint on a nullable column, named but not suggested, as a merge would insert its NULL rows again",
            database: OdbcDatabase::Postgres,
            driver: ("", ""),
            tables: ("erp.logs", "erp.codes"),
            setup: &[
                "CREATE SCHEMA erp",
                "CREATE TABLE erp.logs (id int, at timestamp(3) NOT NULL)",
                "CREATE TABLE erp.codes (code varchar(10) UNIQUE, at timestamp(3) NOT NULL)",
            ],
            expected_reasons: &[
                "erp.logs (kind = \"odbc\"): unique_key = \"{primary_key}\", but the source reports no primary key for the table\n",
                "erp.codes (kind = \"odbc\"): unique_key = \"{primary_key}\", but the source reports no primary key for the table. The ODBC driver reported no primary key, but the database's INFORMATION_SCHEMA lists UNIQUE (code)\n",
            ],
            absent: &["unique_key = [\"code\"]"],
        },
        OdbcKeylessTestCase {
            description: "MySQL through Connector/ODBC, which reports a unique index as the primary key, so only the table without a key is refused",
            database: OdbcDatabase::MySql,
            driver: ("", ""),
            tables: ("logs", "codes"),
            setup: &[
                "CREATE TABLE logs (id int, at DATETIME(3) NOT NULL)",
                "CREATE TABLE codes (code varchar(10) UNIQUE, at DATETIME(3) NOT NULL)",
            ],
            expected_reasons: &[
                "logs (kind = \"odbc\"): unique_key = \"{primary_key}\", but the source reports no primary key for the table\n",
                "Errors (1)",
            ],
            absent: &[],
        },
        OdbcKeylessTestCase {
            description: "MySQL 8.4 through MariaDB Connector/ODBC 3.1, which reports no primary key from MySQL 8.4 (it compares COLUMN_KEY with 'pri'): its key, which INFORMATION_SCHEMA lists under the database as the schema, is named instead of the table merging on no key",
            database: OdbcDatabase::MySql,
            driver: ("MySQL ODBC Unicode Driver", "MariaDB Unicode"),
            tables: ("logs", "items"),
            setup: &[
                "CREATE TABLE logs (id int, at DATETIME(3) NOT NULL)",
                "CREATE TABLE items (id int PRIMARY KEY, at DATETIME(3) NOT NULL)",
            ],
            expected_reasons: &[
                "logs (kind = \"odbc\"): unique_key = \"{primary_key}\", but the source reports no primary key for the table\n",
                "items (kind = \"odbc\"): unique_key = \"{primary_key}\", but the source reports no primary key for the table. The ODBC driver reported no primary key, but the database's INFORMATION_SCHEMA lists PRIMARY KEY (id), so the driver's key lookup is wrong (MariaDB Connector/ODBC 3.1 finds none in MySQL 8.4)\n",
                "fix     sqlload.toml:26 name the key columns, such as unique_key = [\"id\"]",
            ],
            absent: &[],
        },
    ];
    for test_case in test_cases {
        run_async(async {
            let server = start_odbc_server(test_case.database).await?;
            odbc_execute(&server.connection_string, test_case.setup)?;
            let (tested, other) = test_case.driver;
            let connection_string = server.connection_string.replace(tested, other);
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let database = folder.path().join("out.duckdb");
            let config = folder.path().join("sqlload.toml");
            std::fs::write(
                &config,
                keyless_project(&connection_string, test_case.tables, &database),
            )
            .map_err(|error| error.to_string())?;
            let (succeeded, output) = run_sqlload(&config, &["plan"], ("", ""))?;
            assert!(
                !succeeded
                    && test_case
                        .expected_reasons
                        .iter()
                        .all(|reason| words(&output).contains(&words(reason)))
                    && !test_case
                        .absent
                        .iter()
                        .any(|text| words(&output).contains(&words(text))),
                "{}:\n{output}",
                test_case.description
            );
            Ok(())
        })?;
    }
    Ok(())
}
