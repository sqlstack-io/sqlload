use sqlload_testkit::odbc_servers::main::start_odbc_server::start_odbc_server;
use sqlload_testkit::odbc_servers::models::OdbcDatabase;

use crate::helpers::{duckdb_text, run_async, run_sqlload};
use crate::odbc_helpers::{memory_project, odbc_execute, typed_rows};
use crate::test_types::{OdbcLargeValuesTestCase, OdbcValueReadTestCase, OdbcWideTableTestCase};

/// The log filter that shows each fetch read value by value.
const FETCH_LOG: &str = "info,sqlload::odbc_source=debug,odbc_api=error,arrow_odbc=warn";

/// `typed_rows` of the table every driver loads: its columns in the source's order, a 100,000-character value whole, and NULLs.
const TYPED: &str = "id,body,title,price,at,day,n | 1:100000:xxx:t1:12.50:2026-01-31 10:00:00.123:2026-01-31:9000000000 ; 2:7:sho:t2:0.10:2026-02-01 00:00:00.000:2026-02-01:-1 ; 3:NULL:NULL:NULL:NULL:NULL:NULL:NULL";

#[test]
fn given_each_odbc_driver_when_a_table_has_an_unbounded_text_column_then_every_value_is_read_value_by_value_in_the_columns_order()
-> Result<(), String> {
    let test_cases = [
        OdbcValueReadTestCase {
            description: "MySQL through Connector/ODBC: LONGTEXT",
            database: OdbcDatabase::MySql,
            driver: ("", ""),
            table: ("typed", "erp__typed"),
            setup: &[
                "CREATE TABLE typed (id int PRIMARY KEY, body LONGTEXT, title varchar(20), price decimal(10,2), at DATETIME(3), day DATE, n bigint)",
                "INSERT INTO typed VALUES (1, REPEAT('x', 100000), 't1', 12.5, '2026-01-31 10:00:00.123', '2026-01-31', 9000000000), (2, 'short 2', 't2', 0.1, '2026-02-01 00:00:00', '2026-02-01', -1), (3, NULL, NULL, NULL, NULL, NULL, NULL)",
            ],
            expected_rows: TYPED,
        },
        OdbcValueReadTestCase {
            description: "MySQL through MariaDB Connector/ODBC 3.1: LONGTEXT",
            database: OdbcDatabase::MySql,
            driver: ("MySQL ODBC Unicode Driver", "MariaDB Unicode"),
            table: ("typed", "erp__typed"),
            setup: &[
                "CREATE TABLE typed (id int PRIMARY KEY, body LONGTEXT, title varchar(20), price decimal(10,2), at DATETIME(3), day DATE, n bigint)",
                "INSERT INTO typed VALUES (1, REPEAT('x', 100000), 't1', 12.5, '2026-01-31 10:00:00.123', '2026-01-31', 9000000000), (2, 'short 2', 't2', 0.1, '2026-02-01 00:00:00', '2026-02-01', -1), (3, NULL, NULL, NULL, NULL, NULL, NULL)",
            ],
            expected_rows: TYPED,
        },
        OdbcValueReadTestCase {
            description: "Postgres through psqlODBC: text, which it describes as 8,190 characters, holding a value of 100,000",
            database: OdbcDatabase::Postgres,
            driver: ("", ""),
            table: ("erp.typed", "erp__typed"),
            setup: &[
                "CREATE SCHEMA erp",
                "CREATE TABLE erp.typed (id int PRIMARY KEY, body text, title varchar(20), price decimal(10,2), at timestamp(3), day date, n bigint)",
                "INSERT INTO erp.typed VALUES (1, repeat('x', 100000), 't1', 12.5, '2026-01-31 10:00:00.123', '2026-01-31', 9000000000), (2, 'short 2', 't2', 0.1, '2026-02-01 00:00:00', '2026-02-01', -1), (3, NULL, NULL, NULL, NULL, NULL, NULL)",
            ],
            expected_rows: TYPED,
        },
        OdbcValueReadTestCase {
            description: "SQL Server through Microsoft's ODBC Driver 18, which can't mix bound columns with SQLGetData on a block cursor: varchar(max)",
            database: OdbcDatabase::SqlServer,
            driver: ("", ""),
            table: ("erp.typed", "erp__typed"),
            setup: &[
                "CREATE SCHEMA erp",
                "CREATE TABLE erp.typed (id int PRIMARY KEY, body varchar(max), title varchar(20), price decimal(10,2), at datetime2(3), day date, n bigint)",
                "INSERT INTO erp.typed VALUES (1, REPLICATE(CAST('x' AS varchar(max)), 100000), 't1', 12.5, '2026-01-31 10:00:00.123', '2026-01-31', 9000000000), (2, 'short 2', 't2', 0.1, '2026-02-01 00:00:00', '2026-02-01', -1), (3, NULL, NULL, NULL, NULL, NULL, NULL)",
            ],
            expected_rows: TYPED,
        },
    ];
    for test_case in test_cases {
        run_async(|| async move {
            let server = start_odbc_server(test_case.database).await?;
            odbc_execute(&server.connection_string, test_case.setup)?;
            let (tested, other) = test_case.driver;
            let connection_string = server.connection_string.replace(tested, other);
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let database = folder.path().join("out.duckdb");
            let config = folder.path().join("sqlload.toml");
            std::fs::write(
                &config,
                memory_project((&connection_string, ""), &[test_case.table.0], &database),
            )
            .map_err(|error| error.to_string())?;
            let (succeeded, loaded) = run_sqlload(&config, &["load", "--yes"], ("", ""))?;
            let (_, planned) = run_sqlload(&config, &["plan"], ("", ""))?;
            let output = format!("{planned}\n{loaded}");
            assert!(
                succeeded
                    && output
                        .contains("1 long column (body), so all 7 columns are read value by value"),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(&database, &typed_rows(test_case.table.1))?,
                test_case.expected_rows,
                "{}",
                test_case.description
            );
            Ok(())
        })?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn given_a_mysql_table_of_265_text_columns_when_loading_then_it_reads_value_by_value_within_the_budget_and_a_narrow_table_keeps_its_fetch()
-> Result<(), String> {
    use crate::odbc_helpers::run_measured;
    let test_cases = [OdbcWideTableTestCase {
        description: "265 TEXT columns of short values, which MySQL describes as 65,535 bytes each: before, the bulk reader bound 512 MiB of buffers for them; a narrow table beside it keeps 65,536 rows a fetch",
        wide: (265, 5_000),
        second_settings: "max_batch_memory = \"1000GB\"",
        expected_lines: &[
            "fetch   up to ",
            " or 65,536 rows per fetch; 265 long columns (c001, c002, c003, …), so all 266 columns are read value by value",
            " per fetch, 65,536 rows\n",
            "2 loaded",
        ],
        expected_warning: "lower max_batch_memory on source erp, or lower concurrency (now 4)",
        expected_max_peak_kib: 384 * 1024,
        expected_wide: "5000 | v265-4321 | id,c001,c002,c264,c265",
    }];
    for test_case in test_cases {
        run_async(|| async move {
            let server = start_odbc_server(OdbcDatabase::MySql).await?;
            let (count, rows) = test_case.wide;
            let columns: String = (1..=count)
                .map(|number| format!(", c{number:03} TEXT"))
                .collect();
            let values: String = (1..=count)
                .map(|number| format!(", CONCAT('v{number:03}-', n)"))
                .collect();
            let create = format!("CREATE TABLE wide (id int PRIMARY KEY{columns})");
            let insert = format!(
                "INSERT INTO wide WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < {rows}) SELECT n{values} FROM seq"
            );
            let root = server.connection_string.replace("Uid=sqlload", "Uid=root");
            odbc_execute(
                &root,
                &[
                    "SET SESSION innodb_strict_mode = OFF",
                    "SET SESSION cte_max_recursion_depth = 10000",
                    &create,
                    &insert,
                    "CREATE TABLE narrow (id int PRIMARY KEY, name varchar(40), price decimal(10,2))",
                    "INSERT INTO narrow VALUES (1, 'a', 1.5), (2, 'b', 2.5)",
                ],
            )?;
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let database = folder.path().join("out.duckdb");
            let config = folder.path().join("sqlload.toml");
            std::fs::write(
                &config,
                memory_project(
                    (&server.connection_string, ""),
                    &["wide", "narrow"],
                    &database,
                ),
            )
            .map_err(|error| error.to_string())?;
            let (succeeded, loaded, peak) = run_measured(&config, &["load", "--yes"], "info")?;
            let (_, planned) = run_sqlload(&config, &["plan"], ("", ""))?;
            let output = format!("{planned}\n{loaded}");
            assert!(
                succeeded
                    && test_case
                        .expected_lines
                        .iter()
                        .all(|line| output.contains(line)),
                "{}:\n{output}",
                test_case.description
            );
            assert!(
                peak < test_case.expected_max_peak_kib,
                "{}: the load peaked at {peak} KiB:\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(
                    &database,
                    "SELECT (SELECT count(*) FROM raw.erp__wide)::VARCHAR || ' | ' || (SELECT c265 FROM raw.erp__wide WHERE id = 4321) || ' | ' || (SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'erp__wide' AND ordinal_position IN (1, 2, 3, 265, 266))"
                )?,
                test_case.expected_wide,
                "{}: every row loads, each value whole, the columns in the source's order",
                test_case.description
            );
            std::fs::write(
                &config,
                memory_project(
                    (&server.connection_string, test_case.second_settings),
                    &["wide", "narrow"],
                    &database,
                ),
            )
            .map_err(|error| error.to_string())?;
            let (succeeded, output) = run_sqlload(&config, &["load", "--yes"], ("", ""))?;
            assert!(
                succeeded && output.contains(test_case.expected_warning),
                "{}: load repeats plan's check, warning that the fetches near the machine's memory:\n{output}",
                test_case.description
            );
            Ok(())
        })?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn given_a_mysql_table_with_a_few_very_large_text_values_when_loading_then_fetches_end_at_the_budget_and_a_value_larger_than_it_loads_and_is_logged()
-> Result<(), String> {
    use crate::odbc_helpers::run_measured;
    let test_cases = [OdbcLargeValuesTestCase {
        description: "200 rows fit one fetch of 65,536, but the fifth 2 MB value (row 100) passes the 8 MiB budget and the 24 MB one (row 150) passes it alone, each ending its fetch",
        setup: &[
            "CREATE TABLE notes (id int PRIMARY KEY, title varchar(20), body LONGTEXT)",
            "INSERT INTO notes WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 200) SELECT n, CONCAT('t', n), CONCAT('short ', n) FROM seq",
            "UPDATE notes SET body = REPEAT('y', 2000000) WHERE id IN (20, 40, 60, 80, 100, 120)",
            "UPDATE notes SET body = REPEAT('z', 24000000) WHERE id = 150",
        ],
        source_settings: "max_batch_memory = \"8MiB\"",
        expected_fetch_rows: &[100, 50, 50],
        expected_max_fetch_bytes: 8 * 1024 * 1024 + 24_000_000,
        expected_log: &[
            "a value is larger than the table's fetch buffer (max_batch_memory); it loads, and its fetch ends after its row",
            "column=body bytes=24000000 budget=8388608",
        ],
        expected_values: "200:24000000:6",
    }];
    for test_case in test_cases {
        run_async(|| async move {
            let server = start_odbc_server(OdbcDatabase::MySql).await?;
            odbc_execute(&server.connection_string, test_case.setup)?;
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let database = folder.path().join("out.duckdb");
            let config = folder.path().join("sqlload.toml");
            std::fs::write(
                &config,
                memory_project(
                    (&server.connection_string, test_case.source_settings),
                    &["notes"],
                    &database,
                ),
            )
            .map_err(|error| error.to_string())?;
            let (succeeded, output, _) = run_measured(&config, &["load", "--yes"], FETCH_LOG)?;
            let fetches: Vec<(usize, u64)> = output
                .lines()
                .filter(|line| line.contains("fetched rows value by value"))
                .filter_map(|line| {
                    let field = |name: &str| {
                        line.split_whitespace()
                            .find_map(|field| field.strip_prefix(name))
                            .and_then(|value| value.parse::<u64>().ok())
                    };
                    Some((usize::try_from(field("rows=")?).ok()?, field("bytes=")?))
                })
                .collect();
            let rows: Vec<usize> = fetches.iter().map(|(rows, _)| *rows).collect();
            assert!(
                succeeded
                    && rows == test_case.expected_fetch_rows
                    && fetches
                        .iter()
                        .all(|&(_, bytes)| bytes < test_case.expected_max_fetch_bytes),
                "{}: {fetches:?}\n{output}",
                test_case.description
            );
            assert!(
                test_case
                    .expected_log
                    .iter()
                    .all(|line| output.contains(line)),
                "{}: the 24 MB value is logged:\n{output}",
                test_case.description
            );
            assert_eq!(
                duckdb_text(
                    &database,
                    "SELECT count(*)::VARCHAR || ':' || max(length(body))::VARCHAR || ':' || (count(*) FILTER (WHERE length(body) = 2000000))::VARCHAR FROM raw.erp__notes"
                )?,
                test_case.expected_values,
                "{}: every value loads whole",
                test_case.description
            );
            Ok(())
        })?;
    }
    Ok(())
}
