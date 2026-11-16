use sqlload_testkit::odbc_servers::main::start_odbc_server::start_odbc_server;
use sqlload_testkit::odbc_servers::models::OdbcDatabase;

use crate::helpers::{run_async, words};
use crate::odbc_helpers::{catalog_project, odbc_execute, run_traced};
use crate::test_types::OdbcCatalogTestCase;

const POSTGRES_TABLES: &[&str] = &[
    "CREATE SCHEMA erp",
    "CREATE SCHEMA other",
    "CREATE TABLE erp.items (id int PRIMARY KEY, name varchar(40), at timestamp(3) NOT NULL)",
    "CREATE TABLE erp.events (id int, name varchar(40), at timestamp(3) NOT NULL)",
    "CREATE TABLE erp.notes (id int PRIMARY KEY, at timestamp(3) NOT NULL)",
    "CREATE TABLE erp.ignored (id int)",
    "CREATE TABLE other.items (id int PRIMARY KEY)",
];

const MYSQL_TABLES: &[&str] = &[
    "CREATE TABLE items (id int PRIMARY KEY, name varchar(40), at DATETIME(3) NOT NULL)",
    "CREATE TABLE logs (id int, at DATETIME(3) NOT NULL)",
    "CREATE TABLE events (id int, name varchar(40), at DATETIME(3) NOT NULL)",
];

#[cfg(target_os = "linux")]
#[test]
fn given_each_odbc_driver_when_planning_included_tables_then_the_driver_is_asked_for_them_alone_and_for_keys_only_where_a_table_uses_them()
-> Result<(), String> {
    let test_cases = [
        OdbcCatalogTestCase {
            description: "psqlODBC, replace tables named exactly: one lookup each, no primary key read, and no INFORMATION_SCHEMA query for the table without a key",
            database: OdbcDatabase::Postgres,
            driver: ("", ""),
            setup: POSTGRES_TABLES,
            source: r#"include = ["erp.items", "erp.events"]
write_strategy = "replace"

[sources.erp.tables."erp.items"]
description = "Items."

[sources.erp.tables."erp.events"]
description = "Events, without a key."
"#,
            expected_calls: &["SQLTables(erp, items)", "SQLTables(erp, events)"],
            expected_output: (
                true,
                &[
                    "raw.erp__events replace",
                    "raw.erp__items replace",
                    "2 selected",
                ],
            ),
            absent: &[],
        },
        OdbcCatalogTestCase {
            description: "psqlODBC: a merge on \"{primary_key}\" reads its key, a merge on its own columns reads it for plan's note, and a replace table reads none",
            database: OdbcDatabase::Postgres,
            driver: ("", ""),
            setup: POSTGRES_TABLES,
            source: r#"include = ["erp.items", "erp.events", "erp.notes"]
write_strategy = "replace"

[sources.erp.tables."erp.items"]
description = "Items, merged on their primary key."
write_strategy = "merge"
unique_key = "{primary_key}"
cursor_column = "at"

[sources.erp.tables."erp.events"]
description = "Events."

[sources.erp.tables."erp.notes"]
description = "Notes, merged on id."
write_strategy = "merge"
unique_key = ["id"]
cursor_column = "at"
"#,
            expected_calls: &[
                "SQLTables(erp, items)",
                "SQLTables(erp, events)",
                "SQLTables(erp, notes)",
                "SQLPrimaryKeys(erp, items)",
                "SQLPrimaryKeys(erp, notes)",
            ],
            expected_output: (
                true,
                &[
                    "raw.erp__events replace",
                    "raw.erp__items merge (unique_key: id ← {primary_key})",
                    "raw.erp__notes merge (unique_key: id)",
                ],
            ),
            absent: &[],
        },
        OdbcCatalogTestCase {
            description: "psqlODBC, one table merging on its own columns: its key is read, as plan says the source reports the same one",
            database: OdbcDatabase::Postgres,
            driver: ("", ""),
            setup: POSTGRES_TABLES,
            source: r#"include = ["erp.notes"]

[sources.erp.tables."erp.notes"]
description = "Notes, merged on id."
write_strategy = "merge"
unique_key = ["id"]
cursor_column = "at"
"#,
            expected_calls: &["SQLTables(erp, notes)", "SQLPrimaryKeys(erp, notes)"],
            expected_output: (
                true,
                &[
                    "raw.erp__notes merge (unique_key: id)",
                    "the source reports the same primary key",
                ],
            ),
            absent: &[],
        },
        OdbcCatalogTestCase {
            description: "psqlODBC, which compares names case-sensitively: include in another case finds nothing by its lookup, so the whole catalog is listed once and the table found, ignoring case, as before",
            database: OdbcDatabase::Postgres,
            driver: ("", ""),
            setup: POSTGRES_TABLES,
            source: r#"include = ["ERP.Items"]
write_strategy = "replace"
"#,
            expected_calls: &["SQLTables(ERP, Items)", "SQLTables(%, %)"],
            expected_output: (true, &["raw.erp__items replace", "1 selected"]),
            absent: &[],
        },
        OdbcCatalogTestCase {
            description: "psqlODBC, a schema with a table pattern and an exclude: one lookup of the schema's tables, and the same tables as before",
            database: OdbcDatabase::Postgres,
            driver: ("", ""),
            setup: POSTGRES_TABLES,
            source: r#"include = ["erp.*"]
exclude = ["erp.ignored"]
write_strategy = "replace"
"#,
            expected_calls: &["SQLTables(erp, %)"],
            expected_output: (
                true,
                &[
                    "raw.erp__events replace",
                    "raw.erp__items replace",
                    "raw.erp__notes replace",
                    "3 selected",
                ],
            ),
            absent: &["erp__ignored", "other__items"],
        },
        OdbcCatalogTestCase {
            description: "psqlODBC, a table that doesn't exist: its lookup finds nothing, the whole catalog is listed once, and plan fails as before",
            database: OdbcDatabase::Postgres,
            driver: ("", ""),
            setup: POSTGRES_TABLES,
            source: r#"include = ["erp.items", "erp.missing"]
write_strategy = "replace"

[sources.erp.tables."erp.items"]
description = "Items."

[sources.erp.tables."erp.missing"]
description = "A table the database doesn't have."
"#,
            expected_calls: &[
                "SQLTables(erp, items)",
                "SQLTables(erp, missing)",
                "SQLTables(%, %)",
            ],
            expected_output: (
                false,
                &[
                    "erp.missing",
                    "listed under [sources.erp.tables] but not found in the source (or not selected by include/exclude)",
                ],
            ),
            absent: &[],
        },
        OdbcCatalogTestCase {
            description: "Microsoft's ODBC Driver 18, whose SQL Server compares names ignoring case: include in another case is found by its own lookup",
            database: OdbcDatabase::SqlServer,
            driver: ("", ""),
            setup: &[
                "CREATE SCHEMA erp",
                "CREATE TABLE erp.items (id int PRIMARY KEY, name nvarchar(40), at datetime2(3) NOT NULL)",
                "CREATE TABLE erp.events (id int, name nvarchar(40), at datetime2(3) NOT NULL)",
            ],
            source: r#"include = ["erp.items", "ERP.EVENTS"]
write_strategy = "replace"

[sources.erp.tables."erp.items"]
description = "Items, merged on their primary key."
write_strategy = "merge"
unique_key = "{primary_key}"
cursor_column = "at"
"#,
            expected_calls: &[
                "SQLTables(erp, items)",
                "SQLTables(ERP, EVENTS)",
                "SQLPrimaryKeys(erp, items)",
            ],
            expected_output: (
                true,
                &[
                    "raw.erp__events replace",
                    "raw.erp__items merge (unique_key: id ← {primary_key})",
                ],
            ),
            absent: &[],
        },
        OdbcCatalogTestCase {
            description: "MySQL Connector/ODBC, a driver without schemas that refuses a schema pattern: asked again without one, then without one for every later lookup; a name in another case finds nothing (MySQL on Linux compares table names case-sensitively), so the whole catalog is listed once",
            database: OdbcDatabase::MySql,
            driver: ("", ""),
            setup: MYSQL_TABLES,
            source: r#"include = ["items", "EVENTS"]
write_strategy = "replace"

[sources.erp.tables.items]
description = "Items, merged on their primary key."
write_strategy = "merge"
unique_key = "{primary_key}"
cursor_column = "at"
"#,
            expected_calls: &[
                "SQLTables(%, items) refused",
                "SQLTables(NULL, items)",
                "SQLTables(NULL, EVENTS)",
                "SQLTables(%, %) refused",
                "SQLTables(NULL, %)",
                "SQLPrimaryKeys(NULL, items)",
            ],
            expected_output: (
                true,
                &[
                    "raw.erp__events replace",
                    "raw.erp__items merge (unique_key: id ← {primary_key})",
                ],
            ),
            absent: &[],
        },
        OdbcCatalogTestCase {
            description: "MySQL 8.4 through MariaDB Connector/ODBC 3.1, which takes a schema pattern and reports no primary key there: INFORMATION_SCHEMA is asked only for the tables merging on \"{primary_key}\", whose error it explains",
            database: OdbcDatabase::MySql,
            driver: ("MySQL ODBC Unicode Driver", "MariaDB Unicode"),
            setup: MYSQL_TABLES,
            source: r#"include = ["items", "logs", "events"]
write_strategy = "merge"
unique_key = "{primary_key}"

[sources.erp.tables.items]
description = "Items, merged on their primary key."
cursor_column = "at"

[sources.erp.tables.logs]
description = "Logs, without a key."
cursor_column = "at"

[sources.erp.tables.events]
description = "Events, replaced."
write_strategy = "replace"
"#,
            expected_calls: &[
                "SQLTables(%, items)",
                "SQLTables(%, logs)",
                "SQLTables(%, events)",
                "SQLPrimaryKeys(NULL, items)",
                "INFORMATION_SCHEMA",
                "SQLPrimaryKeys(NULL, logs)",
                "INFORMATION_SCHEMA",
            ],
            expected_output: (
                false,
                &[
                    "items (kind = \"odbc\"): unique_key = \"{primary_key}\", but the source reports no primary key for the table. The ODBC driver reported no primary key, but the database's INFORMATION_SCHEMA lists PRIMARY KEY (id)",
                    "logs (kind = \"odbc\"): unique_key = \"{primary_key}\", but the source reports no primary key for the table\n",
                    "raw.erp__events replace",
                ],
            ),
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
                catalog_project(&connection_string, test_case.source, &database),
            )
            .map_err(|error| error.to_string())?;
            let (succeeded, output, calls) = run_traced(&config, &["plan"])?;
            let (expected_success, expected_texts) = test_case.expected_output;
            assert_eq!(
                calls, test_case.expected_calls,
                "{}: catalog calls\n{output}",
                test_case.description
            );
            assert!(
                succeeded == expected_success
                    && expected_texts
                        .iter()
                        .all(|text| words(&output).contains(&words(text)))
                    && !test_case.absent.iter().any(|text| output.contains(text)),
                "{}:\n{output}",
                test_case.description
            );
            Ok(())
        })?;
    }
    Ok(())
}
