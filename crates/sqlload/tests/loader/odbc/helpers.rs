//! Helpers for the ODBC loader test.

/// The project: an odbc source over psqlODBC, its items table merged on a cursor, a query table, and DuckDB as the target.
pub(crate) fn project(port: u16, database: &std::path::Path, max_text_size: usize) -> String {
    format!(
        r#"
[connections.erp]
kind = "odbc"
connection_string = "Driver={{PostgreSQL Unicode}};Server=127.0.0.1;Port={port};Database=app;Uid=sqlload;Pwd=sqlload;BoolsAsChar=0;"

[connections.local]
kind = "duckdb"
database = "{database}"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Tables of a generic ERP."
include = ["erp.*"]
exclude = ["erp.ignored"]
max_text_size = {max_text_size}

[sources.erp.tables."erp.items"]
description = "Items, changed rows found by updated_at."
cursor_column = "updated_at"
unique_key = ["id"]
write_strategy = "merge"

[sources.erp.tables.recent]
description = "The newest items."
sql = "SELECT id, name FROM erp.items WHERE id > 2990 -- newest"
"#,
        database = database.display(),
    )
}

/// A project reading `erp.notes` and `erp.codes` over psqlODBC, each appended by its text `label` cursor, into DuckDB.
pub(crate) fn text_cursor_project(port: u16, database: &std::path::Path) -> String {
    format!(
        r#"
[connections.erp]
kind = "odbc"
connection_string = "Driver={{PostgreSQL Unicode}};Server=127.0.0.1;Port={port};Database=app;Uid=sqlload;Pwd=sqlload;"

[connections.local]
kind = "duckdb"
database = "{database}"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Labelled rows of a generic ERP."
include = ["erp.*"]

[sources.erp.tables."erp.notes"]
description = "Notes; the first maximum ends in a backslash."
cursor_column = "label"
write_strategy = "append"

[sources.erp.tables."erp.codes"]
description = "Codes; the first maximum holds a backslash before a quote."
cursor_column = "label"
write_strategy = "append"
"#,
        database = database.display(),
    )
}

/// Run each of `statements` through ODBC, one at a time.
pub(crate) fn odbc_execute(connection_string: &str, statements: &[&str]) -> Result<(), String> {
    let connection = arrow_odbc::odbc_api::environment()
        .map_err(|error| error.to_string())?
        .connect_with_connection_string(
            connection_string,
            arrow_odbc::odbc_api::ConnectionOptions::default(),
        )
        .map_err(|error| format!("cannot connect: {error}"))?;
    statements.iter().try_for_each(|statement| {
        connection
            .execute(statement, (), None)
            .map(|_| ())
            .map_err(|error| format!("{statement}: {error}"))
    })
}

/// A project reading an events table appended by its millisecond `happened_at` cursor and a days table replaced from its last `opened` day, through `connection_string`, into DuckDB.
pub(crate) fn driver_project(
    connection_string: &str,
    (events, days): (&str, &str),
    database: &std::path::Path,
) -> String {
    format!(
        r#"
[connections.erp]
kind = "odbc"
connection_string = "{connection_string}"

[connections.local]
kind = "duckdb"
database = '{database}'

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Events and days of a generic ERP."
include = ["{events}", "{days}"]

[sources.erp.tables."{events}"]
description = "Events, read after the newest happened_at."
cursor_column = "happened_at"
write_strategy = "append"

[sources.erp.tables."{days}"]
description = "Days, the last one read again."
cursor_column = "opened"
write_strategy = "delete_insert"
"#,
        database = database.display(),
    )
}
