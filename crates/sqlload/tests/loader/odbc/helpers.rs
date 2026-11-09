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
