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
