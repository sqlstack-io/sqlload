//! Helpers for the DuckDB source loader test.

/// The project: a duckdb source over `source` with include and exclude patterns, an excluded column, its orders merged after a cursor with `where` and hard deletes, a query table, and DuckDB file `target` as the target.
pub(crate) fn duckdb_source_project(source: &std::path::Path, target: &std::path::Path) -> String {
    format!(
        r#"default_target = "local"

[connections.shop_file]
kind = "duckdb"
database = "{source}"

[connections.warehouse]
kind = "duckdb"
database = "{target}"

[targets.local]
connection = "warehouse"
loader_schema = "raw"

[sources.shop]
kind = "duckdb"
connection = "shop_file"
description = "The shop's DuckDB file."
include = ["sales.*"]
exclude = ["sales.tmp_*"]
exclude_columns = ["card"]
write_strategy = "replace"

[sources.shop.tables."sales.orders"]
description = "Orders, one row each."
write_strategy = "merge"
unique_key = ["id"]
cursor_column = "updated_at"
where = "region = 'EU'"
deletes = "hard"
checks = {{ max_row_drop = "100%" }}

[sources.shop.tables.big_query]
description = "Orders over 25, by query."
sql = "SELECT id, amount FROM sales.orders WHERE amount > 25 -- big ones"
"#,
        source = source.display(),
        target = target.display()
    )
}
