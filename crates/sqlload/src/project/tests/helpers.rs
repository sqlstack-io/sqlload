/// A project using every section, with `${...}` references.
pub(crate) const PROJECT: &str = r#"
default_target = "dev"

[connections.warehouse]
kind = "snowflake"
account = "acct"
user = "LOADER"
auth = { method = "key_pair", private_key_file = "${KEY_FILE}" }
database = "ANALYTICS"

[connections.app_pg]
kind = "postgres"
host = "localhost"
user = "reader"
password = "${PG_PASSWORD}"
database = "app"

[targets.dev]
connection = "warehouse"
loader_schema = "DEV_SQLLOAD"

[sources.app]
kind = "postgres"
connection = "app_pg"
include = ["public.*"]
exclude = ["public.audit_*"]
new_tables = "skip"
exclude_columns = ["*email*"]
write_strategy = "merge"
unique_key = "id"
cursor_column = "updated_at"
lookback = "1h"

[sources.app.tables."public.accounts"]
columns = { balance = "decimal(18,2)" }
type_change_rebuild = "allow"

[sources.app.tables."public.events"]
write_strategy = "append"
unique_key = ["event_id", "line_no"]
"#;

/// Connections and a target that the error cases' sources can refer to.
pub(crate) const CONNECTIONS: &str = "[connections.wh]\nkind = \"duckdb\"\npath = \":memory:\"\n[connections.pg]\nkind = \"postgres\"\nhost = \"h\"\nuser = \"u\"\ndatabase = \"d\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n";

/// Every `${NAME}` resolves to `<NAME>`.
pub(crate) fn environment(name: &str) -> Option<String> {
    Some(format!("<{name}>"))
}
