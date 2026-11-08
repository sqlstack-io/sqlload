/// A project using every section, with `${...}` references.
pub(crate) const PROJECT: &str = r#"
default_target = "dev"

[connections.warehouse]
kind = "snowflake"
account = "acct"
user = "LOADER"
auth = { method = "key_pair", private_key_file = "${ENV:KEY_FILE}" }
database = "ANALYTICS"
load_method = { type = "internal_user_stage" }

[connections.app_pg]
kind = "postgres"
host = "localhost"
user = "reader"
password = "${ENV:PG_PASSWORD}"
database = "app"

[connections.bq]
kind = "bigquery"
location = "europe-west2"
service_account_key = "${ENV:SA_KEY}"

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
checks = { unique = ["id"], not_null = ["id"], max_row_drop = "20%", warn = ["max_row_drop"] }

[sources.app.tables."public.events"]
write_strategy = "append"
unique_key = ["event_id", "line_no"]
"#;

/// A project writing Postgres tables as Parquet files to a directory.
pub(crate) const FILES_PROJECT: &str = r#"
[connections.app_pg]
kind = "postgres"
host = "localhost"
user = "reader"
database = "app"

[connections.lake]
kind = "filesystem"
root = "/data/lake"

[targets.lake]
connection = "lake"

[sources.app]
kind = "postgres"
connection = "app_pg"
include = ["public.*"]
destination_path = "shop/{table}/"

[sources.app.tables."public.events"]
destination_path = "shop/{table}/day={cursor:date}/"
cursor_column = "created_at"

[sources.events]
kind = "files"
connection = "lake"
path = "events/"
format = "csv"
compression = "zstd"
csv = { delimiter = ";", null_values = ["NA", ""] }
"#;

/// Connections and a target that the error cases' sources can refer to.
pub(crate) const CONNECTIONS: &str = "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[connections.pg]\nkind = \"postgres\"\nhost = \"h\"\nuser = \"u\"\ndatabase = \"d\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n";

/// Every `${ENV:NAME}` resolves to `<NAME>`.
pub(crate) fn environment(name: &str) -> Option<String> {
    Some(format!("<{name}>"))
}

/// `toml` after [`CONNECTIONS`] parsed with the Python `sources` (the host's JSON), and the value `read` takes from it.
pub(crate) fn parse_with_python(
    toml: &str,
    sources: &str,
    read: fn(&crate::project::models::ProjectConfig) -> String,
) -> Result<String, String> {
    let python: Vec<crate::python_sources::models::PythonSource> =
        serde_json::from_str(sources).map_err(|error| error.to_string())?;
    crate::project::main::parse_python_project::parse_python_project(
        &format!("{CONNECTIONS}{toml}"),
        &environment,
        &python,
    )
    .map(|project| read(&project))
    .map_err(|error| error.to_string())
}
