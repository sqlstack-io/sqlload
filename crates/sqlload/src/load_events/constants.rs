/// The version of the load events' shape. Adding a field or an event keeps it; renaming or removing one moves it on. Version 0 may still change until a reader outside SQLLoad depends on it.
pub const EVENTS_SCHEMA_VERSION: u32 = 0;

/// How long a step before the plan runs before its own phase line stays on screen; quicker ones that end well only count towards the `Connected` and `Discovered` summary lines (`-v` shows every line).
pub const SLOW_STEP: std::time::Duration = std::time::Duration::from_secs(1);

/// The names the summary of the connections gives the systems a connection's label names in capitals, as the driver or the connection's `kind` gives them; others keep the label's words.
pub const SYSTEM_NAMES: [(&str, &str); 12] = [
    ("DUCKDB", "DuckDB"),
    ("MOTHERDUCK", "MotherDuck"),
    ("POSTGRES", "PostgreSQL"),
    ("POSTGRESQL", "PostgreSQL"),
    ("MICROSOFT SQL SERVER", "SQL Server"),
    ("MYSQL", "MySQL"),
    ("MARIADB", "MariaDB"),
    ("OPENEDGE", "OpenEdge"),
    ("SNOWFLAKE", "Snowflake"),
    ("BIGQUERY", "BigQuery"),
    ("DATABRICKS", "Databricks"),
    ("CLICKHOUSE", "ClickHouse"),
];
