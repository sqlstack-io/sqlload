/// Keys every table setting may use, at source level or per table.
pub const TABLE_SETTING_KEYS: &[&str] = &[
    "write_strategy",
    "unique_key",
    "cursor_column",
    "cursor_start",
    "lookback",
    "append_cursor_inclusive",
    "contract",
    "on_schema_change",
    "auto_widen",
    "type_change_rebuild",
];

/// Keys only a source may use.
pub const SOURCE_ONLY_KEYS: &[&str] = &[
    "kind",
    "connection",
    "description",
    "destination",
    "schema",
    "include",
    "exclude",
    "new_tables",
    "table_name",
    "exclude_columns",
    "skip_tables_with_columns",
    "allow_columns",
    "mask_key",
    "tables",
];

/// Keys only a table may use.
pub const TABLE_ONLY_KEYS: &[&str] = &[
    "description",
    "name",
    "columns",
    "mask",
    "exclude_columns",
    "allow_columns",
];

/// Top-level keys that mark a file as a loader project rather than an exporter configuration.
pub const PROJECT_MARKER_KEYS: &[&str] = &["connections", "targets", "sources"];

/// The placeholder every destination table name template must contain.
pub const TABLE_PLACEHOLDER: &str = "{table}";

/// The connection kinds a target may load into.
pub const WAREHOUSE_CONNECTION_KINDS: &[&str] = &["snowflake", "duckdb"];

/// The Postgres port used when a connection sets none.
pub const DEFAULT_POSTGRES_PORT: u16 = 5432;

/// The destination table name template used when a source sets none.
pub const DEFAULT_TABLE_NAME: &str = "{source}__{table}";
