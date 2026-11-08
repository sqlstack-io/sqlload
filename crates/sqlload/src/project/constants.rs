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
    "batch_size",
    "engine",
    "order_by",
    "partition_by",
    "primary_key",
    "table_settings",
    #[cfg(feature = "container-tests")]
    "test_loader_route",
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
    "path",
    "format",
    "compression",
    "csv",
    "destination_path",
    "partition_columns",
    "path_columns",
    "unmatched_paths",
    "constant_columns",
    "copy_to_internal_stage",
    "exclude_columns",
    "skip_tables_with_columns",
    "allow_columns",
    "mask_key",
    "tables",
    "publication",
    "slot",
    "cdc_mode",
    "deletes",
    "batch_interval",
];

/// Keys a Python source may set (`kind` is added by discovery), besides the table settings.
pub const PYTHON_SOURCE_KEYS: &[&str] = &[
    "kind",
    "connection",
    "description",
    "destination",
    "schema",
    "table_name",
    "destination_path",
    "exclude_columns",
    "allow_columns",
    "mask_key",
    "shared_client",
    "parallel_tables",
    "tables",
];

/// Table settings only Python sources and tables take.
pub const PYTHON_SETTING_KEYS: &[&str] = &["allow_empty", "state"];

/// Keys only a table may use.
pub const TABLE_ONLY_KEYS: &[&str] = &[
    "description",
    "name",
    "destination_path",
    "columns",
    "mask",
    "exclude_columns",
    "allow_columns",
    "flatten",
];

/// Top-level keys that mark a file as a loader project rather than an exporter configuration.
pub const PROJECT_MARKER_KEYS: &[&str] = &["connections", "targets", "sources"];

/// The placeholder every destination table name template must contain.
pub const TABLE_PLACEHOLDER: &str = "{table}";

/// The warehouse connection kinds a target may load into.
pub const WAREHOUSE_CONNECTION_KINDS: &[&str] = &[
    "snowflake",
    "databricks",
    "bigquery",
    "clickhouse",
    "duckdb",
];

/// The connection kind of a ClickHouse server, whose targets name no database above their loader schema.
pub const CLICKHOUSE_CONNECTION_KIND: &str = "clickhouse";

/// The connection kinds a target writes files into.
pub const FILES_CONNECTION_KINDS: &[&str] = &["s3", "filesystem"];

/// The table folder template of a files target that sets no `path`.
pub const DEFAULT_DESTINATION_PATH: &str = "{source}/{table}/";

/// The most Parquet footers `plan` reads for one files table unless the target sets `max_footer_reads`.
pub const DEFAULT_MAX_FOOTER_READS: usize = 100;

/// The Postgres port used when a connection sets none.
pub const DEFAULT_POSTGRES_PORT: u16 = 5432;

/// The ClickHouse user a connection that sets none signs in as.
pub const DEFAULT_CLICKHOUSE_USER: &str = "default";

/// The destination table name template used when a source sets none.
pub const DEFAULT_TABLE_NAME: &str = "{source}__{table}";

/// The destination table name template for a files source, which has one table.
pub const DEFAULT_FILES_TABLE_NAME: &str = "{source}";

/// The `batch_size` value that opts a table out of windows and ordered reads.
pub const WHOLE_BATCH: &str = "none";

/// The parts of an existing stage's name in a Snowflake connection's `load_method`: `DATABASE.SCHEMA.STAGE`.
pub const STAGE_NAME_PARTS: usize = 3;

/// The longest Postgres identifier, in bytes: a `postgres_cdc` slot or publication name.
pub const MAX_POSTGRES_NAME_BYTES: usize = 63;

/// The shortest `batch_interval` a `postgres_cdc` source takes.
pub const MIN_BATCH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);
