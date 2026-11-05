/// The account and bucket the test runs against.
#[derive(Debug, Clone)]
pub struct Live {
    pub account: String,
    pub user: String,
    /// TOML for the connection's `auth`.
    pub auth: String,
    pub role: String,
    pub warehouse: String,
    pub database: String,
    pub schema: String,
    pub bucket: String,
    /// Below the bucket, ending in `/`: `sqlload/`.
    pub prefix: String,
    pub region: String,
    pub integration: String,
}

/// The files a parity case loads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParityFiles {
    /// Every candidate type, with NULLs and edge values.
    EveryType,
    /// Values whose digests must match byte for byte.
    Hashed,
    /// Text that parses as JSON of every shape.
    Json,
    /// Text that does not parse as JSON.
    BadJson,
    /// Files in typed partitions, of different lengths.
    Partitioned,
    /// A partition value that is not a date.
    BadPartition,
    /// NaN and both infinities, in both float widths.
    FloatEdges,
    /// Timestamps of each unit the direct route allows, with sub-millisecond digits, before and after 1970.
    TimestampEdges,
    /// Timestamps of each unit the direct route allows, in the Arrow zone `+00:00`, with sub-millisecond digits, before and after 1970.
    OffsetTimestamps,
    /// Decimals at the edges of precision and scale.
    DecimalEdges,
    /// The minimum and maximum of each integer width.
    IntegerEdges,
    /// No files: the comparison itself, on tables that differ only at microseconds, and that do not differ.
    ComparisonSelfTest,
}

/// How one side of a parity case loads the files.
#[derive(Debug, Clone, Copy)]
pub struct Side {
    /// `direct` (external stage, Snowflake reads the files where it can), `streaming` or `insert`.
    pub target: &'static str,
    /// TOML added to the source; `SOURCE` is its name.
    pub settings: &'static str,
}

/// The same files loaded two ways.
#[derive(Debug, Clone, Copy)]
pub struct ParityCase {
    /// Short and SQL-safe; names the tables and folder.
    pub name: &'static str,
    pub files: ParityFiles,
    pub partition_columns: &'static str,
    pub left: Side,
    pub right: Side,
    /// Whether the left side must be Snowflake reading the files itself.
    pub left_is_direct: bool,
    /// Whether the files embed the Arrow schema; without it the reader infers each type from Parquet alone.
    pub embedded_schema: bool,
}

/// The statements comparing two tables, typed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComparisonSql {
    /// One text: rows only on the left, rows only on the right, rows on the left, rows on the right.
    pub counts: String,
    /// Some rows only on the left, as text for the report.
    pub left_only: String,
    pub right_only: String,
}

/// One column of a compared table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComparedColumn<'a> {
    pub name: &'a str,
    /// As the information schema reports it.
    pub data_type: &'a str,
}
