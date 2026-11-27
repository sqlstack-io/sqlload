/// The most digits before the point a parameterised `NUMERIC` holds; more need `BIGNUMERIC`.
pub const NUMERIC_MAX_INTEGER_DIGITS: u8 = 29;

/// The most digits after the point a parameterised `NUMERIC` holds; more need `BIGNUMERIC`.
pub const NUMERIC_MAX_SCALE: u8 = 9;

/// The precision and scale BigQuery gives a `NUMERIC` declared without them.
pub const NUMERIC_DEFAULT: (u8, u8) = (38, 9);

/// Statements that would open or close a transaction, which separate query jobs cannot hold open.
pub const TRANSACTION_STATEMENTS: &[&str] = &[
    "BEGIN",
    "BEGIN TRANSACTION",
    "COMMIT",
    "COMMIT TRANSACTION",
    "ROLLBACK",
];

/// Shown for `copy_to_internal_stage` on a BigQuery target.
pub const NO_INTERNAL_STAGE_COPY: &str = "copy_to_internal_stage is for Snowflake targets; BigQuery takes the rows through its Storage Write API";

/// The most pending streams one batch's requests are spread over; each opens only when a request needs it.
pub const STREAMS_PER_BATCH: usize = 4;

/// The most bytes one append request carries, under the Storage Write API's 10 MB request limit.
pub const APPEND_REQUEST_BYTES: usize = 9 * 1024 * 1024;

/// The types BigQuery reads a Parquet decimal as, the first that holds its precision and scale: a temporary external table's `decimalTargetTypes`.
pub const DECIMAL_TARGET_TYPES: [&str; 3] = ["NUMERIC", "BIGNUMERIC", "STRING"];

/// The longest column name BigQuery takes.
pub const MAX_COLUMN_NAME: usize = 300;

/// Prefixes BigQuery reserves for its own columns (in upper case; names ignore case), which no column it reads from Parquet may start with.
pub const RESERVED_COLUMN_PREFIXES: [&str; 6] = [
    "_TABLE_",
    "_FILE_",
    "_PARTITION",
    "_ROW_TIMESTAMP",
    "__ROOT__",
    "_COLIDENTIFIER",
];

/// The most bytes of SQL one query job over files in GCS holds, under BigQuery's limit of 1,024K characters for a query before it is resolved.
pub const MAX_QUERY_BYTES: usize = 900 * 1024;

/// The most files one query job over files in GCS reads.
pub const MAX_FILES_PER_QUERY: usize = 1000;

/// What a query over files in GCS fails with on a row of a file not in its batch, before the file's name.
pub const UNEXPECTED_FILE: &str = "sqlload: unexpected file ";

/// The most columns BigQuery clusters a table by.
pub const MAX_CLUSTER_COLUMNS: usize = 4;

/// The table setting that makes BigQuery refuse queries of a table without a filter on its partition column.
pub const REQUIRE_PARTITION_FILTER: &str = "require_partition_filter";

/// The script variable a pruned write declares: the staged rows' partitions, or a range partition's lowest and highest staged value.
pub const STAGED_PARTITIONS: &str = "__sqlload_partitions";

/// The partition column's staged value, as the script that declares the staged partitions names it.
pub const PARTITION_VALUE: &str = "__sqlload_partition_value";
