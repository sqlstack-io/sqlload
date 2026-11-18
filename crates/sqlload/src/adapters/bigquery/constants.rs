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

/// Why BigQuery doesn't load a files table's Parquet in GCS itself while the table keeps `_sqlload_source_row`: BigQuery's own reads never say where in its file a row was.
pub const ROW_NUMBER_REASON: &str =
    "BigQuery doesn't report a row's position in its file, so it can't fill _sqlload_source_row";

/// The name the query that loads a batch's files in GCS gives the temporary external table over them.
pub const GCS_FILES_TABLE: &str = "_sqlload_files";

/// The types BigQuery reads a Parquet decimal as, the first that holds it: those SQLLoad's decimals load as, then text, which the cast to the column's type then refuses rather than round.
pub const PARQUET_DECIMAL_TYPES: &[&str] = &["NUMERIC", "BIGNUMERIC", "STRING"];
