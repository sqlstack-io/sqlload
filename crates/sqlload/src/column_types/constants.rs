/// The portable type names, as suggested in error messages.
pub const PORTABLE_NAMES: &[&str] = &[
    "bool",
    "smallint",
    "int",
    "decimal(p,s)",
    "float",
    "text",
    "varchar(n)",
    "date",
    "time",
    "timestamp_tz",
    "timestamp_ntz",
    "json",
    "uuid",
    "binary",
    "native(\"...\")",
];

/// The largest decimal precision every supported warehouse can hold.
pub const MAX_DECIMAL_PRECISION: u8 = 38;

/// Timestamps and times are carried at microsecond precision, the finest every warehouse shares.
pub const TIME_PRECISION_DIGITS: u8 = 6;

/// The most fractional-seconds digits any warehouse accepts in a time type's precision.
pub const MAX_FRACTIONAL_SECONDS_DIGITS: u8 = 9;

/// The bare name whose meaning (with or without a time zone) differs between warehouses.
pub const AMBIGUOUS_TIMESTAMP_NAME: &str = "timestamp";

/// BigQuery `NUMERIC` holds at most this many digits before the decimal point.
pub const BIGQUERY_NUMERIC_MAX_INTEGER_DIGITS: u8 = 29;

/// BigQuery `NUMERIC` holds at most this many digits after the decimal point.
pub const BIGQUERY_NUMERIC_MAX_SCALE: u8 = 9;

/// The longest SQL Server `NVARCHAR(n)`; longer columns are `NVARCHAR(MAX)`.
pub const SQL_SERVER_MAX_NVARCHAR_LENGTH: u32 = 4000;

/// Field metadata key marking a text column's portable type when Arrow has no matching type (`json`, `uuid`).
pub const ARROW_TYPE_METADATA_KEY: &str = "sqlload.type";

/// Snowflake's default (and largest common) `VARCHAR` length; columns this long are plain text.
pub const SNOWFLAKE_MAX_TEXT_LENGTH: i64 = 16_777_216;

/// Digits an `int` needs: 64-bit integers have up to 19.
pub const INT_DIGITS: u8 = 19;

/// Digits a `smallint` needs.
pub const SMALLINT_DIGITS: u8 = 5;

/// Characters in a hyphenated UUID.
pub const UUID_TEXT_LENGTH: u32 = 36;
