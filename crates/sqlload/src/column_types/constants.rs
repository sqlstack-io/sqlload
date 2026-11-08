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

/// Field metadata key marking a text column's portable type when Arrow has no matching type (`json`, `uuid`).
pub const ARROW_TYPE_METADATA_KEY: &str = "sqlload.type";

/// Digits an `int` needs: 64-bit integers have up to 19.
pub const INT_DIGITS: u8 = 19;

/// Digits a `smallint` needs.
pub const SMALLINT_DIGITS: u8 = 5;

/// Characters in a hyphenated UUID.
pub const UUID_TEXT_LENGTH: u32 = 36;

/// The name of the array type, `array(<element>)`.
pub const ARRAY_NAME: &str = "array";

/// The element types an `array(...)` holds besides `decimal(p,s)`: scalars every destination keeps in an array.
pub const ARRAY_ELEMENTS: &[crate::column_types::models::ColumnType] = &[
    crate::column_types::models::ColumnType::Bool,
    crate::column_types::models::ColumnType::SmallInt,
    crate::column_types::models::ColumnType::Int,
    crate::column_types::models::ColumnType::Float,
    crate::column_types::models::ColumnType::Text,
    crate::column_types::models::ColumnType::Date,
    crate::column_types::models::ColumnType::TimestampTz,
    crate::column_types::models::ColumnType::TimestampNtz,
    crate::column_types::models::ColumnType::Uuid,
];

/// The name of an array's element field in Arrow and Parquet lists.
pub const ARRAY_ITEM: &str = "item";
