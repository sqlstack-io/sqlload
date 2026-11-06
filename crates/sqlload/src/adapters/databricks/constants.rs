/// The table property letting Delta widen a column's type in place.
pub const TYPE_WIDENING: &str = "'delta.enableTypeWidening' = 'true'";

/// The table feature a `TIMESTAMP_NTZ` column added to an existing Delta table needs.
pub const TIMESTAMP_NTZ_FEATURE: &str = "'delta.feature.timestampNtz' = 'supported'";

/// How Databricks spells a timestamp without a time zone.
pub const TIMESTAMP_NTZ: &str = "TIMESTAMP_NTZ";
