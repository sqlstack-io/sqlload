/// Why a declared column type was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TypeError {
    #[error("unknown column type {written:?}; did you mean {suggestion:?}?")]
    Unknown { written: String, suggestion: String },
    #[error("column type {written:?} is malformed: {reason}")]
    Malformed { written: String, reason: String },
    #[error("{written:?} {readings}; use {portable}")]
    DependsOnDestination {
        written: String,
        /// How the table's destinations read it, such as `is timestamp_tz in BigQuery, but timestamp_ntz in DuckDB`.
        readings: String,
        portable: String,
    },
    #[error(
        "{written:?} is a {warehouse} type, but this table also loads into {others}; use the portable name {portable:?}"
    )]
    WarehouseSpecific {
        written: String,
        warehouse: &'static str,
        others: String,
        portable: String,
    },
    #[error("{written:?} {reason}")]
    Unsupported { written: String, reason: String },
}
