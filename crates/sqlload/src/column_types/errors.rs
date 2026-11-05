/// Why a declared column type was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TypeError {
    #[error("unknown column type {written:?}; did you mean {suggestion:?}?")]
    Unknown { written: String, suggestion: String },
    #[error("column type {written:?} is malformed: {reason}")]
    Malformed { written: String, reason: String },
    #[error(
        "{written:?} means different things in different warehouses; use \"timestamp_tz\" (with a time zone) or \"timestamp_ntz\" (without)"
    )]
    AmbiguousTimestamp { written: String },
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
