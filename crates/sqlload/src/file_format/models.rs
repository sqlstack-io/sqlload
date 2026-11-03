/// The Parquet type a column is written as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParquetKind {
    Int32,
    Int64,
    /// A UTC instant in milliseconds.
    Timestamp,
    Date,
    Text,
}

/// The column list and `SELECT` expressions a Parquet load uses, in envelope order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadSelect {
    /// `("A", "B")`.
    pub columns: String,
    /// `$1:"A"::NUMBER(38,0), PARSE_JSON($1:"B"::VARCHAR)`.
    pub expressions: String,
}
