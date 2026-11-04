/// A column type from the portable vocabulary, or a warehouse-only type passed through.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnType {
    Bool,
    SmallInt,
    /// 64-bit integer.
    Int,
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// 64-bit floating point.
    Float,
    Text,
    Varchar(u32),
    Date,
    Time,
    TimestampTz,
    TimestampNtz,
    Json,
    Uuid,
    Binary,
    /// `native("...")`: passed through to the warehouse verbatim; never widened or coerced.
    Native(String),
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bool => formatter.write_str("bool"),
            Self::SmallInt => formatter.write_str("smallint"),
            Self::Int => formatter.write_str("int"),
            Self::Decimal { precision, scale } => write!(formatter, "decimal({precision},{scale})"),
            Self::Float => formatter.write_str("float"),
            Self::Text => formatter.write_str("text"),
            Self::Varchar(length) => write!(formatter, "varchar({length})"),
            Self::Date => formatter.write_str("date"),
            Self::Time => formatter.write_str("time"),
            Self::TimestampTz => formatter.write_str("timestamp_tz"),
            Self::TimestampNtz => formatter.write_str("timestamp_ntz"),
            Self::Json => formatter.write_str("json"),
            Self::Uuid => formatter.write_str("uuid"),
            Self::Binary => formatter.write_str("binary"),
            Self::Native(name) => write!(formatter, "native({name:?})"),
        }
    }
}

/// An engine SQLLoad reads from or writes to; every engine sqlbuild has an adapter for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Warehouse {
    Snowflake,
    BigQuery,
    Databricks,
    Postgres,
    DuckDb,
    MotherDuck,
    ClickHouse,
    SqlServer,
    Sqlite,
}

impl std::fmt::Display for Warehouse {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Snowflake => "Snowflake",
            Self::BigQuery => "BigQuery",
            Self::Databricks => "Databricks",
            Self::Postgres => "Postgres",
            Self::DuckDb => "DuckDB",
            Self::MotherDuck => "MotherDuck",
            Self::ClickHouse => "ClickHouse",
            Self::SqlServer => "SQL Server",
            Self::Sqlite => "SQLite",
        })
    }
}

/// A declared type after parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedType {
    pub column_type: ColumnType,
    /// The text as written, when it was an alias or a warehouse SQL name rather than the portable name.
    pub written_as: Option<String>,
    /// Problems that do not stop the plan (for example a `native` type on a table with mixed destinations).
    pub warnings: Vec<String>,
}

/// A column type as one warehouse spells it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarehouseType {
    pub sql: String,
    /// Why the warehouse cannot hold the portable type exactly, if it cannot.
    pub lossy: Option<&'static str>,
}
