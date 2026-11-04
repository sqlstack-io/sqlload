use crate::column_types::constants::{
    BIGQUERY_NUMERIC_MAX_INTEGER_DIGITS, BIGQUERY_NUMERIC_MAX_SCALE, SQL_SERVER_MAX_NVARCHAR_LENGTH,
};
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::{ColumnType, Warehouse, WarehouseType};

const UUID_AS_TEXT: &str = "stored as text";
const JSON_AS_TEXT: &str = "stored as JSON text";
const LENGTH_NOT_ENFORCED: &str = "the length limit is not enforced";

fn exact(sql: impl Into<String>) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: None,
    }
}

fn lossy(sql: impl Into<String>, reason: &'static str) -> WarehouseType {
    WarehouseType {
        sql: sql.into(),
        lossy: Some(reason),
    }
}

/// Snowflake, spelled as `DESCRIBE TABLE` reports the types.
pub(crate) fn snowflake(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("BOOLEAN"),
        ColumnType::SmallInt => lossy("NUMBER(38,0)", "stored as a 38-digit integer"),
        ColumnType::Int => exact("NUMBER(38,0)"),
        ColumnType::Decimal { precision, scale } => exact(format!("NUMBER({precision},{scale})")),
        ColumnType::Float => exact("FLOAT"),
        ColumnType::Text => exact("VARCHAR"),
        ColumnType::Varchar(length) => exact(format!("VARCHAR({length})")),
        ColumnType::Date => exact("DATE"),
        ColumnType::Time => exact("TIME(6)"),
        ColumnType::TimestampTz => exact("TIMESTAMP_TZ(6)"),
        ColumnType::TimestampNtz => exact("TIMESTAMP_NTZ(6)"),
        ColumnType::Json => exact("VARIANT"),
        ColumnType::Uuid => lossy("VARCHAR(36)", UUID_AS_TEXT),
        ColumnType::Binary => exact("BINARY"),
        ColumnType::Native(name) => exact(name.clone()),
    }
}

pub(crate) fn bigquery(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("BOOL"),
        ColumnType::SmallInt => lossy("INT64", "stored as a 64-bit integer"),
        ColumnType::Int => exact("INT64"),
        ColumnType::Decimal { precision, scale }
            if precision - scale <= BIGQUERY_NUMERIC_MAX_INTEGER_DIGITS
                && *scale <= BIGQUERY_NUMERIC_MAX_SCALE =>
        {
            exact(format!("NUMERIC({precision},{scale})"))
        }
        ColumnType::Decimal { precision, scale } => {
            exact(format!("BIGNUMERIC({precision},{scale})"))
        }
        ColumnType::Float => exact("FLOAT64"),
        ColumnType::Text => exact("STRING"),
        ColumnType::Varchar(length) => exact(format!("STRING({length})")),
        ColumnType::Date => exact("DATE"),
        ColumnType::Time => exact("TIME"),
        ColumnType::TimestampTz => lossy(
            "TIMESTAMP",
            "stored as a UTC instant; the offset is not kept",
        ),
        ColumnType::TimestampNtz => exact("DATETIME"),
        ColumnType::Json => exact("JSON"),
        ColumnType::Uuid => lossy("STRING", UUID_AS_TEXT),
        ColumnType::Binary => exact("BYTES"),
        ColumnType::Native(name) => exact(name.clone()),
    }
}

pub(crate) fn databricks(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("BOOLEAN"),
        ColumnType::SmallInt => exact("SMALLINT"),
        ColumnType::Int => exact("BIGINT"),
        ColumnType::Decimal { precision, scale } => exact(format!("DECIMAL({precision},{scale})")),
        ColumnType::Float => exact("DOUBLE"),
        ColumnType::Text => exact("STRING"),
        ColumnType::Varchar(length) => exact(format!("VARCHAR({length})")),
        ColumnType::Date => exact("DATE"),
        ColumnType::Time => lossy("STRING", "Databricks has no time type; stored as ISO text"),
        ColumnType::TimestampTz => lossy(
            "TIMESTAMP",
            "stored as a UTC instant; the offset is not kept",
        ),
        ColumnType::TimestampNtz => exact("TIMESTAMP_NTZ"),
        ColumnType::Json => lossy("STRING", JSON_AS_TEXT),
        ColumnType::Uuid => lossy("STRING", UUID_AS_TEXT),
        ColumnType::Binary => exact("BINARY"),
        ColumnType::Native(name) => exact(name.clone()),
    }
}

pub(crate) fn postgres(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("boolean"),
        ColumnType::SmallInt => exact("smallint"),
        ColumnType::Int => exact("bigint"),
        ColumnType::Decimal { precision, scale } => exact(format!("numeric({precision},{scale})")),
        ColumnType::Float => exact("double precision"),
        ColumnType::Text => exact("text"),
        ColumnType::Varchar(length) => exact(format!("character varying({length})")),
        ColumnType::Date => exact("date"),
        ColumnType::Time => exact("time without time zone"),
        ColumnType::TimestampTz => lossy(
            "timestamp with time zone",
            "stored as a UTC instant; the offset is not kept",
        ),
        ColumnType::TimestampNtz => exact("timestamp without time zone"),
        ColumnType::Json => exact("jsonb"),
        ColumnType::Uuid => exact("uuid"),
        ColumnType::Binary => exact("bytea"),
        ColumnType::Native(name) => exact(name.clone()),
    }
}

/// DuckDB and MotherDuck.
pub(crate) fn duckdb(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("BOOLEAN"),
        ColumnType::SmallInt => exact("SMALLINT"),
        ColumnType::Int => exact("BIGINT"),
        ColumnType::Decimal { precision, scale } => exact(format!("DECIMAL({precision},{scale})")),
        ColumnType::Float => exact("DOUBLE"),
        ColumnType::Text => exact("VARCHAR"),
        ColumnType::Varchar(_) => lossy("VARCHAR", LENGTH_NOT_ENFORCED),
        ColumnType::Date => exact("DATE"),
        ColumnType::Time => exact("TIME"),
        ColumnType::TimestampTz => lossy(
            "TIMESTAMP WITH TIME ZONE",
            "stored as a UTC instant; the offset is not kept",
        ),
        ColumnType::TimestampNtz => exact("TIMESTAMP"),
        ColumnType::Json => exact("JSON"),
        ColumnType::Uuid => exact("UUID"),
        ColumnType::Binary => exact("BLOB"),
        ColumnType::Native(name) => exact(name.clone()),
    }
}

pub(crate) fn clickhouse(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("Bool"),
        ColumnType::SmallInt => exact("Int16"),
        ColumnType::Int => exact("Int64"),
        ColumnType::Decimal { precision, scale } => exact(format!("Decimal({precision}, {scale})")),
        ColumnType::Float => exact("Float64"),
        ColumnType::Text => exact("String"),
        ColumnType::Varchar(_) => lossy("String", LENGTH_NOT_ENFORCED),
        ColumnType::Date => exact("Date32"),
        ColumnType::Time => lossy("String", "stored as ISO text"),
        ColumnType::TimestampTz => lossy(
            "DateTime64(6, 'UTC')",
            "stored as a UTC instant; the offset is not kept",
        ),
        ColumnType::TimestampNtz => exact("DateTime64(6)"),
        ColumnType::Json => lossy("String", JSON_AS_TEXT),
        ColumnType::Uuid => exact("UUID"),
        ColumnType::Binary => exact("String"),
        ColumnType::Native(name) => exact(name.clone()),
    }
}

pub(crate) fn sql_server(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => exact("BIT"),
        ColumnType::SmallInt => exact("SMALLINT"),
        ColumnType::Int => exact("BIGINT"),
        ColumnType::Decimal { precision, scale } => exact(format!("DECIMAL({precision},{scale})")),
        ColumnType::Float => exact("FLOAT"),
        ColumnType::Text => exact("NVARCHAR(MAX)"),
        ColumnType::Varchar(length) if *length <= SQL_SERVER_MAX_NVARCHAR_LENGTH => {
            exact(format!("NVARCHAR({length})"))
        }
        ColumnType::Varchar(_) => lossy("NVARCHAR(MAX)", LENGTH_NOT_ENFORCED),
        ColumnType::Date => exact("DATE"),
        ColumnType::Time => exact("TIME(6)"),
        ColumnType::TimestampTz => exact("DATETIMEOFFSET(6)"),
        ColumnType::TimestampNtz => exact("DATETIME2(6)"),
        ColumnType::Json => lossy("NVARCHAR(MAX)", JSON_AS_TEXT),
        ColumnType::Uuid => exact("UNIQUEIDENTIFIER"),
        ColumnType::Binary => exact("VARBINARY(MAX)"),
        ColumnType::Native(name) => exact(name.clone()),
    }
}

pub(crate) fn sqlite(column_type: &ColumnType) -> WarehouseType {
    match column_type {
        ColumnType::Bool => lossy("INTEGER", "stored as 0 or 1"),
        ColumnType::SmallInt | ColumnType::Int => exact("INTEGER"),
        ColumnType::Decimal { .. } => lossy("TEXT", "stored as decimal text to keep every digit"),
        ColumnType::Float => exact("REAL"),
        ColumnType::Text => exact("TEXT"),
        ColumnType::Varchar(_) => lossy("TEXT", LENGTH_NOT_ENFORCED),
        ColumnType::Date
        | ColumnType::Time
        | ColumnType::TimestampTz
        | ColumnType::TimestampNtz => lossy("TEXT", "stored as ISO 8601 text"),
        ColumnType::Json => lossy("TEXT", JSON_AS_TEXT),
        ColumnType::Uuid => lossy("TEXT", UUID_AS_TEXT),
        ColumnType::Binary => exact("BLOB"),
        ColumnType::Native(name) => exact(name.clone()),
    }
}

/// Parquet files, spelled as the Arrow type the files store; a `native` type has none.
pub(crate) fn parquet(column_type: &ColumnType) -> WarehouseType {
    match arrow_field("", column_type) {
        Some(field) => exact(field.data_type().to_string()),
        None => lossy(
            column_type.to_string(),
            "a native type has no Arrow type, so Parquet files cannot hold it",
        ),
    }
}

/// The mapping for `warehouse`.
pub(crate) fn for_warehouse(warehouse: Warehouse) -> fn(&ColumnType) -> WarehouseType {
    match warehouse {
        Warehouse::Snowflake => snowflake,
        Warehouse::BigQuery => bigquery,
        Warehouse::Databricks => databricks,
        Warehouse::Postgres => postgres,
        Warehouse::DuckDb | Warehouse::MotherDuck => duckdb,
        Warehouse::ClickHouse => clickhouse,
        Warehouse::SqlServer => sql_server,
        Warehouse::Sqlite => sqlite,
        Warehouse::Parquet => parquet,
    }
}
