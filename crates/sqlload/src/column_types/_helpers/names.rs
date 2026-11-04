use crate::column_types::_helpers::syntax::TypeSyntax;
use crate::column_types::constants::{
    AMBIGUOUS_TIMESTAMP_NAME, MAX_DECIMAL_PRECISION, MAX_FRACTIONAL_SECONDS_DIGITS,
    TIME_PRECISION_DIGITS,
};
use crate::column_types::errors::TypeError;
use crate::column_types::models::{ColumnType, Warehouse};

/// Where a recognised type name comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NameKind {
    /// A portable name, written exactly as SQLLoad spells it.
    Portable,
    /// A portable alias or a standard SQL name that means the same everywhere.
    Standard,
    /// A name only these warehouses use.
    Specific(&'static [Warehouse]),
}

const SNOWFLAKE: &[Warehouse] = &[Warehouse::Snowflake];
const POSTGRES: &[Warehouse] = &[Warehouse::Postgres];
const BIGQUERY: &[Warehouse] = &[Warehouse::BigQuery];
const CLICKHOUSE: &[Warehouse] = &[Warehouse::ClickHouse];
const SQL_SERVER: &[Warehouse] = &[Warehouse::SqlServer];
const SNOWFLAKE_DATABRICKS: &[Warehouse] = &[Warehouse::Snowflake, Warehouse::Databricks];
const POSTGRES_DUCKDB: &[Warehouse] = &[
    Warehouse::Postgres,
    Warehouse::DuckDb,
    Warehouse::MotherDuck,
];
const DUCKDB_SQLITE: &[Warehouse] = &[Warehouse::DuckDb, Warehouse::MotherDuck, Warehouse::Sqlite];

/// Names that take no arguments.
const PLAIN_NAMES: &[(&str, ColumnType, NameKind)] = &[
    ("bool", ColumnType::Bool, NameKind::Portable),
    ("smallint", ColumnType::SmallInt, NameKind::Portable),
    ("int", ColumnType::Int, NameKind::Portable),
    ("float", ColumnType::Float, NameKind::Portable),
    ("text", ColumnType::Text, NameKind::Portable),
    ("date", ColumnType::Date, NameKind::Portable),
    ("json", ColumnType::Json, NameKind::Portable),
    ("uuid", ColumnType::Uuid, NameKind::Portable),
    ("binary", ColumnType::Binary, NameKind::Portable),
    ("boolean", ColumnType::Bool, NameKind::Standard),
    ("integer", ColumnType::Int, NameKind::Standard),
    ("bigint", ColumnType::Int, NameKind::Standard),
    ("string", ColumnType::Text, NameKind::Standard),
    ("varchar", ColumnType::Text, NameKind::Standard),
    ("character varying", ColumnType::Text, NameKind::Standard),
    ("double", ColumnType::Float, NameKind::Standard),
    ("double precision", ColumnType::Float, NameKind::Standard),
    ("varbinary", ColumnType::Binary, NameKind::Standard),
    ("datetime", ColumnType::TimestampNtz, NameKind::Standard),
    (
        "timestamp with time zone",
        ColumnType::TimestampTz,
        NameKind::Standard,
    ),
    (
        "timestamp without time zone",
        ColumnType::TimestampNtz,
        NameKind::Standard,
    ),
    (
        "variant",
        ColumnType::Json,
        NameKind::Specific(SNOWFLAKE_DATABRICKS),
    ),
    ("object", ColumnType::Json, NameKind::Specific(SNOWFLAKE)),
    ("array", ColumnType::Json, NameKind::Specific(SNOWFLAKE)),
    ("jsonb", ColumnType::Json, NameKind::Specific(POSTGRES)),
    ("bytea", ColumnType::Binary, NameKind::Specific(POSTGRES)),
    ("int2", ColumnType::SmallInt, NameKind::Specific(POSTGRES)),
    ("int4", ColumnType::Int, NameKind::Specific(POSTGRES)),
    ("int8", ColumnType::Int, NameKind::Specific(POSTGRES)),
    ("float8", ColumnType::Float, NameKind::Specific(POSTGRES)),
    (
        "timestamptz",
        ColumnType::TimestampTz,
        NameKind::Specific(POSTGRES_DUCKDB),
    ),
    (
        "blob",
        ColumnType::Binary,
        NameKind::Specific(DUCKDB_SQLITE),
    ),
    ("int64", ColumnType::Int, NameKind::Specific(BIGQUERY)),
    ("float64", ColumnType::Float, NameKind::Specific(BIGQUERY)),
    ("bytes", ColumnType::Binary, NameKind::Specific(BIGQUERY)),
    (
        "int16",
        ColumnType::SmallInt,
        NameKind::Specific(CLICKHOUSE),
    ),
    ("int32", ColumnType::Int, NameKind::Specific(CLICKHOUSE)),
    ("bit", ColumnType::Bool, NameKind::Specific(SQL_SERVER)),
    ("nvarchar", ColumnType::Text, NameKind::Specific(SQL_SERVER)),
    (
        "datetime2",
        ColumnType::TimestampNtz,
        NameKind::Specific(SQL_SERVER),
    ),
    (
        "datetimeoffset",
        ColumnType::TimestampTz,
        NameKind::Specific(SQL_SERVER),
    ),
    (
        "uniqueidentifier",
        ColumnType::Uuid,
        NameKind::Specific(SQL_SERVER),
    ),
];

/// Names that are rejected outright, with the reason.
const REJECTED_NAMES: &[(&str, &str)] = &[
    (
        "timestamp_ltz",
        "depends on each session's time zone; use \"timestamp_tz\"",
    ),
    (
        "real",
        "is 32-bit in some warehouses and 64-bit in others; use \"float\" (64-bit)",
    ),
    (
        "tinyint",
        "has a different range in each warehouse; use \"smallint\"",
    ),
    (
        "char",
        "pads values with spaces; use \"text\" or \"varchar(n)\"",
    ),
    (
        "character",
        "pads values with spaces; use \"text\" or \"varchar(n)\"",
    ),
];

/// Every name SQLLoad recognises, for suggestions.
pub(crate) fn known_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = PLAIN_NAMES.iter().map(|(name, _, _)| *name).collect();
    names.extend([
        "decimal",
        "numeric",
        "number",
        "time",
        "timestamp_tz",
        "timestamp_ntz",
    ]);
    names
}

/// The type `syntax` names, `Ok(None)` if the name is unknown.
pub(crate) fn resolve_name(
    syntax: &TypeSyntax,
    written: &str,
) -> Result<Option<(ColumnType, NameKind)>, TypeError> {
    if let Some((_, reason)) = REJECTED_NAMES.iter().find(|(name, _)| *name == syntax.name) {
        return Err(TypeError::Unsupported {
            written: written.to_owned(),
            reason: (*reason).to_owned(),
        });
    }
    if syntax.name == AMBIGUOUS_TIMESTAMP_NAME {
        return Err(TypeError::AmbiguousTimestamp {
            written: written.to_owned(),
        });
    }
    match syntax.args.as_deref() {
        None => match PLAIN_NAMES.iter().find(|(name, _, _)| *name == syntax.name) {
            Some((_, column_type, kind)) => Ok(Some((column_type.clone(), *kind))),
            None => resolve_with_args(syntax, &[], written),
        },
        Some(args) => resolve_with_args(syntax, args, written),
    }
}

fn resolve_with_args(
    syntax: &TypeSyntax,
    args: &[String],
    written: &str,
) -> Result<Option<(ColumnType, NameKind)>, TypeError> {
    let malformed = |reason: &str| TypeError::Malformed {
        written: written.to_owned(),
        reason: reason.to_owned(),
    };
    let resolved = match syntax.name.as_str() {
        "decimal" | "numeric" if args.is_empty() => {
            return Err(malformed(
                "needs a precision and scale, for example decimal(18,2)",
            ));
        }
        "decimal" => (decimal(args, written)?, NameKind::Portable),
        "numeric" => (decimal(args, written)?, NameKind::Standard),
        "number" => {
            let column_type = if args.is_empty() {
                ColumnType::Decimal {
                    precision: MAX_DECIMAL_PRECISION,
                    scale: 0,
                }
            } else {
                decimal(args, written)?
            };
            (column_type, NameKind::Specific(SNOWFLAKE))
        }
        "varchar" | "character varying" | "nvarchar" | "string" => {
            let kind = match syntax.name.as_str() {
                "varchar" => NameKind::Portable,
                "character varying" => NameKind::Standard,
                "nvarchar" => NameKind::Specific(SQL_SERVER),
                _ => NameKind::Specific(BIGQUERY),
            };
            (varchar(args, written)?, kind)
        }
        "varbinary" if is_max(args) => (ColumnType::Binary, NameKind::Specific(SQL_SERVER)),
        "varbinary" | "binary" if matches!(args, [length] if length.parse::<u32>().is_ok()) => {
            (ColumnType::Binary, NameKind::Standard)
        }
        "time" => (
            precision_only(args, written, ColumnType::Time)?,
            NameKind::Portable,
        ),
        "timestamp_tz" => (
            precision_only(args, written, ColumnType::TimestampTz)?,
            NameKind::Portable,
        ),
        "timestamp_ntz" => (
            precision_only(args, written, ColumnType::TimestampNtz)?,
            NameKind::Portable,
        ),
        _ if args.is_empty() => return Ok(None),
        _ => {
            return PLAIN_NAMES
                .iter()
                .find(|(name, _, _)| *name == syntax.name)
                .map_or(Ok(None), |_| Err(malformed("takes no arguments")));
        }
    };
    Ok(Some(resolved))
}

fn decimal(args: &[String], written: &str) -> Result<ColumnType, TypeError> {
    let malformed = |reason: String| TypeError::Malformed {
        written: written.to_owned(),
        reason,
    };
    let number = |text: &String| {
        text.parse::<u8>()
            .map_err(|_| malformed(format!("{text:?} is not a whole number")))
    };
    let (precision, scale) = match args {
        [precision] => (number(precision)?, 0),
        [precision, scale] => (number(precision)?, number(scale)?),
        _ => return Err(malformed("takes a precision and a scale".to_owned())),
    };
    if precision == 0 || precision > MAX_DECIMAL_PRECISION {
        return Err(malformed(format!(
            "precision must be between 1 and {MAX_DECIMAL_PRECISION}"
        )));
    }
    if scale > precision {
        return Err(malformed(
            "scale cannot be larger than the precision".to_owned(),
        ));
    }
    Ok(ColumnType::Decimal { precision, scale })
}

fn varchar(args: &[String], written: &str) -> Result<ColumnType, TypeError> {
    match args {
        [] => Ok(ColumnType::Text),
        [max] if max.eq_ignore_ascii_case("max") => Ok(ColumnType::Text),
        [length] => match length.parse::<u32>() {
            Ok(length) if length > 0 => Ok(ColumnType::Varchar(length)),
            _ => Err(TypeError::Malformed {
                written: written.to_owned(),
                reason: format!("length {length:?} must be a positive whole number"),
            }),
        },
        _ => Err(TypeError::Malformed {
            written: written.to_owned(),
            reason: "takes one length".to_owned(),
        }),
    }
}

fn is_max(args: &[String]) -> bool {
    matches!(args, [max] if max.eq_ignore_ascii_case("max"))
}

/// A time type that may carry a fractional-seconds precision, which SQLLoad fixes at microseconds.
fn precision_only(
    args: &[String],
    written: &str,
    column_type: ColumnType,
) -> Result<ColumnType, TypeError> {
    match args {
        [] => Ok(column_type),
        [digits]
            if digits
                .parse::<u8>()
                .is_ok_and(|digits| digits <= MAX_FRACTIONAL_SECONDS_DIGITS) =>
        {
            Ok(column_type)
        }
        _ => Err(TypeError::Malformed {
            written: written.to_owned(),
            reason: format!(
                "takes at most one fractional-seconds precision (SQLLoad stores {TIME_PRECISION_DIGITS} digits)"
            ),
        }),
    }
}
