use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::models::ColumnType;
use crate::snowflake_source::constants::{
    LONGEST_INT_DIGITS, LONGEST_VARCHAR, TIME_ZERO, UNLOAD_INTEGER,
};

/// A Snowflake type family, which decides how a column unloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Family {
    Number {
        precision: u8,
        scale: u8,
    },
    Float,
    Text {
        length: Option<u32>,
    },
    Bool,
    Date,
    Time,
    TimestampNtz,
    /// `TIMESTAMP_TZ` and `TIMESTAMP_LTZ`: instants.
    TimestampZoned,
    /// `VARIANT`, `OBJECT`, `ARRAY` and `MAP`.
    SemiStructured,
    Binary,
    /// Geospatial, vectors and anything newer.
    Other,
}

/// The family of a type as Snowflake prints it, such as `NUMBER(38,0)`, `VARCHAR(16777216)` or `TIMESTAMP_TZ(9)`.
pub(crate) fn family(type_text: &str) -> Family {
    let upper = type_text.trim().to_ascii_uppercase();
    let (base, arguments) = match upper.split_once('(') {
        Some((base, rest)) => (base.trim(), rest.trim_end_matches(')')),
        None => (upper.as_str(), ""),
    };
    let numbers: Vec<u32> = arguments.split(',').filter_map(type_argument).collect();
    let digits = |value: &u32| u8::try_from(*value).unwrap_or(u8::MAX);
    match base {
        "NUMBER" | "DECIMAL" | "NUMERIC" => Family::Number {
            precision: numbers.first().map_or(MAX_DECIMAL_PRECISION, digits),
            scale: numbers.get(1).map_or(0, digits),
        },
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "BYTEINT" => Family::Number {
            precision: MAX_DECIMAL_PRECISION,
            scale: 0,
        },
        "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "REAL" => Family::Float,
        "VARCHAR" | "TEXT" | "STRING" | "CHAR" | "CHARACTER" | "NCHAR" | "NVARCHAR" => {
            Family::Text {
                length: numbers.first().copied(),
            }
        }
        "BOOLEAN" => Family::Bool,
        "DATE" => Family::Date,
        "TIME" => Family::Time,
        "TIMESTAMP_NTZ" | "DATETIME" | "TIMESTAMP" => Family::TimestampNtz,
        "TIMESTAMP_TZ" | "TIMESTAMP_LTZ" => Family::TimestampZoned,
        "VARIANT" | "OBJECT" | "ARRAY" | "MAP" => Family::SemiStructured,
        "BINARY" | "VARBINARY" => Family::Binary,
        _ => Family::Other,
    }
}

/// One argument of a type, such as the `38` of `NUMBER(38,0)`; `None` for an argument that is not a number.
fn type_argument(part: &str) -> Option<u32> {
    match part.trim().parse::<u32>() {
        Ok(value) => Some(value),
        Err(_) => None,
    }
}

/// The portable type a Snowflake type maps to, or why a type must be pinned.
pub(crate) fn natural_type(type_text: &str) -> Result<ColumnType, String> {
    Ok(match family(type_text) {
        Family::Number {
            precision,
            scale: 0,
        } if precision <= LONGEST_INT_DIGITS => ColumnType::Int,
        Family::Number { precision, scale } if precision <= MAX_DECIMAL_PRECISION => {
            ColumnType::Decimal { precision, scale }
        }
        Family::Number { .. } => {
            return Err(format!(
                "{type_text} is wider than {MAX_DECIMAL_PRECISION} digits; pin \"text\" or a narrower decimal"
            ));
        }
        Family::Float => ColumnType::Float,
        Family::Text {
            length: Some(length),
        } if length < LONGEST_VARCHAR => ColumnType::Varchar(length),
        Family::Text { .. } => ColumnType::Text,
        Family::Bool => ColumnType::Bool,
        Family::Date => ColumnType::Date,
        Family::Time => ColumnType::Time,
        Family::TimestampNtz => ColumnType::TimestampNtz,
        Family::TimestampZoned => ColumnType::TimestampTz,
        Family::SemiStructured => ColumnType::Json,
        Family::Binary => ColumnType::Binary,
        Family::Other => {
            return Err(format!(
                "{type_text} has no portable type; pin one, such as \"text\""
            ));
        }
    })
}

/// The unload select expression reading column `quoted` (a quoted identifier) of Snowflake type `type_text` as `output`, or `None` if it cannot. Snowflake cannot unload `TIMESTAMP_TZ`/`LTZ` to Parquet and writes every other timestamp at milliseconds, so times and timestamps unload as whole microseconds, which SQLLoad turns back into them.
pub(crate) fn unload_expression(
    type_text: &str,
    output: &ColumnType,
    quoted: &str,
) -> Option<String> {
    let source = family(type_text);
    let micros = |value: String| format!("DATE_PART(EPOCH_MICROSECOND, {value})::{UNLOAD_INTEGER}");
    let expression = match (output, source) {
        (
            ColumnType::TimestampTz | ColumnType::TimestampNtz,
            Family::TimestampNtz | Family::TimestampZoned,
        ) => micros(quoted.to_owned()),
        (ColumnType::TimestampTz | ColumnType::TimestampNtz, Family::Date) => {
            micros(format!("{quoted}::TIMESTAMP_NTZ"))
        }
        (ColumnType::Time, Family::Time) => {
            format!("DATEDIFF(MICROSECOND, {TIME_ZERO}, {quoted})::{UNLOAD_INTEGER}")
        }
        (ColumnType::Date, Family::Date) => quoted.to_owned(),
        (ColumnType::Date, Family::TimestampNtz) => format!("{quoted}::DATE"),
        (ColumnType::Date, Family::TimestampZoned) => {
            format!("CONVERT_TIMEZONE('UTC', {quoted})::DATE")
        }
        (ColumnType::Int | ColumnType::SmallInt, Family::Number { scale: 0, .. }) => {
            format!("{quoted}::{UNLOAD_INTEGER}")
        }
        (ColumnType::Decimal { precision, scale }, Family::Number { .. } | Family::Float) => {
            format!("{quoted}::NUMBER({precision},{scale})")
        }
        (ColumnType::Float, Family::Number { .. } | Family::Float) => format!("{quoted}::DOUBLE"),
        (ColumnType::Bool, Family::Bool) | (ColumnType::Binary, Family::Binary) => {
            quoted.to_owned()
        }
        (ColumnType::Json, Family::SemiStructured)
        | (ColumnType::Text | ColumnType::Varchar(_), Family::SemiStructured) => {
            format!("TO_JSON({quoted})")
        }
        (
            ColumnType::Json | ColumnType::Uuid | ColumnType::Text | ColumnType::Varchar(_),
            Family::Text { .. },
        ) => quoted.to_owned(),
        (ColumnType::Text | ColumnType::Varchar(_), _) => format!("TO_VARCHAR({quoted})"),
        _ => return None,
    };
    Some(expression)
}

/// The name a Snowflake column loads as: a plain upper-case identifier in lower case, any other name exactly.
pub(crate) fn loaded_name(name: &str) -> String {
    let mut characters = name.chars();
    let plain = characters
        .next()
        .is_some_and(|first| first.is_ascii_uppercase() || first == '_')
        && characters.all(|character| {
            character.is_ascii_uppercase()
                || character.is_ascii_digit()
                || character == '_'
                || character == '$'
        });
    match plain {
        true => name.to_ascii_lowercase(),
        false => name.to_owned(),
    }
}

/// `name` as a Snowflake quoted identifier.
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
