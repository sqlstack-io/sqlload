use crate::column_types::constants::MAX_DECIMAL_PRECISION;
use crate::column_types::models::ColumnType;
use crate::postgres_source::constants::{ARRAY_CATEGORY, NATIVE_TYPE_NAMES};
use crate::postgres_source::models::{PgType, ReadColumn, SourceColumn, WireType};

/// A Postgres column's size details from the catalog.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TypeDetails {
    pub(crate) precision: Option<i32>,
    pub(crate) scale: Option<i32>,
    pub(crate) char_length: Option<i32>,
}

/// The type family of `pg_type.typname` with category `category`.
pub(crate) fn pg_type(type_name: &str, category: &str) -> PgType {
    if category == ARRAY_CATEGORY {
        return PgType::Array;
    }
    NATIVE_TYPE_NAMES
        .iter()
        .find(|(name, _)| *name == type_name)
        .map_or(PgType::Other, |(_, pg_type)| *pg_type)
}

/// The portable type a Postgres type maps to, or why a type must be pinned.
pub(crate) fn natural_type(pg_type: PgType, details: TypeDetails) -> Result<ColumnType, String> {
    Ok(match pg_type {
        PgType::Bool => ColumnType::Bool,
        PgType::Int2 => ColumnType::SmallInt,
        PgType::Int4 | PgType::Int8 | PgType::Oid => ColumnType::Int,
        PgType::Float4 | PgType::Float8 => ColumnType::Float,
        PgType::Numeric => return numeric_type(details),
        PgType::Varchar => match details.char_length.map(u32::try_from) {
            Some(Ok(length)) => ColumnType::Varchar(length),
            _ => ColumnType::Text,
        },
        PgType::Text | PgType::Other => ColumnType::Text,
        PgType::Date => ColumnType::Date,
        PgType::Time => ColumnType::Time,
        PgType::Timestamp => ColumnType::TimestampNtz,
        PgType::TimestampTz => ColumnType::TimestampTz,
        PgType::Json | PgType::Array => ColumnType::Json,
        PgType::Uuid => ColumnType::Uuid,
        PgType::Bytea => ColumnType::Binary,
    })
}

fn numeric_type(details: TypeDetails) -> Result<ColumnType, String> {
    let (Some(precision), Some(scale)) = (details.precision, details.scale) else {
        return Err(
            "numeric without a precision has no fixed scale; pin a type such as \"decimal(38,10)\" or \"text\""
                .to_owned(),
        );
    };
    match (u8::try_from(precision), u8::try_from(scale)) {
        (Ok(precision), Ok(scale)) if precision <= MAX_DECIMAL_PRECISION => {
            Ok(ColumnType::Decimal { precision, scale })
        }
        _ => Err(format!(
            "numeric({precision},{scale}) is wider than {MAX_DECIMAL_PRECISION} digits; pin \"text\" or a narrower decimal"
        )),
    }
}

/// The select expression and wire type that read a `pg_type` column as `output`; `None` if it cannot.
fn read_as(pg_type: PgType, output: &ColumnType, quoted: &str) -> Option<(String, WireType)> {
    let native = |wire: WireType| Some((quoted.to_owned(), wire));
    let cast = |to: &str, wire: WireType| Some((format!("{quoted}::{to}"), wire));
    match (output, pg_type) {
        (ColumnType::Bool, PgType::Bool) => native(WireType::Bool),
        (ColumnType::SmallInt | ColumnType::Int, PgType::Int2) => native(WireType::Int2),
        (ColumnType::Int, PgType::Int4) => native(WireType::Int4),
        (ColumnType::Int, PgType::Int8) => native(WireType::Int8),
        (ColumnType::Int, PgType::Oid) => cast("int8", WireType::Int8),
        (ColumnType::Float, PgType::Float4) => native(WireType::Float4),
        (ColumnType::Float, PgType::Float8) => native(WireType::Float8),
        (ColumnType::Float, PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Numeric) => {
            cast("float8", WireType::Float8)
        }
        (ColumnType::Decimal { .. }, PgType::Numeric) => native(WireType::Numeric),
        (ColumnType::Decimal { .. }, PgType::Int2 | PgType::Int4 | PgType::Int8) => {
            cast("numeric", WireType::Numeric)
        }
        (ColumnType::Json, PgType::Array) => {
            Some((format!("to_jsonb({quoted})::text"), WireType::Text))
        }
        (ColumnType::Json, PgType::Json) => {
            Some((format!("{quoted}::jsonb::text"), WireType::Text))
        }
        (ColumnType::Uuid, PgType::Uuid) => native(WireType::Uuid),
        (ColumnType::Text | ColumnType::Varchar(_), PgType::Text | PgType::Varchar) => {
            native(WireType::Text)
        }
        (ColumnType::Text | ColumnType::Varchar(_), _) => cast("text", WireType::Text),
        (ColumnType::Date, PgType::Date) => native(WireType::Date),
        (ColumnType::Time, PgType::Time) => native(WireType::Time),
        (ColumnType::TimestampTz, PgType::TimestampTz)
        | (ColumnType::TimestampNtz, PgType::Timestamp) => native(WireType::Timestamp),
        (ColumnType::TimestampNtz, PgType::TimestampTz) => Some((
            format!("({quoted} AT TIME ZONE 'UTC')"),
            WireType::Timestamp,
        )),
        (ColumnType::Binary, PgType::Bytea) => native(WireType::Bytea),
        _ => None,
    }
}

/// How to read `column` so that it arrives as `output`, or why it cannot.
pub(crate) fn read_column(
    column: &SourceColumn,
    output: &ColumnType,
) -> Result<ReadColumn, String> {
    let quoted = quote_identifier(&column.name);
    let Some((expression, wire)) = read_as(column.pg_type, output, &quoted) else {
        return Err(format!(
            "{} has type {}, which cannot be read as {output}",
            column.name, column.postgres_type
        ));
    };
    Ok(ReadColumn {
        name: column.name.clone(),
        expression,
        wire,
        output: output.clone(),
    })
}

/// `name` as a Postgres quoted identifier.
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
