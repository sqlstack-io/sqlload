use arrow_array::{Array, ArrayRef, StringArray};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_cast::{CastOptions, cast_with_options};
use std::sync::Arc;

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::external_stage::constants::STAGE_KEY_SYMBOLS;

/// Whether the direct `COPY` loads `column_type` exactly as SQLLoad's own reader does.
pub(crate) fn copyable_type(column_type: &ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Bool
            | ColumnType::SmallInt
            | ColumnType::Int
            | ColumnType::Decimal { .. }
            | ColumnType::Float
            | ColumnType::Json
            | ColumnType::Text
            | ColumnType::Date
            | ColumnType::TimestampTz
            | ColumnType::TimestampNtz
    )
}

/// Whether a stage path can name `key` exactly.
pub(crate) fn plain_key(key: &str) -> bool {
    key.chars()
        .all(|character| character.is_ascii_alphanumeric() || STAGE_KEY_SYMBOLS.contains(character))
}

/// A partition value as text Snowflake casts to the same value SQLLoad's reader produces: the path text converted as the reader converts it, printed in full.
pub(crate) fn partition_text(raw: &str, column_type: &ColumnType) -> Result<String, String> {
    if matches!(column_type, ColumnType::Text) {
        return Ok(raw.to_owned());
    }
    let field = arrow_field("value", column_type)
        .ok_or_else(|| format!("{column_type} has no Arrow representation"))?;
    let source: ArrayRef = Arc::new(StringArray::from(vec![raw]));
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let cast = cast_with_options(&source, field.data_type(), &strict)
        .map_err(|error| format!("{raw:?} does not convert to {column_type}: {error}"))?;
    if cast.is_null(0) {
        return Err(format!("{raw:?} does not convert to {column_type}"));
    }
    let options = FormatOptions::default()
        .with_timestamp_format(Some("%Y-%m-%d %H:%M:%S%.6f"))
        .with_timestamp_tz_format(Some("%Y-%m-%d %H:%M:%S%.6f %:z"))
        .with_date_format(Some("%Y-%m-%d"));
    let formatter =
        ArrayFormatter::try_new(cast.as_ref(), &options).map_err(|error| error.to_string())?;
    Ok(formatter.value(0).to_string())
}
