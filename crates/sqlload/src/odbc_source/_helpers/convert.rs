//! Batches as the driver gives them, converted to the columns' planned types.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Schema, SchemaRef};

use crate::column_types::main::arrow_field::arrow_field;
use crate::postgres_source::models::ReadColumn;

/// The schema of the batches a read passes on: each read column under its name, in its planned type.
pub(crate) fn output_schema(columns: &[ReadColumn]) -> Result<SchemaRef, String> {
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        let field = arrow_field(&column.name, &column.output).ok_or_else(|| {
            format!(
                "{} cannot be read as {}; pin another type",
                column.name, column.output
            )
        })?;
        fields.push(field);
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// `batch`'s columns, by position, converted to `schema`'s types; a value that does not convert fails, naming its column.
pub(crate) fn converted(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, String> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        if let (DataType::Decimal128(precision, scale), Some(texts)) =
            (field.data_type(), array.as_string_opt::<i32>())
        {
            exact_decimals(texts.iter().flatten(), field.name(), (*precision, *scale))?;
        }
        let cast = cast_with_options(array, field.data_type(), &strict).map_err(|error| {
            format!(
                "{} does not load as {}: {error}",
                field.name(),
                field.data_type()
            )
        })?;
        arrays.push(cast);
    }
    RecordBatch::try_new(schema.clone(), arrays).map_err(|error| error.to_string())
}

/// Fails on the first of `texts` with more decimal places than `scale`, which casting would round away, naming the column and a type that holds it.
fn exact_decimals<'a>(
    texts: impl Iterator<Item = &'a str>,
    name: &str,
    (precision, scale): (u8, i8),
) -> Result<(), String> {
    for text in texts {
        let places = decimal_places(text)
            .map_err(|problem| format!("{name} holds {text}, which {problem}"))?;
        if places > i64::from(scale) {
            return Err(format!(
                "{name} holds {text}, which has more decimal places than decimal({precision},{scale}) keeps; pin a type that holds it, such as columns = {{ {name} = \"decimal(38,{})\" }} or \"text\"",
                places.min(38),
            ));
        }
    }
    Ok(())
}

/// How many decimal places `text` needs to be held exactly, such as 2 for `1.250` (trailing zeros need none) and 4 for `12e-4`; fails when it isn't a plain decimal number.
fn decimal_places(text: &str) -> Result<i64, String> {
    let unsigned = text.trim().trim_start_matches(['-', '+']);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (
            mantissa,
            exponent
                .parse::<i64>()
                .map_err(|_| "is not a decimal number".to_owned())?,
        ),
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{whole}{fraction}");
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("is not a decimal number".to_owned());
    }
    let significant = digits.trim_end_matches('0').len() as i64;
    let point = whole.len() as i64 + exponent;
    Ok((significant - point).max(0))
}
