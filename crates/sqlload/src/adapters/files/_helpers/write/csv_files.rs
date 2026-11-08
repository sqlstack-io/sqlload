//! Render Arrow batches as CSV text, and compress the text as it is written.

use std::io::Write;

use arrow_array::{Array, ArrayRef, Float32Array, Float64Array, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Schema};
use flate2::Compression as GzipLevel;
use flate2::write::GzEncoder;

use crate::adapters::files::constants::{
    CSV_DATE, CSV_TIME, CSV_TIMESTAMP, CSV_TIMESTAMP_TZ, ZSTD_FRAME_BYTES,
};
use crate::adapters::files::models::CsvOut;
use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::models::ColumnType;
use crate::project::models::{CsvOutput, OutputCompression};
use crate::row_batches::constants::UTC_ZONE;

/// The header line: every column name, quoted where it needs to be.
pub(crate) fn csv_header(schema: &Schema, csv: &CsvOutput) -> Vec<u8> {
    let names: Vec<Option<String>> = schema
        .fields()
        .iter()
        .map(|field| Some(field.name().clone()))
        .collect();
    csv_line(names.iter(), csv).into_bytes()
}

/// `batch` as CSV lines, each value rendered as the files target documents it.
pub(crate) fn csv_rows(batch: &RecordBatch, csv: &CsvOutput) -> Result<Vec<u8>, String> {
    let schema = batch.schema();
    let columns: Vec<Vec<Option<String>>> = batch
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .map(|(array, field)| column_text(array, field))
        .collect::<Result<_, String>>()?;
    let lines: String = (0..batch.num_rows())
        .map(|row| csv_line(columns.iter().map(|column| &column[row]), csv))
        .collect();
    Ok(lines.into_bytes())
}

/// An empty CSV body, compressed as `compression` says.
pub(crate) fn new_csv(compression: OutputCompression) -> CsvOut {
    match compression {
        OutputCompression::None => CsvOut::Plain(Vec::new()),
        OutputCompression::Gzip => CsvOut::Gzip(GzEncoder::new(Vec::new(), GzipLevel::default())),
        OutputCompression::Zstd => CsvOut::Zstd {
            done: Vec::new(),
            pending: Vec::new(),
        },
    }
}

/// `out` with `text` added; zstd text is compressed one frame at a time once enough waits.
pub(crate) fn push_csv(out: CsvOut, text: &[u8]) -> Result<CsvOut, String> {
    Ok(match out {
        CsvOut::Plain(mut bytes) => {
            bytes.extend_from_slice(text);
            CsvOut::Plain(bytes)
        }
        CsvOut::Gzip(mut encoder) => {
            encoder.write_all(text).map_err(|error| error.to_string())?;
            CsvOut::Gzip(encoder)
        }
        CsvOut::Zstd { done, mut pending } => {
            pending.extend_from_slice(text);
            match pending.len() >= ZSTD_FRAME_BYTES {
                true => CsvOut::Zstd {
                    done: zstd_frame(done, &pending)?,
                    pending: Vec::new(),
                },
                false => CsvOut::Zstd { done, pending },
            }
        }
    })
}

/// The file's bytes: the text so far, compression finished.
pub(crate) fn finish_csv(out: CsvOut) -> Result<Vec<u8>, String> {
    match out {
        CsvOut::Plain(bytes) => Ok(bytes),
        CsvOut::Gzip(encoder) => encoder.finish().map_err(|error| error.to_string()),
        CsvOut::Zstd { done, pending } if pending.is_empty() && !done.is_empty() => Ok(done),
        CsvOut::Zstd { done, pending } => zstd_frame(done, &pending),
    }
}

/// The bytes a CSV body holds in memory.
pub(crate) fn csv_bytes(out: &CsvOut) -> usize {
    match out {
        CsvOut::Plain(bytes) => bytes.len(),
        CsvOut::Gzip(encoder) => encoder.get_ref().len(),
        CsvOut::Zstd { done, pending } => done.len() + pending.len(),
    }
}

/// `done` with `text` added as one more zstd frame (a zstd file may hold any number of frames).
fn zstd_frame(mut done: Vec<u8>, text: &[u8]) -> Result<Vec<u8>, String> {
    let frame = zstd::encode_all(text, zstd::DEFAULT_COMPRESSION_LEVEL)
        .map_err(|error| error.to_string())?;
    done.extend_from_slice(&frame);
    Ok(done)
}

/// Each value of one column as text, `None` for NULL.
fn column_text(array: &ArrayRef, field: &Field) -> Result<Vec<Option<String>>, String> {
    let json = field
        .metadata()
        .get(ARROW_TYPE_METADATA_KEY)
        .is_some_and(|written| *written == ColumnType::Json.to_string());
    let texts: Vec<Option<String>> = match (array.data_type(), json) {
        (DataType::Float64, _) => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|values| values.iter().map(|value| value.map(float_text)).collect())
            .ok_or_else(|| format!("{}: not a float column", field.name()))?,
        (DataType::Float32, _) => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|values| values.iter().map(|value| value.map(float_text)).collect())
            .ok_or_else(|| format!("{}: not a float column", field.name()))?,
        (DataType::Utf8, true) => array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| format!("{}: not a text column", field.name()))?
            .iter()
            .map(|value| {
                value
                    .map(|value| compact_json(value, field.name()))
                    .transpose()
            })
            .collect::<Result<_, String>>()?,
        (DataType::Timestamp(unit, Some(zone)), _) if zone.as_ref() != UTC_ZONE => {
            let utc = DataType::Timestamp(*unit, Some(UTC_ZONE.into()));
            let in_utc = cast(array, &utc).map_err(|error| format!("{}: {error}", field.name()))?;
            return column_text(&in_utc, field);
        }
        _ => {
            let options = FormatOptions::default()
                .with_timestamp_tz_format(Some(CSV_TIMESTAMP_TZ))
                .with_timestamp_format(Some(CSV_TIMESTAMP))
                .with_date_format(Some(CSV_DATE))
                .with_time_format(Some(CSV_TIME));
            let formatter = ArrayFormatter::try_new(array.as_ref(), &options)
                .map_err(|error| format!("{}: {error}", field.name()))?;
            (0..array.len())
                .map(|row| match array.is_null(row) {
                    true => None,
                    false => Some(formatter.value(row).to_string()),
                })
                .collect()
        }
    };
    Ok(texts)
}

/// A float in plain notation with the fewest digits that read back the same value, never an exponent; `NaN`, `Infinity` and `-Infinity` otherwise.
fn float_text<F: Into<f64> + std::fmt::Display + Copy>(value: F) -> String {
    let wide: f64 = value.into();
    match (wide.is_nan(), wide.is_infinite(), wide.is_sign_negative()) {
        (true, _, _) => "NaN".to_owned(),
        (false, true, false) => "Infinity".to_owned(),
        (false, true, true) => "-Infinity".to_owned(),
        _ => format!("{value}"),
    }
}

/// `text`, which must be one JSON value, without the whitespace between its tokens; keys keep their order and numbers their digits.
fn compact_json(text: &str, column: &str) -> Result<String, String> {
    serde_json::from_str::<serde::de::IgnoredAny>(text)
        .map_err(|error| format!("{column}: {text:?} is not JSON: {error}"))?;
    let mut compact = String::with_capacity(text.len());
    let (mut in_string, mut escaped) = (false, false);
    for character in text.chars() {
        let keep = in_string || !matches!(character, ' ' | '\t' | '\n' | '\r');
        if keep {
            compact.push(character);
        }
        (in_string, escaped) = match (in_string, escaped, character) {
            (true, true, _) => (true, false),
            (true, false, '\\') => (true, true),
            (true, false, '"') | (false, _, '"') => (!in_string, false),
            (state, _, _) => (state, false),
        };
    }
    Ok(compact)
}

/// One CSV line of `values`: NULL as `null_value` unquoted, and a value quoted when it is empty, equals `null_value`, or holds the delimiter, the quote or a line break.
fn csv_line<'a>(values: impl Iterator<Item = &'a Option<String>>, csv: &CsvOutput) -> String {
    let mut line = String::new();
    for (index, value) in values.enumerate() {
        if index > 0 {
            line.push(csv.delimiter);
        }
        let Some(value) = value else {
            line.push_str(&csv.null_value);
            continue;
        };
        let quoted = value.is_empty()
            || *value == csv.null_value
            || value.contains([csv.delimiter, csv.quote, '\n', '\r']);
        match quoted {
            true => {
                let doubled = format!("{}{}", csv.quote, csv.quote);
                line.push(csv.quote);
                line.push_str(&value.replace(csv.quote, &doubled));
                line.push(csv.quote);
            }
            false => line.push_str(value),
        }
    }
    line.push('\n');
    line
}
