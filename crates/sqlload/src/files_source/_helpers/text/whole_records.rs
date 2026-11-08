//! JSON objects and CSV lines as one `json` value each, for `shape = "record"`.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use crate::files_source::_helpers::text::json_records::JsonRecord;

/// `records` as one text column `column`, each object's compact JSON text (keys in file order, a key given twice once with its last value, where it last appears), and the line each starts on.
pub(crate) fn json_records_batch(
    records: &[JsonRecord],
    column: &str,
) -> Result<(RecordBatch, Vec<u64>), String> {
    let texts: Vec<String> = records.iter().map(object_text).collect();
    let batch = one_column(column, texts)?;
    Ok((batch, records.iter().map(|record| record.line).collect()))
}

/// `batch`'s rows, CSV fields as text columns, as one text column `column`: each line a JSON array of its fields, a NULL field as `null`.
pub(crate) fn csv_lines_batch(batch: &RecordBatch, column: &str) -> Result<RecordBatch, String> {
    let fields: Vec<&StringArray> = batch
        .columns()
        .iter()
        .map(|array| array.as_any().downcast_ref::<StringArray>())
        .collect::<Option<_>>()
        .ok_or("a CSV field is not text")?;
    let texts: Vec<String> = (0..batch.num_rows())
        .map(|row| line_text(&fields, row))
        .collect::<Result<_, String>>()?;
    one_column(column, texts)
}

/// Row `row` of `fields` as a JSON array of strings and `null`s.
fn line_text(fields: &[&StringArray], row: usize) -> Result<String, String> {
    let values: Vec<Option<&str>> = fields
        .iter()
        .map(|field| (!field.is_null(row)).then(|| field.value(row)))
        .collect();
    serde_json::to_string(&values).map_err(|error| error.to_string())
}

/// One object's JSON text from its fields: nested objects and arrays as their compact source text, other values as JSON.
fn object_text(record: &JsonRecord) -> String {
    let members: Vec<String> = record
        .fields
        .iter()
        .map(|(key, value, nested)| {
            let key = serde_json::Value::String(key.clone());
            match nested {
                Some(text) => format!("{key}:{text}"),
                None => format!("{key}:{value}"),
            }
        })
        .collect();
    format!("{{{}}}", members.join(","))
}

/// A batch of one nullable text column `name` holding `texts`.
fn one_column(name: &str, texts: Vec<String>) -> Result<RecordBatch, String> {
    let schema = Schema::new(vec![Field::new(name, DataType::Utf8, true)]);
    let array: ArrayRef = Arc::new(StringArray::from(texts));
    RecordBatch::try_new(Arc::new(schema), vec![array]).map_err(|error| error.to_string())
}
