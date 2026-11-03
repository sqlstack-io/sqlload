use std::io::Read;

use flate2::read::GzDecoder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use serde_json::Value;

use crate::fake_snowflake::constants::VARIANT_TYPE;

/// Gzip NDJSON rows, or None when the body is not gzip text.
pub(crate) fn ndjson_rows(body: &[u8]) -> Option<Vec<Value>> {
    let mut text = String::new();
    match GzDecoder::new(body).read_to_string(&mut text) {
        Ok(_) => {}
        Err(_) => return None,
    }
    Some(
        text.lines()
            .filter_map(|line| match serde_json::from_str(line) {
                Ok(value) => Some(value),
                Err(_) => None,
            })
            .collect(),
    )
}

/// Parquet rows as JSON objects; text in VARIANT columns is parsed, as the pipe's `PARSE_JSON` does.
pub(crate) fn parquet_rows(body: &[u8], columns: &[(String, String)]) -> Option<Vec<Value>> {
    let reader = match SerializedFileReader::new(bytes::Bytes::copy_from_slice(body)) {
        Ok(reader) => reader,
        Err(_) => return None,
    };
    let rows = match reader.get_row_iter(None) {
        Ok(rows) => rows,
        Err(_) => return None,
    };
    let variants: Vec<&String> = columns
        .iter()
        .filter(|(_, data_type)| data_type == VARIANT_TYPE)
        .map(|(name, _)| name)
        .collect();
    let mut parsed: Vec<Value> = Vec::new();
    for row in rows {
        let row = match row {
            Ok(row) => row,
            Err(_) => return None,
        };
        let Value::Object(mut object) = row.to_json_value() else {
            return None;
        };
        for name in &variants {
            if let Some(Value::String(text)) = object.get(*name) {
                let value = serde_json::from_str(text).unwrap_or(Value::Null);
                object.insert((*name).clone(), value);
            }
        }
        parsed.push(Value::Object(object));
    }
    Some(parsed)
}
