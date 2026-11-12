//! Root row ids a merge derives from its key.

use arrow_array::RecordBatch;

use crate::row_ids::_helpers::digest::digest_row_id;
use crate::row_ids::_helpers::json_values::json_values;

/// Each row's id from its `key` columns of `batch`, each as `(name in the id, column index)`: the SHAKE128 digest of the key as compact JSON with its names sorted, written as dlt writes it (orjson), so a row keeps its id on every load.
pub fn key_row_ids(batch: &RecordBatch, key: &[(String, usize)]) -> Result<Vec<String>, String> {
    let mut sorted: Vec<&(String, usize)> = key.iter().collect();
    sorted.sort_by(|left, right| left.0.cmp(&right.0));
    let mut parts: Vec<(String, Vec<String>)> = Vec::with_capacity(sorted.len());
    for (name, index) in sorted {
        let name = serde_json::to_string(name).map_err(|error| error.to_string())?;
        parts.push((name, json_values(batch.column(*index))?));
    }
    Ok((0..batch.num_rows())
        .map(|row| digest_row_id(key_json(&parts, row).as_bytes()))
        .collect())
}

/// Row `row` of the key `parts` (each a quoted name and its column's JSON values) as one compact JSON object.
fn key_json(parts: &[(String, Vec<String>)], row: usize) -> String {
    let mut fields: Vec<String> = Vec::with_capacity(parts.len());
    for (name, values) in parts {
        fields.push(format!("{name}:{}", values[row]));
    }
    format!("{{{}}}", fields.join(","))
}
