//! Read the read spec a yielded `sl.json(...)` or `sl.ndjson(...)` crosses with.

use crate::row_batches::main::projection::projection;
use crate::row_batches::models::{JsonLayout, JsonRead, JsonReadSpec};

/// The read `text` (a yielded JSON item's `__sqlload_json__()` spec) asks for, in batches of `batch_rows`, or why it can't be read: an NDJSON read with a `path`, or `fields` and `keep_record` that don't describe a projection.
pub fn json_read_spec(text: &str, batch_rows: usize) -> Result<JsonRead, String> {
    let spec: JsonReadSpec =
        serde_json::from_str(text).map_err(|error| format!("fields: {error}"))?;
    if spec.layout == JsonLayout::Lines && spec.path.is_some() {
        return Err("path picks rows out of a JSON document; NDJSON has none".to_owned());
    }
    let projection = projection(spec.fields.as_deref(), spec.keep_record.as_deref())?;
    Ok(JsonRead {
        layout: spec.layout,
        path: spec.path,
        batch_rows,
        projection,
    })
}
