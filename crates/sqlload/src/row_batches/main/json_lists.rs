//! JSON array text as Arrow lists.

use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use arrow_schema::DataType;
use serde_json::Value;

use crate::column_types::main::exact_cast::exact_cast;

/// `texts`, JSON arrays as text (NULL for NULL), as a list array of `list_type`: each element converted from its text as a string column would be (a JSON string's own text, a number or boolean as written), strictly and exactly (see [`exact_cast`]), so a value that does not convert, or would lose digits, is an error, never NULL. A JSON `null` element is NULL; anything but an array of scalars is an error.
pub fn json_lists(texts: &ArrayRef, list_type: &DataType) -> Result<ArrayRef, String> {
    let texts = texts
        .as_string_opt::<i32>()
        .ok_or_else(|| format!("{} is not JSON text", texts.data_type()))?;
    let mut lists = ListBuilder::new(StringBuilder::new());
    for row in 0..texts.len() {
        if texts.is_null(row) {
            lists.append(false);
            continue;
        }
        let parsed: Value = serde_json::from_str(texts.value(row))
            .map_err(|error| format!("row {}: not JSON: {error}", row + 1))?;
        let Value::Array(values) = parsed else {
            return Err(format!(
                "row {}: {} is not a JSON array",
                row + 1,
                texts.value(row)
            ));
        };
        for value in &values {
            match value {
                Value::Null => lists.values().append_null(),
                Value::String(text) => lists.values().append_value(text),
                Value::Number(_) | Value::Bool(_) => lists.values().append_value(value.to_string()),
                Value::Array(_) | Value::Object(_) => {
                    return Err(format!(
                        "row {}: element {value} is not a single value",
                        row + 1
                    ));
                }
            }
        }
        lists.append(true);
    }
    let lists: ArrayRef = Arc::new(lists.finish());
    exact_cast(&lists, list_type)
        .map_err(|error| format!("an element does not convert to {list_type}: {error}"))
}
