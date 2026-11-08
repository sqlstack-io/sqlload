//! Type a files source's `constant_columns`.

use std::collections::BTreeMap;

use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::files_source::_helpers::plan::partitions::converts;

/// Each constant's column, type and value as text: a string is `text`, an integer `int`, a float `float` and a boolean `bool`; `{ value = "...", type = "..." }` takes any type `destinations` take, and must convert to it.
pub fn constant_columns(
    constants: &BTreeMap<String, toml::Value>,
    destinations: &[Warehouse],
) -> Result<Vec<(String, ColumnType, String)>, String> {
    constants
        .iter()
        .map(|(name, value)| {
            let fail = |reason: String| format!("constant column {name}: {reason}");
            let (column_type, text) = match value {
                toml::Value::Table(typed) => {
                    let (Some(value), Some(toml::Value::String(written)), 2) =
                        (typed.get("value"), typed.get("type"), typed.len())
                    else {
                        return Err(fail(format!(
                            "{value} must be a string, number or boolean, or {{ value = \"...\", type = \"...\" }}"
                        )));
                    };
                    let parsed = parse_type(written, destinations)
                        .map_err(|error| fail(error.to_string()))?;
                    let text = scalar_text(value).ok_or_else(|| {
                        fail(format!("{value} is not a string, number or boolean"))
                    })?;
                    converts(&text, &parsed.column_type).map_err(|reason| {
                        fail(format!("{text:?} is not a {}: {reason}", parsed.column_type))
                    })?;
                    (parsed.column_type, text)
                }
                toml::Value::String(text) => (ColumnType::Text, text.clone()),
                toml::Value::Integer(_) => (ColumnType::Int, value.to_string()),
                toml::Value::Float(_) => (ColumnType::Float, value.to_string()),
                toml::Value::Boolean(_) => (ColumnType::Bool, value.to_string()),
                _ => {
                    return Err(fail(format!(
                        "{value} must be a string, number or boolean, or {{ value = \"...\", type = \"...\" }}"
                    )));
                }
            };
            Ok((name.clone(), column_type, text))
        })
        .collect()
}

/// A string, number or boolean as text.
fn scalar_text(value: &toml::Value) -> Option<String> {
    match value {
        toml::Value::String(text) => Some(text.clone()),
        toml::Value::Integer(_) | toml::Value::Float(_) | toml::Value::Boolean(_) => {
            Some(value.to_string())
        }
        _ => None,
    }
}
