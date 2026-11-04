//! Connection settings for Python table functions.

use crate::python_sources::_helpers::json_values::json_value;

/// Every connection's settings in the (interpolated) project `value`, as JSON, by name.
pub fn connection_settings(value: &toml::Value) -> serde_json::Map<String, serde_json::Value> {
    match value.get("connections").map(json_value) {
        Some(serde_json::Value::Object(settings)) => settings,
        _ => serde_json::Map::new(),
    }
}
