//! Resolve `${NAME}` references in parsed TOML.

use crate::config::_helpers::interpolate::interpolate_value;

/// `value` with `${NAME}` in every string replaced by what `lookup` returns; `$${` stays literal.
pub fn interpolate_toml(
    mut value: toml::Value,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<toml::Value, String> {
    interpolate_value(&mut value, lookup)?;
    Ok(value)
}
