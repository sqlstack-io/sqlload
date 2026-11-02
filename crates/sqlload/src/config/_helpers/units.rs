use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::config::constants::SIZE_UNITS;

/// Parse `32MB`, `3.5MiB`, `1024` or `2 GB` into bytes.
pub(crate) fn parse_byte_size(text: &str) -> Result<u64, String> {
    let trimmed = text.trim();
    let split = trimmed
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split);
    let value: f64 = number
        .parse()
        .map_err(|_| format!("invalid size {text:?}"))?;
    let unit = unit.trim().to_ascii_lowercase();
    let multiplier = SIZE_UNITS
        .iter()
        .find(|(name, _)| *name == unit)
        .map(|(_, multiplier)| *multiplier)
        .ok_or_else(|| format!("unknown size unit {unit:?} in {text:?}"))?;
    Ok((value * multiplier).round() as u64)
}

/// Deserialize a duration written as text such as `60s`, `3m` or `7d`.
pub(crate) fn deserialize_duration<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Duration, D::Error> {
    let text = String::deserialize(deserializer)?;
    humantime::parse_duration(&text).map_err(serde::de::Error::custom)
}

/// Deserialize an optional duration written as text.
pub(crate) fn deserialize_optional_duration<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error> {
    let text = Option::<String>::deserialize(deserializer)?;
    text.map(|value| humantime::parse_duration(&value).map_err(serde::de::Error::custom))
        .transpose()
}
