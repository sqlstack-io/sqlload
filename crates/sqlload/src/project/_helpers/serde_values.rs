use std::time::Duration;

use serde::{Deserialize, Deserializer};

use crate::project::constants::{MAX_PERCENT, WHOLE_BATCH};
use crate::project::models::BatchStep;

/// Deserialize an optional duration written as text such as `1h` or `2d`.
pub(crate) fn deserialize_optional_duration<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error> {
    Option::<String>::deserialize(deserializer)?
        .map(|text| humantime::parse_duration(&text).map_err(serde::de::Error::custom))
        .transpose()
}

#[derive(Deserialize)]
#[serde(untagged)]
enum BatchText {
    Duration(String),
    Integer(u64),
}

/// Deserialize a `batch_size`: a duration written as text (`"1d"`), a positive integer step, or `"none"`.
pub(crate) fn deserialize_optional_batch<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<BatchStep>, D::Error> {
    match Option::<BatchText>::deserialize(deserializer)? {
        None => Ok(None),
        Some(BatchText::Integer(0)) => Err(serde::de::Error::custom(
            "batch_size must be a positive step",
        )),
        Some(BatchText::Integer(step)) => Ok(Some(BatchStep::Integer(step))),
        Some(BatchText::Duration(text)) if text == WHOLE_BATCH => Ok(Some(BatchStep::Whole)),
        Some(BatchText::Duration(text)) => match humantime::parse_duration(&text) {
            Ok(step) if step.is_zero() => Err(serde::de::Error::custom(
                "batch_size must be a positive duration",
            )),
            Ok(step) => Ok(Some(BatchStep::Duration(step))),
            Err(error) => Err(serde::de::Error::custom(format!(
                "batch_size {text:?} is not a duration such as \"1d\" or \"none\": {error}"
            ))),
        },
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

/// Deserialize a key written as one column name or a list of them.
pub(crate) fn deserialize_optional_keys<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<String>>, D::Error> {
    Ok(
        Option::<OneOrMany>::deserialize(deserializer)?.map(|keys| match keys {
            OneOrMany::One(key) => vec![key],
            OneOrMany::Many(keys) => keys,
        }),
    )
}

/// Each source's `constant_columns` dates and times as `{ value, type }`: deserializing turns a TOML date or time into a plain string, which would load as text.
pub(crate) fn typed_constants(mut value: toml::Value) -> toml::Value {
    let Some(toml::Value::Table(sources)) = value.get_mut("sources") else {
        return value;
    };
    for (_, source) in sources.iter_mut() {
        let Some(toml::Value::Table(constants)) = source.get_mut("constant_columns") else {
            continue;
        };
        for (_, constant) in constants.iter_mut() {
            let toml::Value::Datetime(datetime) = constant else {
                continue;
            };
            let written = match (datetime.date, datetime.time, datetime.offset) {
                (Some(_), None, _) => "date",
                (Some(_), Some(_), Some(_)) => "timestamp_tz",
                (Some(_), Some(_), None) => "timestamp_ntz",
                (None, _, _) => "time",
            };
            *constant = toml::Value::Table(toml::Table::from_iter([
                (
                    "value".to_owned(),
                    toml::Value::String(datetime.to_string()),
                ),
                ("type".to_owned(), toml::Value::String(written.to_owned())),
            ]));
        }
    }
    value
}

/// Deserialize an optional percentage written as text such as `"20%"`: a whole number from 0 to 100.
pub(crate) fn deserialize_optional_percent<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u8>, D::Error> {
    let Some(text) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };
    let parsed = text
        .strip_suffix('%')
        .map(|number| number.trim().parse::<u8>());
    match parsed {
        Some(Ok(percent)) if percent <= MAX_PERCENT => Ok(Some(percent)),
        _ => Err(serde::de::Error::custom(format!(
            "{text:?} is not a percentage; write a whole number from 0 to {MAX_PERCENT} followed by %, such as \"20%\""
        ))),
    }
}
