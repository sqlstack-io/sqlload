use std::time::Duration;

use serde::{Deserialize, Deserializer};

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

/// Deserialize a `batch`: a duration written as text (`"1d"`) or a positive integer step.
pub(crate) fn deserialize_optional_batch<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<BatchStep>, D::Error> {
    match Option::<BatchText>::deserialize(deserializer)? {
        None => Ok(None),
        Some(BatchText::Integer(0)) => {
            Err(serde::de::Error::custom("batch must be a positive step"))
        }
        Some(BatchText::Integer(step)) => Ok(Some(BatchStep::Integer(step))),
        Some(BatchText::Duration(text)) => match humantime::parse_duration(&text) {
            Ok(step) if step.is_zero() => Err(serde::de::Error::custom(
                "batch must be a positive duration",
            )),
            Ok(step) => Ok(Some(BatchStep::Duration(step))),
            Err(error) => Err(serde::de::Error::custom(format!(
                "batch {text:?} is not a duration such as \"1d\": {error}"
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
