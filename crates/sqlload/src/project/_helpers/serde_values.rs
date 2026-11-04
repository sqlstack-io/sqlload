use std::time::Duration;

use serde::{Deserialize, Deserializer};

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
