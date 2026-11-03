//! Parse configuration text.

use crate::config::_helpers::interpolate::interpolate_value;
use crate::config::_helpers::validate::validate;
use crate::config::errors::ConfigError;
use crate::config::models::Config;

/// Parse and validate TOML text, resolving `${NAME}` through `lookup`.
pub fn parse_config(
    text: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<Config, ConfigError> {
    let mut value: toml::Value =
        toml::from_str(text).map_err(|error| ConfigError::Syntax(error.to_string()))?;
    interpolate_value(&mut value, lookup).map_err(ConfigError::Interpolation)?;
    let mut config: Config = value
        .try_into()
        .map_err(|error: toml::de::Error| ConfigError::Syntax(error.to_string()))?;
    validate(&config)?;
    config.destination.database = config.destination.database.to_ascii_uppercase();
    config.destination.schema = config.destination.schema.to_ascii_uppercase();
    if let Some(s3) = config.staging.s3.as_mut() {
        s3.stage = s3.stage.to_ascii_uppercase();
    }
    Ok(config)
}
