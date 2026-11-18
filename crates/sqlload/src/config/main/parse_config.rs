//! Parse configuration text.

use crate::config::_helpers::defaults::destination_defaults;
use crate::config::_helpers::interpolate::interpolate_value;
use crate::config::_helpers::validate::validate;
use crate::config::errors::ConfigError;
use crate::config::models::{Config, Destination};

/// Parse and validate TOML text, resolving `${ENV:NAME}` through `lookup`.
pub fn parse_config(
    text: &str,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<Config, ConfigError> {
    let mut value: toml::Value =
        toml::from_str(text).map_err(|error| ConfigError::Syntax(error.to_string()))?;
    interpolate_value(&mut value, lookup).map_err(ConfigError::Interpolation)?;
    destination_defaults(&mut value);
    let mut config: Config = value
        .try_into()
        .map_err(|error: toml::de::Error| ConfigError::Syntax(error.to_string()))?;
    validate(&config)?;
    match &mut config.destination {
        Destination::Snowflake(snowflake) => {
            snowflake.database = snowflake.database.to_ascii_uppercase();
            snowflake.schema = snowflake.schema.to_ascii_uppercase();
            if let Some(s3) = config.staging.s3.as_mut() {
                s3.stage = s3.stage.to_ascii_uppercase();
            }
        }
        Destination::Databricks(_) | Destination::BigQuery(_) => {
            if let Destination::Databricks(databricks) = &mut config.destination {
                databricks.catalog = databricks.catalog.to_ascii_lowercase();
                databricks.schema = databricks.schema.to_ascii_lowercase();
            }
            let naming = &mut config.naming;
            naming.table_prefix = naming.table_prefix.to_ascii_uppercase();
            for table in naming.map.values_mut().chain(naming.fallback.as_mut()) {
                *table = table.to_ascii_uppercase();
            }
        }
    }
    Ok(config)
}
