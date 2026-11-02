//! Load the configuration file.

use std::path::Path;

use crate::config::errors::ConfigError;
use crate::config::main::parse_config::parse_config;
use crate::config::models::Config;

/// Read, interpolate from the process environment, and validate a configuration file.
pub fn load_config(path: &Path) -> Result<Config, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.display().to_string(),
        source,
    })?;
    parse_config(&text, &environment_variable)
}

fn environment_variable(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => None,
    }
}
