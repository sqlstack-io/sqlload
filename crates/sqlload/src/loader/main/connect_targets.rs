//! Open the destinations a project's targets use.

use std::collections::BTreeMap;

use crate::duckdb_destination::main::open_duckdb::open_duckdb;
use crate::loader::errors::LoaderError;
use crate::loader::models::Destination;
use crate::project::models::{ConnectionConfig, ProjectConfig};
use crate::snowflake_destination::main::connect_snowflake::connect_snowflake;
use crate::snowflake_destination::models::SnowflakeAccount;

/// One open destination per target, keyed by target name.
pub async fn connect_targets(
    project: &ProjectConfig,
) -> Result<BTreeMap<String, Destination>, LoaderError> {
    let mut destinations: BTreeMap<String, Destination> = BTreeMap::new();
    for (name, target) in &project.targets {
        let destination = match project.connections.get(&target.connection) {
            Some(ConnectionConfig::DuckDb { path }) => Destination::DuckDb(open_duckdb(path)?),
            Some(ConnectionConfig::Snowflake {
                account,
                user,
                auth,
                role,
                warehouse,
                database,
                base_url,
            }) => Destination::Snowflake(Box::new(connect_snowflake(SnowflakeAccount {
                account: account.clone(),
                user: user.clone(),
                auth: auth.clone(),
                role: role.clone(),
                warehouse: warehouse.clone(),
                database: target.database.clone().unwrap_or_else(|| database.clone()),
                base_url: base_url.clone(),
            })?)),
            Some(other) => {
                return Err(LoaderError::Invalid(format!(
                    "target {name:?}: {} destinations are not supported yet",
                    other.kind()
                )));
            }
            None => {
                return Err(LoaderError::Invalid(format!(
                    "target {name:?}: connection {:?} does not exist",
                    target.connection
                )));
            }
        };
        destinations.insert(name.clone(), destination);
    }
    Ok(destinations)
}
