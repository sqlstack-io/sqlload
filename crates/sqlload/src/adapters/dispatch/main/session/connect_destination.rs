//! Open the destination one target uses.

use crate::adapters::bigquery::main::session::_connect_bigquery::connect_bigquery;
use crate::adapters::contract::models::TableName;
use crate::adapters::databricks::main::session::_connect_databricks::connect_databricks;
use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_open_duckdb::open_duckdb;
use crate::adapters::files::models::FilesDestination;
use crate::adapters::snowflake::main::session::_connect_snowflake::connect_snowflake;
use crate::adapters::snowflake::main::stage::_forced_stage::forced_stage;
use crate::adapters::snowflake::models::SnowflakeAccount;
use crate::loader::errors::LoaderError;
use crate::project::models::{ConnectionConfig, ProjectConfig};
use crate::store_paths::models::StoreKind;

/// The open destination of target `name`.
pub async fn connect_destination(
    project: &ProjectConfig,
    name: &str,
) -> Result<Destination, LoaderError> {
    let target = project
        .targets
        .get(name)
        .ok_or_else(|| LoaderError::Invalid(format!("target {name:?} does not exist")))?;
    let destination = match project.connections.get(&target.connection) {
        #[cfg(feature = "duckdb")]
        Some(ConnectionConfig::DuckDb { database }) => Destination::DuckDb(open_duckdb(database)?),
        #[cfg(not(feature = "duckdb"))]
        Some(ConnectionConfig::DuckDb { .. }) => {
            return Err(LoaderError::Invalid(format!(
                "target {name:?}: this sqlload was built without DuckDB (cargo feature \"duckdb\")"
            )));
        }
        Some(connection @ ConnectionConfig::Snowflake { .. }) => {
            Destination::Snowflake(Box::new(snowflake(connection, target, name)?))
        }
        Some(connection @ ConnectionConfig::Databricks { .. }) => Destination::Databricks(
            Box::new(connect_databricks(connection, target.database.as_deref())?),
        ),
        Some(connection @ ConnectionConfig::BigQuery { .. }) => Destination::BigQuery(Box::new(
            connect_bigquery(connection, target.database.as_deref())?,
        )),
        Some(ConnectionConfig::S3 {
            bucket,
            region,
            endpoint,
        }) => Destination::Files(Box::new(FilesDestination {
            connection: target.connection.clone(),
            store: StoreKind::S3 {
                bucket: bucket.clone(),
                region: region.clone(),
                endpoint: endpoint.clone(),
            },
            max_footer_reads: target.footer_read_limit(),
        })),
        Some(ConnectionConfig::Filesystem { root }) => {
            Destination::Files(Box::new(FilesDestination {
                connection: target.connection.clone(),
                store: StoreKind::Filesystem { root: root.clone() },
                max_footer_reads: target.footer_read_limit(),
            }))
        }
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
    Ok(destination)
}

/// The Snowflake destination of target `name`, from its `snowflake` connection.
fn snowflake(
    connection: &ConnectionConfig,
    target: &crate::project::models::TargetConfig,
    name: &str,
) -> Result<crate::adapters::snowflake::models::SnowflakeDestination, LoaderError> {
    let ConnectionConfig::Snowflake {
        account,
        user,
        auth,
        role,
        warehouse,
        database,
        base_url,
        load_method,
    } = connection
    else {
        return Err(LoaderError::Invalid(format!(
            "target {name:?}: its connection is not a Snowflake one"
        )));
    };
    Ok(connect_snowflake(SnowflakeAccount {
        account: account.clone(),
        user: user.clone(),
        auth: auth.clone(),
        role: role.clone(),
        warehouse: warehouse.clone(),
        database: target.database.clone().unwrap_or_else(|| database.clone()),
        base_url: base_url.clone(),
        stage: forced_stage(
            load_method.as_deref().ok_or_else(|| {
                LoaderError::Invalid(format!(
                    "target {name:?}: its connection has no load_method"
                ))
            })?,
            TableName {
                database: Some(target.database.clone().unwrap_or_else(|| database.clone())),
                schema: target.loader_schema.clone().unwrap_or_default(),
                table: String::new(),
            },
        )
        .map_err(LoaderError::Invalid)?,
    })?)
}
