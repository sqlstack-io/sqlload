//! Open the destination one target uses.

#[cfg(feature = "duckdb")]
use crate::duckdb_destination::main::open_duckdb::open_duckdb;
use crate::files_destination::models::FilesDestination;
use crate::loader::errors::LoaderError;
use crate::loader::models::Destination;
use crate::project::models::{ConnectionConfig, ProjectConfig, StageConfig};
use crate::snowflake_destination::main::connect_snowflake::connect_snowflake;
use crate::snowflake_destination::models::{
    ExternalStage, ForcedStage, SnowflakeAccount, StageObject,
};
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};

/// The route a Snowflake connection's `stage` setting forces.
fn forced_stage(stage: &StageConfig) -> Result<ForcedStage, String> {
    let (url, integration, name, store) = match stage {
        StageConfig::User => return Ok(ForcedStage::User),
        StageConfig::Table => return Ok(ForcedStage::Table),
        StageConfig::Streaming => return Ok(ForcedStage::Streaming),
        StageConfig::Insert => return Ok(ForcedStage::Insert),
        StageConfig::Internal { name } => {
            return Ok(ForcedStage::Internal { name: name.clone() });
        }
        StageConfig::S3 {
            url,
            storage_integration,
            name,
            region,
            endpoint,
            ..
        } => (url, storage_integration, name, (region, endpoint)),
    };
    let location = resolve_location(None, url).map_err(|error| error.to_string())?;
    let StoreKind::S3 { bucket, .. } = location.store else {
        return Err(format!("stage url {url:?} must be an s3:// URL"));
    };
    let object = match (integration, name) {
        (Some(integration), None) => StageObject::Managed {
            integration: integration.clone(),
        },
        (None, Some(name)) => StageObject::Existing { name: name.clone() },
        _ => return Err("stage needs exactly one of storage_integration or name".to_owned()),
    };
    let keep_staged_files = matches!(
        stage,
        StageConfig::S3 {
            keep_staged_files: true,
            ..
        }
    );
    Ok(ForcedStage::External(ExternalStage {
        location: Location {
            store: StoreKind::S3 {
                bucket,
                region: store.0.clone(),
                endpoint: store.1.clone(),
            },
            prefix: location.prefix,
        },
        object,
        keep_staged_files,
    }))
}

/// The open destination of target `name`.
pub(in crate::loader) async fn connect_target(
    project: &ProjectConfig,
    name: &str,
) -> Result<Destination, LoaderError> {
    let target = project
        .targets
        .get(name)
        .ok_or_else(|| LoaderError::Invalid(format!("target {name:?} does not exist")))?;
    let destination = match project.connections.get(&target.connection) {
        #[cfg(feature = "duckdb")]
        Some(ConnectionConfig::DuckDb { path }) => Destination::DuckDb(open_duckdb(path)?),
        #[cfg(not(feature = "duckdb"))]
        Some(ConnectionConfig::DuckDb { .. }) => {
            return Err(LoaderError::Invalid(format!(
                "target {name:?}: this sqlload was built without DuckDB (cargo feature \"duckdb\")"
            )));
        }
        Some(ConnectionConfig::Snowflake {
            account,
            user,
            auth,
            role,
            warehouse,
            database,
            base_url,
            stage,
        }) => Destination::Snowflake(Box::new(connect_snowflake(SnowflakeAccount {
            account: account.clone(),
            user: user.clone(),
            auth: auth.clone(),
            role: role.clone(),
            warehouse: warehouse.clone(),
            database: target.database.clone().unwrap_or_else(|| database.clone()),
            base_url: base_url.clone(),
            stage: stage
                .as_ref()
                .map(forced_stage)
                .transpose()
                .map_err(LoaderError::Invalid)?,
        })?)),
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
