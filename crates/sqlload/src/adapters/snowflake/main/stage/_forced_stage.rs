//! The route a Snowflake connection's `stage` setting forces.

use crate::adapters::contract::models::TableName;
use crate::adapters::snowflake::models::{ExternalStage, ForcedStage, StageObject};
use crate::project::models::StageConfig;
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};

/// The route a Snowflake connection's `stage` setting forces.
pub(in crate::adapters) fn forced_stage(
    stage: &StageConfig,
    home: TableName,
) -> Result<ForcedStage, String> {
    let (url, integration, name, store) = match stage {
        StageConfig::User {} => return Ok(ForcedStage::User),
        StageConfig::Table {} => return Ok(ForcedStage::Table),
        StageConfig::Streaming {} => return Ok(ForcedStage::Streaming),
        StageConfig::Insert {} => return Ok(ForcedStage::Insert),
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
        home,
        keep_staged_files,
    }))
}
