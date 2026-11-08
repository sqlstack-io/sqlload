//! The route a Snowflake connection's `load_method` sets.

use crate::adapters::contract::models::TableName;
use crate::adapters::snowflake::models::{ExternalStage, ForcedStage, StageObject};
use crate::project::models::LoadMethod;
use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::{Location, StoreKind};

/// The route a Snowflake connection's `load_method` sets.
pub(in crate::adapters) fn forced_stage(
    stage: &LoadMethod,
    home: TableName,
) -> Result<ForcedStage, String> {
    let (url, integration, name, store) = match stage {
        LoadMethod::InternalUserStage {} => return Ok(ForcedStage::User),
        LoadMethod::InternalTableStage {} => return Ok(ForcedStage::Table),
        LoadMethod::Streaming {} => return Ok(ForcedStage::Streaming),
        LoadMethod::Insert {} => return Ok(ForcedStage::Insert),
        LoadMethod::InternalNamedStage { name } => {
            return Ok(ForcedStage::Internal { name: name.clone() });
        }
        LoadMethod::ExternalStage {
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
        return Err(format!("load_method url {url:?} must be an s3:// URL"));
    };
    let object = match (integration, name) {
        (Some(integration), None) => StageObject::Managed {
            integration: integration.clone(),
        },
        (None, Some(name)) => StageObject::Existing { name: name.clone() },
        _ => return Err("load_method needs exactly one of storage_integration or name".to_owned()),
    };
    let keep_staged_files = matches!(
        stage,
        LoadMethod::ExternalStage {
            keep_staged_files: true,
            ..
        }
    );
    Ok(ForcedStage::External(Box::new(ExternalStage {
        location: Location {
            store: StoreKind::S3 {
                bucket,
                region: store.0.clone(),
                endpoint: store.1.clone(),
                profile: None,
            },
            prefix: location.prefix,
        },
        object,
        home,
        keep_staged_files,
    })))
}
