use crate::store_paths::models::StoreKind;

/// An S3 bucket in eu-west-2.
pub(crate) fn bucket(name: &str) -> Option<StoreKind> {
    Some(StoreKind::S3 {
        bucket: name.to_owned(),
        region: Some("eu-west-2".to_owned()),
        endpoint: None,
    })
}
