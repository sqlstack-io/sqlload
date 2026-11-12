use crate::store_paths::models::StoreKind;

/// An S3 bucket in eu-west-2.
pub(crate) fn bucket(name: &str) -> Option<StoreKind> {
    Some(StoreKind::S3 {
        bucket: name.to_owned(),
        region: Some("eu-west-2".to_owned()),
        endpoint: None,
        profile: None,
    })
}

/// The root and folders of `path` as `local_root` finds them, written `root|folders`.
pub(crate) fn root_of(path: &str, windows: bool) -> Option<String> {
    let separated = crate::store_paths::_helpers::local_root::windows_separators(path, windows);
    crate::store_paths::_helpers::local_root::local_root(&separated, windows)
        .map(|(root, folders)| format!("{root}|{folders}"))
}
