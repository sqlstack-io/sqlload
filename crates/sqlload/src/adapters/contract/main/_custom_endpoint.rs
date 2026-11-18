//! Whether an S3 store points at a custom endpoint, which a warehouse reading S3 itself would not follow.

use crate::store_paths::models::StoreKind;

/// Why an S3 store points somewhere a warehouse reading S3 itself would not read: an endpoint in the connection or in `AWS_ENDPOINT` / `AWS_ENDPOINT_URL` (each as `variable` reads it from the environment, unset when empty, as SQLLoad's reader reads them), which SQLLoad's reader follows.
pub(in crate::adapters) fn custom_endpoint(
    store: &StoreKind,
    variable: &dyn Fn(&str) -> Option<String>,
) -> Option<String> {
    let from_env = ["AWS_ENDPOINT", "AWS_ENDPOINT_URL"]
        .iter()
        .any(|name| variable(name).is_some_and(|value| !value.is_empty()));
    match store {
        StoreKind::S3 {
            endpoint: Some(_), ..
        } => Some("the S3 connection sets a custom endpoint".to_owned()),
        StoreKind::S3 { .. } if from_env => {
            Some("AWS_ENDPOINT or AWS_ENDPOINT_URL sets a custom S3 endpoint".to_owned())
        }
        _ => None,
    }
}
