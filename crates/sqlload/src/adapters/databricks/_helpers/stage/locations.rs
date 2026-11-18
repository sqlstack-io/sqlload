//! How SQLLoad compares folders with Databricks' external locations.

/// An external location's `url` as SQLLoad compares folders with it: `s3://` for the `s3a://` and `s3n://` it may be stored as, ending in `/` so that it only covers what lies below it.
pub(crate) fn location_url(url: &str) -> String {
    let url = url.trim_end_matches('/');
    let s3 = ["s3a://", "s3n://"]
        .iter()
        .find_map(|scheme| url.strip_prefix(scheme));
    match s3 {
        Some(rest) => format!("s3://{rest}/"),
        None => format!("{url}/"),
    }
}
