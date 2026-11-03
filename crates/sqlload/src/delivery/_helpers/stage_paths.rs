use std::time::Duration;

use crate::config::models::Config;
use crate::delivery::_helpers::aws_credentials::load_credentials;
use crate::stage_api::models::ObjectTarget;
use crate::time_text::main::iso_utc::iso_utc;
use crate::time_text::main::utc_date::utc_date;

const DAY_MS: i64 = 86_400_000;

/// Dated folder names older than `retain`, newest first, looking back at most `lookback` beyond it.
pub(crate) fn expired_dates(now_ms: i64, retain: Duration, lookback: Duration) -> Vec<String> {
    let retain_days = (retain.as_millis() as u64).div_ceil(DAY_MS as u64).max(1) as i64;
    let lookback_days = (lookback.as_millis() as i64) / DAY_MS;
    (retain_days + 1..=retain_days + lookback_days)
        .map(|days| utc_date(now_ms - days * DAY_MS))
        .collect()
}

/// The values an `s3` folder template is filled with.
pub(crate) struct FolderValues<'a> {
    pub(crate) topic: &'a str,
    pub(crate) table: &'a str,
    pub(crate) stream: &'a str,
    pub(crate) now_ms: i64,
}

/// The folder for one file under the bucket prefix, always ending in `/`.
pub(crate) fn render_folder(template: &str, values: &FolderValues<'_>) -> String {
    let timestamp = iso_utc(values.now_ms);
    let mut folder = template
        .replace("{topic}", values.topic)
        .replace("{table}", values.table)
        .replace("{stream}", values.stream)
        .replace("{date}", &timestamp[..10])
        .replace("{hour}", &timestamp[11..13])
        .trim_start_matches('/')
        .to_owned();
    if !folder.ends_with('/') {
        folder.push('/');
    }
    folder
}

/// The bucket location in `s3` mode, with credentials from the environment or the shared AWS credentials file.
pub(crate) fn s3_target(config: &Config) -> Result<ObjectTarget, String> {
    let s3 = config
        .staging
        .s3
        .as_ref()
        .ok_or("staging.kind = \"s3\" needs a [staging.s3] section")?;
    let mut prefix = s3.prefix.trim_matches('/').to_owned();
    if !prefix.is_empty() {
        prefix.push('/');
    }
    Ok(ObjectTarget {
        endpoint: s3
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", s3.region)),
        bucket: s3.bucket.clone(),
        path_style: s3.path_style,
        prefix,
        region: s3.region.clone(),
        credentials: load_credentials()?,
    })
}
