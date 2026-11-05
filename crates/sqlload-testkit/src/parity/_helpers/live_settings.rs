use crate::parity::constants::ALLOWED_PREFIX;
use crate::parity::models::Live;

fn required(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|_| format!("{name} is not set"))
}

/// The live settings; `Ok(None)` when `SQLLOAD_PARITY_ACCOUNT` is unset, which skips the test.
pub(crate) fn live_settings() -> Result<Option<Live>, String> {
    let Ok(account) = std::env::var("SQLLOAD_PARITY_ACCOUNT") else {
        return Ok(None);
    };
    let auth = match std::env::var("SQLLOAD_PARITY_PRIVATE_KEY_FILE") {
        Ok(file) => format!("{{ method = \"key_pair\", private_key_file = \"{file}\" }}"),
        Err(_) => format!(
            "{{ method = \"pat\", token = \"{}\" }}",
            required("SQLLOAD_PARITY_TOKEN")?
        ),
    };
    let url = required("SQLLOAD_PARITY_STAGE_URL")?;
    let (bucket, prefix) = url
        .strip_prefix("s3://")
        .and_then(|rest| rest.split_once('/'))
        .ok_or_else(|| format!("SQLLOAD_PARITY_STAGE_URL {url:?} must be s3://bucket/prefix/"))?;
    if prefix != ALLOWED_PREFIX {
        return Err(format!(
            "SQLLOAD_PARITY_STAGE_URL must end in /sqlload/, the only place this test may write, not {prefix:?}"
        ));
    }
    Ok(Some(Live {
        account,
        user: required("SQLLOAD_PARITY_USER")?,
        auth,
        role: required("SQLLOAD_PARITY_ROLE")?,
        warehouse: required("SQLLOAD_PARITY_WAREHOUSE")?,
        database: required("SQLLOAD_PARITY_DATABASE")?,
        schema: required("SQLLOAD_PARITY_SCHEMA")?,
        bucket: bucket.to_owned(),
        prefix: prefix.to_owned(),
        region: required("SQLLOAD_PARITY_REGION")?,
        integration: required("SQLLOAD_PARITY_STORAGE_INTEGRATION")?,
    }))
}
