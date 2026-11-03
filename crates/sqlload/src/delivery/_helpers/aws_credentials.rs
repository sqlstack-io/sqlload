use crate::auth::models::Secret;
use crate::stage_api::models::AwsCredentials;

/// AWS keys from `AWS_ACCESS_KEY_ID` and friends, else the shared credentials file (`AWS_SHARED_CREDENTIALS_FILE`, default `~/.aws/credentials`) under `AWS_PROFILE` or `default`.
pub(crate) fn load_credentials() -> Result<AwsCredentials, String> {
    let variable = environment_variable;
    if let (Some(key_id), Some(secret)) = (
        variable("AWS_ACCESS_KEY_ID"),
        variable("AWS_SECRET_ACCESS_KEY"),
    ) {
        return Ok(AwsCredentials {
            key_id,
            secret: Secret(secret),
            session_token: variable("AWS_SESSION_TOKEN").map(Secret),
        });
    }
    let path = variable("AWS_SHARED_CREDENTIALS_FILE")
        .or_else(|| variable("HOME").map(|home| format!("{home}/.aws/credentials")))
        .ok_or("no AWS credentials: set AWS_ACCESS_KEY_ID or AWS_SHARED_CREDENTIALS_FILE")?;
    let profile = variable("AWS_PROFILE").unwrap_or_else(|| "default".to_owned());
    let text = std::fs::read_to_string(&path).map_err(|error| {
        format!("no AWS keys in the environment and cannot read {path}: {error}")
    })?;
    profile_credentials(&text, &profile)
        .ok_or_else(|| format!("{path} has no keys for profile {profile:?}"))
}

/// The keys of `[profile]` in an AWS credentials file.
pub(crate) fn profile_credentials(text: &str, profile: &str) -> Option<AwsCredentials> {
    let mut in_profile = false;
    let mut key_id: Option<String> = None;
    let mut secret: Option<String> = None;
    let mut session_token: Option<String> = None;
    for line in text.lines().map(str::trim) {
        if let Some(section) = line
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
        {
            in_profile = section.trim() == profile;
            continue;
        }
        let Some((name, value)) = line.split_once('=').filter(|_| in_profile) else {
            continue;
        };
        let value = Some(value.trim().to_owned()).filter(|value| !value.is_empty());
        match name.trim() {
            "aws_access_key_id" => key_id = value,
            "aws_secret_access_key" => secret = value,
            "aws_session_token" => session_token = value,
            _ => {}
        }
    }
    Some(AwsCredentials {
        key_id: key_id?,
        secret: Secret(secret?),
        session_token: session_token.map(Secret),
    })
}

fn environment_variable(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        Ok(_) | Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => None,
    }
}
