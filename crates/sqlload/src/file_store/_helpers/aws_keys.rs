//! Where SQLLoad's S3 clients get their keys: the environment, the AWS shared credentials file, or the rest of the chain.

use crate::auth::models::Secret;
use crate::stage_api::models::AwsCredentials;

/// Where an S3 client's keys come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeySource {
    /// `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
    Environment,
    /// A profile of the shared credentials file.
    File {
        path: String,
        profile: String,
        keys: AwsCredentials,
    },
    /// Neither: web identity, then the instance or container role.
    Chain,
}

impl KeySource {
    /// Where the keys come from, for `sqlload debug`; never the keys themselves.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Environment => "AWS_ACCESS_KEY_ID in the environment".to_owned(),
            Self::File { path, profile, .. } => format!("{path}, profile {profile}"),
            Self::Chain => "web identity or the instance or container role".to_owned(),
        }
    }
}

/// Where keys for an S3 client come from, with `variable` reading the environment: the environment's keys; else the shared credentials file (`AWS_SHARED_CREDENTIALS_FILE`, default `~/.aws/credentials`) under `profile` (a connection's), else `AWS_PROFILE`, else `default`; else the rest of the chain. A profile that is named but cannot be read fails rather than falling through.
pub(crate) fn key_source(
    profile: Option<&str>,
    variable: &dyn Fn(&str) -> Option<String>,
) -> Result<KeySource, String> {
    if variable("AWS_ACCESS_KEY_ID").is_some() && variable("AWS_SECRET_ACCESS_KEY").is_some() {
        return Ok(KeySource::Environment);
    }
    let named = profile
        .map(str::to_owned)
        .or_else(|| variable("AWS_PROFILE"));
    let profile = named.clone().unwrap_or_else(|| "default".to_owned());
    let Some(path) = variable("AWS_SHARED_CREDENTIALS_FILE")
        .or_else(|| variable("HOME").map(|home| format!("{home}/.aws/credentials")))
    else {
        return match named {
            Some(_) => Err(format!(
                "AWS profile {profile} is named, but no shared credentials file is: set AWS_SHARED_CREDENTIALS_FILE or HOME"
            )),
            None => Ok(KeySource::Chain),
        };
    };
    let text = match (std::fs::read_to_string(&path), &named) {
        (Ok(text), _) => text,
        (Err(error), Some(_)) => {
            return Err(format!(
                "cannot read {path} for AWS profile {profile}: {error}"
            ));
        }
        (Err(_), None) => return Ok(KeySource::Chain),
    };
    match (profile_credentials(&text, &profile), named) {
        (Some(keys), _) => Ok(KeySource::File {
            path,
            profile,
            keys,
        }),
        (None, Some(_)) => Err(format!(
            "{path} has no aws_access_key_id and aws_secret_access_key for AWS profile {profile}; SQLLoad reads only keys from that file (SSO, assumed roles and credential_process in ~/.aws/config aren't supported)"
        )),
        (None, None) => Ok(KeySource::Chain),
    }
}

/// The keys of `[profile]` in an AWS shared credentials file.
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

/// `name`'s value in the process environment, when set and not empty.
pub(crate) fn environment_variable(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        Ok(_) | Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => None,
    }
}
