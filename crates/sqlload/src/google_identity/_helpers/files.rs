//! Google credentials files, read by their `type`.

use serde_json::Value;

use crate::auth::models::Secret;
use crate::google_identity::constants::{
    AWS_ENVIRONMENT, EXECUTABLE_TIMEOUT_MS, IMPERSONATED_LIFETIME_SECS, TOKEN_URL,
};
use crate::google_identity::models::{
    AuthorizedUser, AwsKeys, AwsSource, ExternalAccount, Impersonation, ServiceAccountKey, Signer,
    SubjectSource,
};

/// The signer a credentials file's JSON `text` holds and the service account it impersonates, if any; `what` names the file in errors. A type SQLLoad cannot sign in with is an error naming it.
pub(crate) fn credentials_file(
    text: &str,
    what: &str,
    variable: &dyn Fn(&str) -> Option<String>,
) -> Result<(Signer, Option<Impersonation>), String> {
    let file: Value = serde_json::from_str(text)
        .map_err(|error| format!("{what} is not a Google credentials file's JSON: {error}"))?;
    let kind = file["type"].as_str().unwrap_or_default();
    match kind {
        "service_account" => Ok((Signer::ServiceAccount(key_fields(&file, what)?), None)),
        "authorized_user" => Ok((Signer::User(user_fields(&file, what)?), None)),
        "external_account" => external_account(&file, what, variable),
        "impersonated_service_account" => {
            let url = text_field(&file, "service_account_impersonation_url", what)?;
            let source = &file["source_credentials"];
            let signer = match source["type"].as_str().unwrap_or_default() {
                "service_account" => Signer::ServiceAccount(key_fields(source, what)?),
                "authorized_user" => Signer::User(user_fields(source, what)?),
                "external_account" => external_account(source, what, variable)?.0,
                other => {
                    return Err(format!(
                        "{what} impersonates a service account from source_credentials of type {other:?}; SQLLoad signs in with service_account, authorized_user or external_account source credentials"
                    ));
                }
            };
            let delegates: Vec<String> = file["delegates"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(delegate_account)
                .collect();
            Ok((signer, Some(impersonation(&url, delegates, None, what)?)))
        }
        "" => Err(format!("{what} has no type")),
        other => Err(format!(
            "{what} is a {other:?} credentials file, which SQLLoad cannot sign in with; it takes service_account, authorized_user, impersonated_service_account and external_account files"
        )),
    }
}

/// A service-account key file's fields.
pub(crate) fn key_fields(file: &Value, what: &str) -> Result<ServiceAccountKey, String> {
    Ok(ServiceAccountKey {
        client_email: text_field(file, "client_email", what)?,
        private_key: Secret(text_field(file, "private_key", what)?),
        token_uri: text_field(file, "token_uri", what)?,
        project_id: text_field(file, "project_id", what)?,
    })
}

/// A gcloud login's fields.
fn user_fields(file: &Value, what: &str) -> Result<AuthorizedUser, String> {
    Ok(AuthorizedUser {
        client_id: text_field(file, "client_id", what)?,
        client_secret: Secret(text_field(file, "client_secret", what)?),
        refresh_token: Secret(text_field(file, "refresh_token", what)?),
        token_uri: file["token_uri"].as_str().unwrap_or(TOKEN_URL).to_owned(),
        quota_project: optional_text(file, "quota_project_id"),
    })
}

/// An `external_account` file's signer, and the service account it impersonates when it names one.
fn external_account(
    file: &Value,
    what: &str,
    variable: &dyn Fn(&str) -> Option<String>,
) -> Result<(Signer, Option<Impersonation>), String> {
    if file["workforce_pool_user_project"].is_string() {
        return Err(format!(
            "{what} signs in through a workforce identity pool, which SQLLoad does not support; use a workload identity pool"
        ));
    }
    let source = subject_source(&file["credential_source"], what, variable)?;
    let account = ExternalAccount {
        audience: text_field(file, "audience", what)?,
        subject_token_type: text_field(file, "subject_token_type", what)?,
        token_url: text_field(file, "token_url", what)?,
        source,
        quota_project: optional_text(file, "quota_project_id"),
    };
    let impersonate = match optional_text(file, "service_account_impersonation_url") {
        Some(url) => {
            let lifetime = file["service_account_impersonation"]["token_lifetime_seconds"].as_u64();
            Some(impersonation(&url, Vec::new(), lifetime, what)?)
        }
        None => None,
    };
    Ok((Signer::External(Box::new(account)), impersonate))
}

/// A `credential_source`: a file, a URL, an executable, or AWS (`environment_id` `aws1`).
fn subject_source(
    source: &Value,
    what: &str,
    variable: &dyn Fn(&str) -> Option<String>,
) -> Result<SubjectSource, String> {
    let field = match source["format"]["type"].as_str() {
        Some("json") => Some(
            source["format"]["subject_token_field_name"]
                .as_str()
                .ok_or_else(|| {
                    format!("{what}: credential_source.format of type json names no subject_token_field_name")
                })?
                .to_owned(),
        ),
        Some("text") | None => None,
        Some(other) => {
            return Err(format!(
                "{what}: credential_source.format type {other:?} is neither text nor json"
            ));
        }
    };
    if let Some(environment) = source["environment_id"].as_str() {
        if !environment.starts_with("aws") {
            return Err(format!(
                "{what}: credential_source.environment_id {environment:?} is not one SQLLoad knows (aws1)"
            ));
        }
        if environment != AWS_ENVIRONMENT {
            return Err(format!(
                "{what}: credential_source.environment_id {environment:?} is a version of AWS's that SQLLoad does not know; it takes aws1"
            ));
        }
        return Ok(SubjectSource::Aws(AwsSource {
            region_url: optional_text(source, "region_url"),
            url: optional_text(source, "url"),
            verification_url: text_field(source, "regional_cred_verification_url", what)?,
            session_url: optional_text(source, "imdsv2_session_token_url"),
            region: variable("AWS_REGION").or_else(|| variable("AWS_DEFAULT_REGION")),
            keys: variable("AWS_ACCESS_KEY_ID")
                .zip(variable("AWS_SECRET_ACCESS_KEY"))
                .map(|(access_key_id, secret)| AwsKeys {
                    access_key_id,
                    secret_access_key: Secret(secret),
                    session_token: variable("AWS_SESSION_TOKEN").map(Secret),
                }),
        }));
    }
    if let Some(path) = optional_text(source, "file") {
        return Ok(SubjectSource::File { path, field });
    }
    if let Some(url) = optional_text(source, "url") {
        let headers: Vec<(String, String)> = source["headers"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(name, value)| (name.clone(), value.as_str().unwrap_or_default().to_owned()))
            .collect();
        return Ok(SubjectSource::Url {
            url,
            headers,
            field,
        });
    }
    if source["executable"].is_object() {
        let executable = &source["executable"];
        return Ok(SubjectSource::Executable {
            command: text_field(executable, "command", what)?,
            timeout_ms: executable["timeout_millis"]
                .as_u64()
                .unwrap_or(EXECUTABLE_TIMEOUT_MS),
            output_file: optional_text(executable, "output_file"),
        });
    }
    Err(format!(
        "{what}: credential_source names no file, url, executable or environment_id"
    ))
}

/// The service account a `generateAccessToken` URL names, with the token lifetime asked for.
pub(crate) fn impersonation(
    url: &str,
    delegates: Vec<String>,
    lifetime_secs: Option<u64>,
    what: &str,
) -> Result<Impersonation, String> {
    let account = url
        .rsplit_once("/serviceAccounts/")
        .and_then(|(_, rest)| rest.strip_suffix(":generateAccessToken"))
        .filter(|account| !account.is_empty())
        .ok_or_else(|| {
            format!(
                "{what}: service_account_impersonation_url {url:?} is not an IAM Credentials generateAccessToken URL"
            )
        })?;
    Ok(Impersonation {
        account: account.to_owned(),
        url: url.to_owned(),
        delegates,
        lifetime_secs: lifetime_secs.unwrap_or(IMPERSONATED_LIFETIME_SECS),
    })
}

/// A delegate's email, from `projects/-/serviceAccounts/EMAIL` or the email itself.
fn delegate_account(delegate: &str) -> String {
    delegate
        .rsplit_once("/serviceAccounts/")
        .map_or(delegate, |(_, account)| account)
        .to_owned()
}

/// A field's non-empty text, or an error naming the field and the file.
fn text_field(file: &Value, name: &str, what: &str) -> Result<String, String> {
    optional_text(file, name).ok_or_else(|| format!("{what} has no {name}"))
}

/// A field's text when it is set and not empty.
fn optional_text(file: &Value, name: &str) -> Option<String> {
    file[name]
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}
