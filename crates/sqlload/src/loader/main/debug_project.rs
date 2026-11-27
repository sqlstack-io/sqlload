//! `sqlload debug`: check a loader project's configuration and connections.

use crate::adapters::dispatch::main::session::check_destination::check_destination;
use crate::adapters::dispatch::main::session::connect_destination::connect_destination;
use crate::auth::models::Secret;
use crate::file_store::main::gcs_key_source::gcs_key_source;
use crate::file_store::main::s3_key_source::s3_key_source;
use crate::google_identity::main::environment_credentials::environment_credentials;
use crate::http_source::main::http_credential_source::http_credential_source;
use crate::loader::main::_check_source::check_source;
use crate::project::models::{ConnectionConfig, ProjectConfig};

/// One line per check (`ok`, `warn` or `fail`), and whether nothing failed: where each S3 connection's keys, how each BigQuery and GCS connection signs in and each HTTP connection's credentials come from, then each target and source; `connect = false` skips them.
pub async fn debug_project(project: &ProjectConfig, connect: bool) -> (Vec<String>, bool) {
    let mut lines = vec![format!(
        "ok    config  {} connections, {} targets, {} sources",
        project.connections.len(),
        project.targets.len(),
        project.sources.len()
    )];
    let used: Vec<&String> = project
        .targets
        .values()
        .map(|target| &target.connection)
        .chain(project.sources.values().map(|source| &source.connection))
        .collect();
    lines.extend(
        project
            .connections
            .keys()
            .filter(|name| !used.contains(name))
            .map(|name| format!("warn  connection {name}  not used by any target or source")),
    );
    if !connect {
        return (lines, true);
    }
    let (connection_lines, mut ok) = connection_lines(project);
    lines.extend(connection_lines);
    for name in project.targets.keys() {
        let checked = match connect_destination(project, name).await {
            Ok(destination) => check_destination(destination).await,
            Err(error) => Err(error),
        };
        let line = match checked {
            Ok(answer) => format!("ok    target {name}  {answer}"),
            Err(error) => {
                ok = false;
                format!("fail  target {name}  {error}")
            }
        };
        lines.push(line);
    }
    for (name, source) in &project.sources {
        let line = match check_source(project, source).await {
            Ok(answer) => format!("ok    source {name}  {answer}"),
            Err(error) => {
                ok = false;
                format!("fail  source {name}  {error}")
            }
        };
        lines.push(line);
    }
    (lines, ok)
}

/// One line per connection whose credentials SQLLoad finds itself: where each S3 connection's keys come from, how each BigQuery and GCS connection signs in, and where each HTTP connection's credentials come from; and whether every one was usable.
fn connection_lines(project: &ProjectConfig) -> (Vec<String>, bool) {
    let mut lines: Vec<String> = Vec::new();
    let mut ok = true;
    for (name, connection) in &project.connections {
        let found = match connection {
            ConnectionConfig::S3 { profile, .. } => {
                s3_key_source(profile.as_deref()).map(|source| format!("S3 keys from {source}"))
            }
            ConnectionConfig::Gcs {
                service_account_key,
                ..
            } => gcs_key_source(service_account_key.is_some())
                .map(|source| format!("GCS signs in with {source}")),
            ConnectionConfig::BigQuery {
                service_account_key,
                impersonate_service_account,
                ..
            } => environment_credentials(
                service_account_key.as_ref(),
                impersonate_service_account.as_deref(),
            )
            .map(|credentials| format!("BigQuery signs in with {}", credentials.describe())),
            ConnectionConfig::Http {
                auth,
                token,
                username,
                password,
                client_id,
                client_secret,
                refresh_token,
                ..
            } => {
                let secret =
                    |value: &Option<Secret>| value.as_ref().is_some_and(|it| !it.0.is_empty());
                let text = |value: &Option<String>| value.as_ref().is_some_and(|it| !it.is_empty());
                let set = [
                    ("token", secret(token)),
                    ("username", text(username)),
                    ("password", secret(password)),
                    ("client_id", text(client_id)),
                    ("client_secret", secret(client_secret)),
                    ("refresh_token", secret(refresh_token)),
                ];
                http_credential_source(auth.as_ref(), &set)
            }
            _ => continue,
        };
        lines.push(match found {
            Ok(source) => format!("ok    connection {name}  {source}"),
            Err(error) => {
                ok = false;
                format!("fail  connection {name}  {error}")
            }
        });
    }
    (lines, ok)
}
