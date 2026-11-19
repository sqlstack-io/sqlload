//! The GCS client SQLLoad reads a bucket with, and where it gets its credentials: the connection's key, else Google's application default credentials.

use object_store::ClientOptions;
use object_store::gcp::GoogleCloudStorageBuilder;

/// Where a GCS client's credentials come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GcsCredentials {
    /// The connection's `service_account_key`.
    Connection,
    /// The file `GOOGLE_APPLICATION_CREDENTIALS` names.
    Named { path: String },
    /// The gcloud CLI's application default credentials file.
    Gcloud { path: String },
    /// Neither: the service account of the machine SQLLoad runs on, from its metadata server.
    Machine,
}

impl GcsCredentials {
    /// Where the credentials come from, for `sqlload debug`; never the credentials themselves.
    pub(crate) fn describe(&self) -> String {
        match self {
            Self::Connection => "the connection's service_account_key".to_owned(),
            Self::Named { path } => format!("GOOGLE_APPLICATION_CREDENTIALS ({path})"),
            Self::Gcloud { path } => format!("gcloud's application default credentials ({path})"),
            Self::Machine => "the machine's service account, from its metadata server".to_owned(),
        }
    }
}

/// Where a GCS client's credentials come from, with `variable` reading the environment: the connection's key when it `has_key`; else the file `GOOGLE_APPLICATION_CREDENTIALS` names, which must exist; else gcloud's application default credentials file (`~/.config/gcloud/application_default_credentials.json`, or `%APPDATA%\gcloud\…` on Windows) when it exists; else the machine's service account.
pub(crate) fn gcs_credentials(
    has_key: bool,
    variable: &dyn Fn(&str) -> Option<String>,
) -> Result<GcsCredentials, String> {
    if has_key {
        return Ok(GcsCredentials::Connection);
    }
    if let Some(path) = variable("GOOGLE_APPLICATION_CREDENTIALS") {
        return match std::path::Path::new(&path).is_file() {
            true => Ok(GcsCredentials::Named { path }),
            false => Err(format!(
                "GOOGLE_APPLICATION_CREDENTIALS names {path}, which is not a file"
            )),
        };
    }
    let (home, below) = match cfg!(windows) {
        true => ("APPDATA", "gcloud/application_default_credentials.json"),
        false => (
            "HOME",
            ".config/gcloud/application_default_credentials.json",
        ),
    };
    Ok(variable(home)
        .map(|home| std::path::Path::new(&home).join(below))
        .filter(|path| path.is_file())
        .map_or(GcsCredentials::Machine, |path| GcsCredentials::Gcloud {
            path: path.display().to_string(),
        }))
}

/// The GCS client builder for `bucket`, signed in with the service-account key's JSON text `key`, else Google's application default credentials as [`gcs_credentials`] finds them with `variable` (the file `GOOGLE_APPLICATION_CREDENTIALS` names, gcloud's, else the machine's service account); the error when that variable names no file, rather than signing in as someone else. `endpoint` replaces Google's, as for an emulator.
pub(crate) fn gcs_builder(
    bucket: &str,
    key: Option<&str>,
    endpoint: Option<&str>,
    variable: &dyn Fn(&str) -> Option<String>,
) -> Result<GoogleCloudStorageBuilder, String> {
    let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
    match (key, gcs_credentials(key.is_some(), variable)?) {
        (Some(key), _) => builder = builder.with_service_account_key(key),
        (None, GcsCredentials::Named { path }) => {
            builder = builder.with_application_credentials(path);
        }
        (None, _) => {}
    }
    if let Some(endpoint) = endpoint {
        builder = builder.with_base_url(endpoint).with_client_options(
            ClientOptions::new().with_allow_http(endpoint.starts_with("http://")),
        );
    }
    Ok(builder)
}
