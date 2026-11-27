//! Helpers for the Google sign-in tests: credentials files of each kind, the fake's states, and signing in against a fresh fake.

use serde_json::{Value, json};
use sqlload::bigquery_api::main::run_query::run_query;
use sqlload::bigquery_api::models::BigQueryClient;
use sqlload::google_identity::main::google_credentials::google_credentials;
use sqlload_testkit::fake_bigquery::main::start_fake_bigquery::start_fake_bigquery;
use sqlload_testkit::fake_bigquery::models::FakeBigQuery;

use crate::helpers::run_async;
use crate::test_types::SignInTestCase;

/// The service account the tests impersonate.
const ACCOUNT: &str = "loader@fake-project.iam.gserviceaccount.com";
/// The workload identity provider the federated files name.
const PROVIDER: &str = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/sqlload-pool/providers/sqlload-provider";
/// What the fake records for a one-hour token impersonating the test account.
pub(crate) const IMPERSONATED: &str = "impersonate loader@fake-project.iam.gserviceaccount.com lifetime=3600s delegates=[] scope=[\"https://www.googleapis.com/auth/bigquery\"]";

/// No credentials file.
pub(crate) fn no_file(_: &str, _: &std::path::Path) -> Result<Value, String> {
    Ok(Value::Null)
}

/// A gcloud login's credentials file, renewed at the fake.
fn user(base_url: &str) -> Value {
    json!({"type": "authorized_user", "client_id": "gcloud-client", "client_secret": "client-secret", "refresh_token": "refresh-1", "token_uri": format!("{base_url}/token")})
}

/// A gcloud login.
pub(crate) fn user_file(base_url: &str, _: &std::path::Path) -> Result<Value, String> {
    Ok(user(base_url))
}

/// The fake's generateAccessToken URL for the test account.
fn impersonation_url(base_url: &str) -> String {
    format!("{base_url}/v1/projects/-/serviceAccounts/{ACCOUNT}:generateAccessToken")
}

/// An impersonated_service_account file whose source is a gcloud login.
pub(crate) fn impersonated_user(base_url: &str, _: &std::path::Path) -> Result<Value, String> {
    Ok(
        json!({"type": "impersonated_service_account", "service_account_impersonation_url": impersonation_url(base_url), "delegates": [], "source_credentials": user(base_url)}),
    )
}

/// A federated file whose subject token is a file, impersonating the test account.
pub(crate) fn token_file(base_url: &str, directory: &std::path::Path) -> Result<Value, String> {
    let path = directory.join("subject-token");
    std::fs::write(&path, "file-subject-token\n").map_err(|error| error.to_string())?;
    Ok(
        json!({"type": "external_account", "audience": PROVIDER, "subject_token_type": "urn:ietf:params:oauth:token-type:jwt", "token_url": format!("{base_url}/v1/token"), "credential_source": {"file": path}, "service_account_impersonation_url": impersonation_url(base_url)}),
    )
}

/// A federated file for Azure: the managed identity's token from IMDS.
pub(crate) fn azure(base_url: &str, _: &std::path::Path) -> Result<Value, String> {
    Ok(
        json!({"type": "external_account", "audience": PROVIDER, "subject_token_type": "urn:ietf:params:oauth:token-type:jwt", "token_url": format!("{base_url}/v1/token"), "credential_source": {"url": format!("{base_url}/metadata/identity/oauth2/token?api-version=2018-02-01&resource=api://sqlload-provider"), "headers": {"Metadata": "True"}, "format": {"type": "json", "subject_token_field_name": "access_token"}}, "service_account_impersonation_url": impersonation_url(base_url)}),
    )
}

/// A federated file whose subject token STS refuses.
pub(crate) fn refused_subject(
    base_url: &str,
    directory: &std::path::Path,
) -> Result<Value, String> {
    let mut file = token_file(base_url, directory)?;
    let path = directory.join("refused-token");
    std::fs::write(&path, "refused").map_err(|error| error.to_string())?;
    file["credential_source"]["file"] = json!(path);
    Ok(file)
}

/// A federated file whose subject token a command prints.
pub(crate) fn executable(base_url: &str, _: &std::path::Path) -> Result<Value, String> {
    Ok(
        json!({"type": "external_account", "audience": PROVIDER, "subject_token_type": "urn:ietf:params:oauth:token-type:jwt", "token_url": format!("{base_url}/v1/token"), "credential_source": {"executable": {"command": "/bin/echo token"}}}),
    )
}

/// A workforce pool file.
pub(crate) fn workforce(base_url: &str, directory: &std::path::Path) -> Result<Value, String> {
    let mut file = token_file(base_url, directory)?;
    file["workforce_pool_user_project"] = json!("fake-project");
    Ok(file)
}

/// Nowhere: the file is not written and nothing names it.
pub(crate) fn nowhere(_: &std::path::Path, _: &Value) -> Result<Option<String>, String> {
    Ok(None)
}

/// The file `GOOGLE_APPLICATION_CREDENTIALS` names.
pub(crate) fn named(root: &std::path::Path, file: &Value) -> Result<Option<String>, String> {
    let path = root.join("credentials.json");
    std::fs::write(&path, file.to_string()).map_err(|error| error.to_string())?;
    Ok(Some(path.display().to_string()))
}

/// gcloud's application default credentials file.
pub(crate) fn gcloud(root: &std::path::Path, file: &Value) -> Result<Option<String>, String> {
    let directory = root.join("home/.config/gcloud");
    std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    std::fs::write(
        directory.join("application_default_credentials.json"),
        file.to_string(),
    )
    .map_err(|error| error.to_string())?;
    Ok(None)
}

/// `GOOGLE_APPLICATION_CREDENTIALS` naming a file that is not there.
pub(crate) fn missing(root: &std::path::Path, _: &Value) -> Result<Option<String>, String> {
    Ok(Some(root.join("gone.json").display().to_string()))
}

/// The fake as it starts.
pub(crate) fn nothing(_: &FakeBigQuery) {}

/// Tokens lasting under the renewal margin.
pub(crate) fn short_tokens(fake: &FakeBigQuery) {
    fake.state.lock().token_lifetime_secs = Some(60);
}

/// Google refusing the login's refresh token.
pub(crate) fn refused_refresh(fake: &FakeBigQuery) {
    fake.state
        .lock()
        .refused_refresh_tokens
        .push("refresh-1".to_owned());
}

/// The test account refusing impersonation.
pub(crate) fn denied(fake: &FakeBigQuery) {
    fake.state.lock().denied_accounts.push(ACCOUNT.to_owned());
}

/// Sign in as `test_case` says against a fresh fake and run its queries: the outcome and the sign-in requests the fake saw.
pub(crate) fn sign_in(
    test_case: &SignInTestCase,
) -> Result<(Result<(), String>, Vec<String>), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let root = directory.path().to_owned();
    let (prepare, file, place, queries) = (
        test_case.prepare,
        test_case.file,
        test_case.place,
        test_case.queries,
    );
    let mut found = (Ok(()), Vec::new());
    let out = &mut found;
    run_async(|| async move {
        let fake = start_fake_bigquery().await?;
        prepare(&fake);
        let host = fake.base_url.trim_start_matches("http://").to_owned();
        let home = root.join("home");
        let mut variables = vec![
            ("HOME", home.display().to_string()),
            ("APPDATA", home.join(".config").display().to_string()),
            (
                "GCE_METADATA_HOST",
                test_case.metadata_host.map_or(host, str::to_owned),
            ),
        ];
        variables.extend(
            place(&root, &file(&fake.base_url, &root)?)?
                .map(|path| ("GOOGLE_APPLICATION_CREDENTIALS", path)),
        );
        let variable = |name: &str| {
            variables
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| value.clone())
        };
        let outcome = async {
            let credentials = google_credentials(None, None, &variable)?;
            let client = BigQueryClient::new(
                sqlload::http::main::http_client::build_http_client(
                    &sqlload::http::models::ConnectionCounter::default(),
                )
                .map_err(|error| error.to_string())?,
                ("fake-project", "US"),
                credentials,
                (Some(&fake.base_url), Some(&fake.storage_url)),
            );
            for _ in 0..queries {
                run_query(&client, "SELECT 1")
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok::<(), String>(())
        }
        .await;
        *out = (outcome, fake.state.lock().sign_ins.clone());
        Ok(())
    })?;
    Ok(found)
}
