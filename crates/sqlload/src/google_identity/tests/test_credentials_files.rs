use serde_json::json;

use crate::auth::models::Secret;
use crate::google_identity::main::google_credentials::google_credentials;
use crate::google_identity::tests::test_types::CredentialsTestCase;

const KEY: &str = r#"{"type": "service_account", "client_email": "loader@demo.iam.gserviceaccount.com", "private_key": "-----BEGIN PRIVATE KEY-----", "token_uri": "https://oauth2.googleapis.com/token", "project_id": "demo"}"#;

#[test]
fn given_a_connection_and_its_environment_when_finding_how_it_signs_in_then_debug_names_the_identity_and_where_it_came_from_and_unusable_files_fail()
-> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let write = |name: &str, value: serde_json::Value| -> Result<String, String> {
        let path = directory.path().join(name);
        std::fs::write(&path, value.to_string()).map_err(|error| error.to_string())?;
        Ok(path.display().to_string())
    };
    let aws = write(
        "aws.json",
        json!({"type": "external_account", "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/sqlload-pool/providers/aws", "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request", "token_url": "https://sts.googleapis.com/v1/token", "credential_source": {"environment_id": "aws1", "region_url": "http://169.254.169.254/latest/meta-data/placement/availability-zone", "url": "http://169.254.169.254/latest/meta-data/iam/security-credentials", "regional_cred_verification_url": "https://sts.{region}.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15"}, "service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/loader@demo.iam.gserviceaccount.com:generateAccessToken"}),
    )?;
    let user = write(
        "user.json",
        json!({"type": "authorized_user", "client_id": "c", "client_secret": "s", "refresh_token": "r", "quota_project_id": "billed"}),
    )?;
    let workforce = write("other.json", json!({"type": "gdch_service_account"}))?;
    let test_cases = [
        CredentialsTestCase {
            description: "the connection's key, with its project",
            key: Some(KEY),
            impersonate: None,
            named: None,
            expected_description: "the service-account key of loader@demo.iam.gserviceaccount.com (the connection's service_account_key)",
            expected_project: Some("demo"),
            expected_error: "",
        },
        CredentialsTestCase {
            description: "the connection's impersonate_service_account acts as that account through IAM Credentials, in its project",
            key: None,
            impersonate: Some("reader@other.iam.gserviceaccount.com"),
            named: Some(&user),
            expected_description: "a gcloud user login (authorized_user) (GOOGLE_APPLICATION_CREDENTIALS, ",
            expected_project: Some("other"),
            expected_error: "",
        },
        CredentialsTestCase {
            description: "an AWS federation file, naming the provider and the account it impersonates",
            key: None,
            impersonate: None,
            named: Some(&aws),
            expected_description: "workload identity federation with AWS keys, by a signed GetCallerIdentity, through provider //iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/sqlload-pool/providers/aws (GOOGLE_APPLICATION_CREDENTIALS, ",
            expected_project: Some("demo"),
            expected_error: "",
        },
        CredentialsTestCase {
            description: "a file and the connection both naming an account to impersonate is refused",
            key: None,
            impersonate: Some("reader@other.iam.gserviceaccount.com"),
            named: Some(&aws),
            expected_description: "",
            expected_project: None,
            expected_error: "both name a service account to impersonate",
        },
        CredentialsTestCase {
            description: "the connection's key must be a service-account key",
            key: Some(r#"{"type": "authorized_user"}"#),
            impersonate: None,
            named: None,
            expected_description: "",
            expected_project: None,
            expected_error: "the connection's service_account_key is a \"authorized_user\" credentials file; it takes a service-account key",
        },
        CredentialsTestCase {
            description: "a credentials file of a type SQLLoad cannot use is named, never skipped",
            key: None,
            impersonate: None,
            named: Some(&workforce),
            expected_description: "",
            expected_project: None,
            expected_error: "is a \"gdch_service_account\" credentials file, which SQLLoad cannot sign in with",
        },
        CredentialsTestCase {
            description: "with nothing else, the metadata server",
            key: None,
            impersonate: None,
            named: None,
            expected_description: "the machine's service account, from its metadata server (metadata.google.internal)",
            expected_project: None,
            expected_error: "",
        },
    ];
    for test_case in &test_cases {
        let variable = |name: &str| {
            test_case
                .named
                .filter(|_| name == "GOOGLE_APPLICATION_CREDENTIALS")
                .map(str::to_owned)
        };
        let key = test_case.key.map(|key| Secret(key.to_owned()));
        let found = google_credentials(key.as_ref(), test_case.impersonate, &variable);
        let described = found
            .as_ref()
            .map(|credentials| credentials.describe())
            .unwrap_or_default();
        let project = found
            .as_ref()
            .ok()
            .and_then(|credentials| credentials.project());
        let error = found.err().unwrap_or_default();
        assert!(
            described.starts_with(test_case.expected_description),
            "{}: {described}",
            test_case.description
        );
        assert_eq!(
            project.as_deref(),
            test_case.expected_project,
            "{}",
            test_case.description
        );
        assert_eq!(
            error.is_empty(),
            test_case.expected_error.is_empty(),
            "{}: {error}",
            test_case.description
        );
        assert!(
            error.contains(test_case.expected_error),
            "{}: {error}",
            test_case.description
        );
    }
    let impersonated =
        google_credentials(None, Some("reader@other.iam.gserviceaccount.com"), &|_| {
            None
        })?;
    assert_eq!(
        impersonated.impersonate.map(|target| target.url),
        Some("https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/reader@other.iam.gserviceaccount.com:generateAccessToken".to_owned())
    );
    Ok(())
}
