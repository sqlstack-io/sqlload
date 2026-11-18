use crate::file_store::_helpers::gcs_keys::gcs_credentials;
use crate::file_store::tests::test_types::GcsCredentialsTestCase;

#[test]
fn given_a_gcs_connection_with_or_without_a_key_when_choosing_its_credentials_then_the_first_in_googles_chain_wins()
-> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let home = directory.path().join("home");
    let gcloud = home.join(".config/gcloud");
    std::fs::create_dir_all(&gcloud).map_err(|error| error.to_string())?;
    std::fs::write(gcloud.join("application_default_credentials.json"), "{}")
        .map_err(|error| error.to_string())?;
    std::fs::write(directory.path().join("sa.json"), "{}").map_err(|error| error.to_string())?;
    let test_cases = [
        GcsCredentialsTestCase {
            description: "the connection's key wins over the environment",
            has_key: true,
            named: Some("sa.json"),
            home: Some("home"),
            expected_text: "the connection's service_account_key",
        },
        GcsCredentialsTestCase {
            description: "GOOGLE_APPLICATION_CREDENTIALS wins over gcloud's file",
            has_key: false,
            named: Some("sa.json"),
            home: Some("home"),
            expected_text: "GOOGLE_APPLICATION_CREDENTIALS (",
        },
        GcsCredentialsTestCase {
            description: "GOOGLE_APPLICATION_CREDENTIALS naming no file fails rather than falling through",
            has_key: false,
            named: Some("gone.json"),
            home: Some("home"),
            expected_text: "gone.json, which is not a file",
        },
        GcsCredentialsTestCase {
            description: "without either, gcloud's application default credentials",
            has_key: false,
            named: None,
            home: Some("home"),
            expected_text: "gcloud's application default credentials (",
        },
        GcsCredentialsTestCase {
            description: "with none of them, the machine's service account",
            has_key: false,
            named: None,
            home: Some("nobody"),
            expected_text: "the machine's service account",
        },
    ];
    let home_variable = if cfg!(windows) { "APPDATA" } else { "HOME" };
    for test_case in &test_cases {
        let variable = |name: &str| {
            let relative = match name {
                "GOOGLE_APPLICATION_CREDENTIALS" => test_case.named,
                other if other == home_variable => test_case.home,
                _ => None,
            };
            relative.map(|relative| directory.path().join(relative).display().to_string())
        };
        let found = gcs_credentials(test_case.has_key, &variable)
            .map(|source| source.describe())
            .unwrap_or_else(|error| error);
        assert!(
            found.contains(test_case.expected_text),
            "{}: {found}",
            test_case.description
        );
    }
    Ok(())
}
