use crate::file_store::_helpers::gcs_keys::{gcs_builder, gcs_credentials};
use crate::file_store::tests::test_types::GcsCredentialsTestCase;

#[test]
fn given_a_gcs_connection_with_or_without_a_key_when_choosing_its_credentials_then_the_first_in_googles_chain_wins_and_a_missing_named_file_fails_the_client()
-> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let home = directory.path().join("home");
    for gcloud in [home.join(".config/gcloud"), home.join("gcloud")] {
        std::fs::create_dir_all(&gcloud).map_err(|error| error.to_string())?;
        std::fs::write(gcloud.join("application_default_credentials.json"), "{}")
            .map_err(|error| error.to_string())?;
    }
    std::fs::write(directory.path().join("sa.json"), "{}").map_err(|error| error.to_string())?;
    let test_cases = [
        GcsCredentialsTestCase {
            description: "the connection's key wins over the environment",
            has_key: true,
            named: Some("sa.json"),
            home: Some("home"),
            expected_text: "the connection's service_account_key",
            expected_builds: true,
        },
        GcsCredentialsTestCase {
            description: "GOOGLE_APPLICATION_CREDENTIALS wins over gcloud's file",
            has_key: false,
            named: Some("sa.json"),
            home: Some("home"),
            expected_text: "GOOGLE_APPLICATION_CREDENTIALS (",
            expected_builds: true,
        },
        GcsCredentialsTestCase {
            description: "GOOGLE_APPLICATION_CREDENTIALS naming no file fails rather than falling through",
            has_key: false,
            named: Some("gone.json"),
            home: Some("home"),
            expected_text: "gone.json, which is not a file",
            expected_builds: false,
        },
        GcsCredentialsTestCase {
            description: "without either, gcloud's application default credentials",
            has_key: false,
            named: None,
            home: Some("home"),
            expected_text: "gcloud's application default credentials (",
            expected_builds: true,
        },
        GcsCredentialsTestCase {
            description: "with none of them, the machine's service account",
            has_key: false,
            named: None,
            home: Some("nobody"),
            expected_text: "the machine's service account",
            expected_builds: true,
        },
    ];
    for test_case in &test_cases {
        let variables = [
            ("GOOGLE_APPLICATION_CREDENTIALS", test_case.named),
            ("HOME", test_case.home),
            ("APPDATA", test_case.home),
        ];
        let variable = |name: &str| {
            variables
                .iter()
                .find(|(variable, _)| *variable == name)
                .and_then(|(_, relative)| *relative)
                .map(|relative| directory.path().join(relative).display().to_string())
        };
        let found = gcs_credentials(test_case.has_key, &variable)
            .map(|source| source.describe())
            .unwrap_or_else(|error| error);
        assert!(
            found.contains(test_case.expected_text),
            "{}: {found}",
            test_case.description
        );
        let built = gcs_builder("lake", test_case.has_key.then_some("{}"), None, &variable);
        assert_eq!(
            built.is_ok(),
            test_case.expected_builds,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
