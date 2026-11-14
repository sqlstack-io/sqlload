use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::write_file::write_file;
use sqlload::store_paths::models::Location;
use sqlload_testkit::seaweedfs::constants::REGION;
use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::helpers::{day_files, run_async, seaweedfs_store};
use crate::test_types::S3ProfileTestCase;

/// The shared credentials file: SeaweedFS's keys under `lake`, none under `default`.
const CREDENTIALS: &str = "[default]\nregion = eu-west-2\n\n[lake]\naws_access_key_id = test\naws_secret_access_key = test\n";

#[test]
fn given_keys_only_in_a_shared_credentials_file_when_loading_from_s3_then_the_connections_profile_supplies_them()
-> Result<(), String> {
    let test_cases = [
        S3ProfileTestCase {
            description: "debug says the keys come from the file's profile, never what they are",
            profile: "lake",
            command: "debug",
            expected_text: "ok    connection files  S3 keys from ",
            expected_ok: true,
        },
        S3ProfileTestCase {
            description: "a files source in S3 loads into DuckDB with the profile's keys",
            profile: "lake",
            command: "load",
            expected_text: "2 rows",
            expected_ok: true,
        },
        S3ProfileTestCase {
            description: "a profile the file lacks fails debug, naming it",
            profile: "missing",
            command: "debug",
            expected_text: "fail  connection files  ",
            expected_ok: false,
        },
        S3ProfileTestCase {
            description: "and fails the load rather than trying other credentials",
            profile: "missing",
            command: "load",
            expected_text: "for AWS profile missing",
            expected_ok: false,
        },
    ];
    run_async(async {
        let server = start_seaweedfs().await?;
        create_bucket(&server, "example-bucket-test").await?;
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let (store, _) = seaweedfs_store(&server.endpoint, directory.path());
        let prefix = format!("profiles-{}/", uuid::Uuid::new_v4());
        let source = open_folder(&Location {
            store,
            prefix: prefix.clone(),
        })
        .map_err(|error| error.to_string())?;
        for (key, body) in day_files("2026-11-01", (1, 0), None)? {
            write_file(&source, &key, body)
                .await
                .map_err(|error| error.to_string())?;
        }
        let credentials = directory.path().join("credentials");
        std::fs::write(&credentials, CREDENTIALS).map_err(|error| error.to_string())?;
        for test_case in test_cases {
            let config = directory.path().join(format!("{}.toml", test_case.profile));
            let text = format!(
                "[connections.wh]\nkind = \"duckdb\"\ndatabase = \"{}\"\n\n[connections.files]\nkind = \"s3\"\nbucket = \"example-bucket-test\"\nregion = \"{REGION}\"\nendpoint = \"{}\"\nprofile = \"{}\"\n\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"raw\"\n\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"{prefix}\"\npartition_columns = {{ day = \"date\" }}\nwrite_strategy = \"replace\"\n",
                directory
                    .path()
                    .join(format!("{}.duckdb", test_case.profile))
                    .display(),
                server.endpoint,
                test_case.profile
            );
            std::fs::write(&config, text).map_err(|error| error.to_string())?;
            let output = std::process::Command::new(env!("CARGO_BIN_EXE_sqlload"))
                .arg("--config")
                .arg(&config)
                .arg("--no-color")
                .arg(test_case.command)
                .env_remove("AWS_ACCESS_KEY_ID")
                .env_remove("AWS_SECRET_ACCESS_KEY")
                .env_remove("AWS_SESSION_TOKEN")
                .env_remove("AWS_PROFILE")
                .env("AWS_SHARED_CREDENTIALS_FILE", &credentials)
                .env("AWS_EC2_METADATA_DISABLED", "true")
                .output()
                .map_err(|error| error.to_string())?;
            let printed = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                printed.contains(test_case.expected_text)
                    && output.status.success() == test_case.expected_ok,
                "{}: {printed}",
                test_case.description
            );
        }
        Ok(())
    })
}
