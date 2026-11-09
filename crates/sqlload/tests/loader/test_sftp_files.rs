use sqlload_testkit::ssh::main::put_file::put_file;
use sqlload_testkit::ssh::main::start_bastion::start_bastion;

use crate::helpers::{duckdb_text, integration, run_async, run_sqlload};
use crate::sftp_helpers::{
    first_events, first_orders, project_text, rewritten_orders, second_events, second_orders,
    snowflake_project,
};
use crate::test_types::{SftpFile, SftpSnowflakePlanTestCase, SftpStepTestCase};

/// The right key, and the server's host keys known.
const KEYED: &str = "private_key_file = \"KEYS/client.key\"\nknown_hosts = \"KEYS/known_hosts\"";
/// `0` while nothing has loaded: no table in `raw`.
const NO_TABLES: &str =
    "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'";
/// Both tables' row counts, and the modification times in each orders row's version (`<size>-<mtime in ms>`).
const COUNTS: &str = "SELECT (SELECT count(*) FROM raw.orders) || ',' || (SELECT count(*) FROM raw.events) || ',' || (SELECT string_agg(DISTINCT regexp_replace(_sqlload_source_version, '^[0-9]+-', ''), '|' ORDER BY regexp_replace(_sqlload_source_version, '^[0-9]+-', '')) FROM raw.orders WHERE regexp_matches(_sqlload_source_version, '^[0-9]+-[0-9]+$'))";

/// Files sources on an SFTP server: append loads only new files (recognised by size and modification time), a rewritten file is noticed, gzip CSV and Parquet read through SQLLoad, and the server is trusted only through `known_hosts`.
#[test]
fn given_an_sftp_server_when_loading_files_then_new_files_load_once_and_unknown_hosts_are_refused()
-> Result<(), String> {
    let test_cases = [
        SftpStepTestCase {
            description: "the plan reads the files over SFTP through SQLLoad",
            files: &[
                SftpFile {
                    path: "/config/drop/orders/a.csv.gz",
                    bytes: first_orders,
                    mtime: 1_794_200_000,
                },
                SftpFile {
                    path: "/config/drop/events/p1.parquet",
                    bytes: first_events,
                    mtime: 1_794_200_000,
                },
            ],
            sign_in: KEYED,
            command: "plan",
            expected_success: true,
            expected_output: "read over SFTP -> read csv into Arrow",
            check_query: NO_TABLES,
            expected_result: "0",
        },
        SftpStepTestCase {
            description: "the first load takes every file",
            files: &[],
            sign_in: KEYED,
            command: "load",
            expected_success: true,
            expected_output: "",
            check_query: COUNTS,
            expected_result: "2,2,1794200000000",
        },
        SftpStepTestCase {
            description: "the next load takes only the new files, skipping a hidden one, and versions them by size and modification time",
            files: &[
                SftpFile {
                    path: "/config/drop/orders/b.csv.gz",
                    bytes: second_orders,
                    mtime: 1_794_200_100,
                },
                SftpFile {
                    path: "/config/drop/orders/_partial.csv.gz",
                    bytes: rewritten_orders,
                    mtime: 1_794_200_100,
                },
                SftpFile {
                    path: "/config/drop/events/p2.parquet",
                    bytes: second_events,
                    mtime: 1_794_200_100,
                },
            ],
            sign_in: KEYED,
            command: "load",
            expected_success: true,
            expected_output: "",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
        SftpStepTestCase {
            description: "a load with nothing new loads nothing",
            files: &[],
            sign_in: KEYED,
            command: "load",
            expected_success: true,
            expected_output: "",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
        SftpStepTestCase {
            description: "a password signs in",
            files: &[],
            sign_in: "password = \"loader-test-password\"\nknown_hosts = \"KEYS/known_hosts\"",
            command: "debug",
            expected_success: true,
            expected_output: "sftp://loader@127.0.0.1/~/drop/orders/ lists",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
        SftpStepTestCase {
            description: "a wrong password is refused",
            files: &[],
            sign_in: "password = \"wrong\"\nknown_hosts = \"KEYS/known_hosts\"",
            command: "debug",
            expected_success: false,
            expected_output: "the server refused loader's password",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
        SftpStepTestCase {
            description: "a key the server does not know is refused",
            files: &[],
            sign_in: "private_key_file = \"KEYS/other.key\"\nknown_hosts = \"KEYS/known_hosts\"",
            command: "debug",
            expected_success: false,
            expected_output: "the server refused loader's key",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
        SftpStepTestCase {
            description: "a server whose host key is not in known_hosts is refused with its fingerprint, before anything is read",
            files: &[],
            sign_in: "private_key_file = \"KEYS/client.key\"\nknown_hosts = \"KEYS/empty_hosts\"",
            command: "load",
            expected_success: false,
            expected_output: "host key SHA256:",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
        SftpStepTestCase {
            description: "a server whose host key changed is refused",
            files: &[],
            sign_in: "private_key_file = \"KEYS/client.key\"\nknown_hosts = \"KEYS/changed_hosts\"",
            command: "load",
            expected_success: false,
            expected_output: "host key changed",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
        SftpStepTestCase {
            description: "known_hosts is required: there is no trust on first use",
            files: &[],
            sign_in: "private_key_file = \"KEYS/client.key\"",
            command: "plan",
            expected_success: false,
            expected_output: "missing field `known_hosts`",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
        SftpStepTestCase {
            description: "a loaded file touched (same bytes, new modification time) counts as rewritten and stops append",
            files: &[SftpFile {
                path: "/config/drop/orders/a.csv.gz",
                bytes: first_orders,
                mtime: 1_794_200_200,
            }],
            sign_in: KEYED,
            command: "load",
            expected_success: false,
            expected_output: "was rewritten in place",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
        SftpStepTestCase {
            description: "the file back as it was loads nothing new",
            files: &[SftpFile {
                path: "/config/drop/orders/a.csv.gz",
                bytes: first_orders,
                mtime: 1_794_200_000,
            }],
            sign_in: KEYED,
            command: "load",
            expected_success: true,
            expected_output: "",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
        SftpStepTestCase {
            description: "a loaded file rewritten with a new size but its old modification time counts as rewritten",
            files: &[SftpFile {
                path: "/config/drop/orders/a.csv.gz",
                bytes: rewritten_orders,
                mtime: 1_794_200_000,
            }],
            sign_in: KEYED,
            command: "load",
            expected_success: false,
            expected_output: "was rewritten in place",
            check_query: COUNTS,
            expected_result: "3,3,1794200000000|1794200100000",
        },
    ];
    run_async(async {
        let bastion = start_bastion().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        for test_case in test_cases {
            for file in test_case.files {
                put_file(&bastion, file.path, &(file.bytes)()?, file.mtime)?;
            }
            std::fs::write(
                &config,
                project_text(&bastion, test_case.sign_in, &database),
            )
            .map_err(|error| error.to_string())?;
            let args: Vec<&str> = test_case.command.split_whitespace().collect();
            let (succeeded, output) = run_sqlload(&config, &args, ("", ""))?;
            assert!(
                succeeded == test_case.expected_success
                    && output.contains(test_case.expected_output),
                "{}:\n{output}",
                test_case.description
            );
            let result = duckdb_text(&database, test_case.check_query)?;
            assert_eq!(
                result, test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

/// Snowflake reads files from S3 itself, but never over SFTP: the files always pass through SQLLoad's reader, even with an external stage set, and `copy_to_internal_stage` is refused.
#[test]
fn given_sftp_files_and_a_snowflake_target_when_planning_then_sqlload_reads_them()
-> Result<(), String> {
    let test_cases = [
        SftpSnowflakePlanTestCase {
            description: "an external stage on S3 does not cover files on an SFTP server",
            settings: "",
            expected_success: true,
            expected_output: "route   read over SFTP -> read csv into Arrow -> Parquet -> ",
        },
        SftpSnowflakePlanTestCase {
            description: "copy_to_internal_stage is refused",
            settings: "copy_to_internal_stage = true",
            expected_success: false,
            expected_output: "copy_to_internal_stage is not for an sftp connection",
        },
    ];
    run_async(async {
        let bastion = start_bastion().await?;
        let fake =
            sqlload_testkit::fake_snowflake::main::start_fake_snowflake::start_fake_snowflake()
                .await?;
        integration(&fake);
        put_file(
            &bastion,
            "/config/drop/orders/a.csv.gz",
            &first_orders()?,
            1_794_200_000,
        )?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let config = folder.path().join("sqlload.toml");
        for test_case in test_cases {
            std::fs::write(
                &config,
                snowflake_project(&bastion, &fake.base_url, test_case.settings),
            )
            .map_err(|error| error.to_string())?;
            let (succeeded, output) = run_sqlload(&config, &["plan"], ("", ""))?;
            assert!(
                succeeded == test_case.expected_success
                    && output.contains(test_case.expected_output)
                    && !output.contains("COPYs directly"),
                "{}:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}
