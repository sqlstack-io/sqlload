use sqlload::files_source::models::AfterLoad;
use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::after_load_helpers::{run_after_load_case, run_s3_after_load};
use crate::helpers::run_async;
use crate::test_types::{AfterLoadStep, AfterLoadTestCase, S3AfterLoadTestCase};

#[test]
fn given_after_load_when_files_load_then_only_committed_files_are_deleted_or_moved_and_never_overwrite()
-> Result<(), String> {
    let test_cases = [
        AfterLoadTestCase {
            description: "keep (the default) leaves every file and reports no after_load",
            settings: "write_strategy = \"append\"\n",
            linked: &[],
            steps: &[AfterLoadStep {
                removed: &[],
                files: &[("drop/a.csv", "id,v\n1,a\n")],
                expected_exit: 0,
                expected_report: r#"{"after_load":null,"problems":[],"rows":1,"skipped":[],"status":"loaded"}"#,
            }],
            expected_files: &["drop/a.csv=id,v\\n1,a\\n"],
        },
        AfterLoadTestCase {
            description: "delete removes each loaded file, a run on the emptied folder skips the table, and a later file of the same name is a new one",
            settings: "write_strategy = \"append\"\nafter_load = \"delete\"\n",
            linked: &[],
            steps: &[
                AfterLoadStep {
                    removed: &[],
                    files: &[("drop/a.csv", "id,v\n1,a\n"), ("drop/b.csv", "id,v\n2,b\n")],
                    expected_exit: 0,
                    expected_report: r#"{"after_load":{"action":"delete","failed":[],"files":2,"status":"done"},"problems":[],"rows":2,"skipped":[],"status":"loaded"}"#,
                },
                AfterLoadStep {
                    removed: &[],
                    files: &[],
                    expected_exit: 0,
                    expected_report: r#"{"after_load":null,"problems":[],"rows":null,"skipped":["no files to load in drop/: after_load deleted or moved the ones already loaded"],"status":null}"#,
                },
                AfterLoadStep {
                    removed: &[],
                    files: &[("drop/a.csv", "id,v\n3,cc\n")],
                    expected_exit: 0,
                    expected_report: r#"{"after_load":{"action":"delete","failed":[],"files":1,"status":"done"},"problems":[],"rows":1,"skipped":[],"status":"loaded"}"#,
                },
            ],
            expected_files: &[],
        },
        AfterLoadTestCase {
            description: "move_to moves each loaded file below it, keeping its key",
            settings: "write_strategy = \"append\"\nafter_load = { move_to = \"done\" }\n",
            linked: &[],
            steps: &[AfterLoadStep {
                removed: &[],
                files: &[
                    ("drop/a.csv", "id,v\n1,a\n"),
                    ("drop/2026/b.csv", "id,v\n2,b\n"),
                ],
                expected_exit: 0,
                expected_report: r#"{"after_load":{"action":"move","failed":[],"files":2,"status":"done"},"problems":[],"rows":2,"skipped":[],"status":"loaded"}"#,
            }],
            expected_files: &["done/2026/b.csv=id,v\\n2,b\\n", "done/a.csv=id,v\\n1,a\\n"],
        },
        AfterLoadTestCase {
            description: "a move onto an existing file leaves both, warns in the report, and the load still counts",
            settings: "write_strategy = \"append\"\nafter_load = { move_to = \"done\" }\n",
            linked: &[],
            steps: &[AfterLoadStep {
                removed: &[],
                files: &[("done/a.csv", "old"), ("drop/a.csv", "id,v\n1,a\n")],
                expected_exit: 0,
                expected_report: r#"{"after_load":{"action":"move","failed":["<root>/drop/a.csv"],"files":0,"status":"failed"},"problems":[],"rows":1,"skipped":[],"status":"loaded"}"#,
            }],
            expected_files: &["done/a.csv=old", "drop/a.csv=id,v\\n1,a\\n"],
        },
        AfterLoadTestCase {
            description: "a move to another volume copies, checks the size, then deletes, escaped keys too",
            settings: "write_strategy = \"append\"\nafter_load = { move_to = \"archive\" }\n",
            linked: &[("archive", "/dev/shm")],
            steps: &[AfterLoadStep {
                removed: &[],
                files: &[
                    ("drop/a.csv", "id,v\n1,a\n"),
                    ("drop/ts=00%3A00/b.csv", "id,v\n2,b\n"),
                ],
                expected_exit: 0,
                expected_report: r#"{"after_load":{"action":"move","failed":[],"files":2,"status":"done"},"problems":[],"rows":2,"skipped":[],"status":"loaded"}"#,
            }],
            expected_files: &[
                "archive/a.csv=id,v\\n1,a\\n",
                "archive/ts=00%3A00/b.csv=id,v\\n2,b\\n",
            ],
        },
        AfterLoadTestCase {
            description: "an emptied folder is skipped only once its table exists, and a folder holding another format or missing still fails",
            settings: "write_strategy = \"append\"\nafter_load = { move_to = \"done\" }\n",
            linked: &[],
            steps: &[
                AfterLoadStep {
                    removed: &[],
                    files: &[("drop/.keep", "")],
                    expected_exit: 1,
                    expected_report: r#"{"after_load":null,"problems":["no files to load in drop/, and scores was never loaded: after_load skips an emptied folder only once its table exists; check the source's path and connection"],"rows":null,"skipped":[],"status":null}"#,
                },
                AfterLoadStep {
                    removed: &[],
                    files: &[("drop/a.csv", "id,v\n1,a\n")],
                    expected_exit: 0,
                    expected_report: r#"{"after_load":{"action":"move","failed":[],"files":1,"status":"done"},"problems":[],"rows":1,"skipped":[],"status":"loaded"}"#,
                },
                AfterLoadStep {
                    removed: &[],
                    files: &[],
                    expected_exit: 0,
                    expected_report: r#"{"after_load":null,"problems":[],"rows":null,"skipped":["no files to load in drop/: after_load deleted or moved the ones already loaded"],"status":null}"#,
                },
                AfterLoadStep {
                    removed: &[],
                    files: &[("drop/b.json", "{}")],
                    expected_exit: 1,
                    expected_report: r#"{"after_load":null,"problems":["<root>/drop/: no .csv, .csv.gz, .csv.zst files found; check the source's path and format (an empty source never empties the destination)"],"rows":null,"skipped":[],"status":null}"#,
                },
                AfterLoadStep {
                    removed: &["drop"],
                    files: &[],
                    expected_exit: 1,
                    expected_report: r#"{"after_load":null,"problems":["<root>/drop/: no .csv, .csv.gz, .csv.zst files found; check the source's path and format (an empty source never empties the destination); the folder cannot be read: No such file or directory (os error 2)"],"rows":null,"skipped":[],"status":null}"#,
                },
            ],
            expected_files: &["done/a.csv=id,v\\n1,a\\n"],
        },
        AfterLoadTestCase {
            description: "a failed load deletes nothing",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\non_duplicate_keys = \"fail\"\nafter_load = \"delete\"\n",
            linked: &[],
            steps: &[AfterLoadStep {
                removed: &[],
                files: &[("drop/a.csv", "id,v\n1,a\n1,b\n")],
                expected_exit: 1,
                expected_report: r#"{"after_load":null,"problems":[],"rows":null,"skipped":[],"status":"failed"}"#,
            }],
            expected_files: &["drop/a.csv=id,v\\n1,a\\n1,b\\n"],
        },
    ];
    run_async(async {
        for test_case in &test_cases {
            let files = run_after_load_case(test_case).await?;
            assert_eq!(files, test_case.expected_files, "{}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_files_on_s3_when_after_load_acts_then_they_move_server_side_or_are_deleted_and_a_taken_or_changed_file_stays()
-> Result<(), String> {
    let test_cases = [
        S3AfterLoadTestCase {
            description: "moves keep their key below move_to",
            folder: "move/",
            after_load: AfterLoad::MoveTo("move/done".to_owned()),
            files: &[("drop/a.csv", "a"), ("drop/2026/b.csv", "b")],
            rewritten: &[],
            expected_report: "move done 2 0",
            expected_files: &["done/2026/b.csv=b", "done/a.csv=a"],
        },
        S3AfterLoadTestCase {
            description: "a file already at the destination is never replaced, and both stay",
            folder: "clash/",
            after_load: AfterLoad::MoveTo("clash/done".to_owned()),
            files: &[
                ("drop/a.csv", "new"),
                ("drop/b.csv", "b"),
                ("done/a.csv", "old"),
            ],
            rewritten: &[],
            expected_report: "move failed 1 1",
            expected_files: &["done/a.csv=old", "done/b.csv=b", "drop/a.csv=new"],
        },
        S3AfterLoadTestCase {
            description: "deletes remove only the listed files, not one rewritten since",
            folder: "delete/",
            after_load: AfterLoad::Delete,
            files: &[("drop/a.csv", "a"), ("drop/b.csv", "b")],
            rewritten: &[("drop/a.csv", "a, longer now")],
            expected_report: "delete failed 1 1",
            expected_files: &["drop/a.csv=a, longer now"],
        },
    ];
    run_async(async {
        let server = start_seaweedfs().await?;
        create_bucket(&server, "after-load-test").await?;
        for test_case in &test_cases {
            let actual =
                run_s3_after_load((&server.endpoint, "after-load-test"), test_case).await?;
            assert_eq!(
                actual,
                (
                    test_case.expected_report.to_owned(),
                    test_case
                        .expected_files
                        .iter()
                        .map(|file| (*file).to_owned())
                        .collect()
                ),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
