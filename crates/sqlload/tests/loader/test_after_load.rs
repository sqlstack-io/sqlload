use crate::after_load_helpers::run_after_load_case;
use crate::helpers::run_async;
use crate::test_types::{AfterLoadStep, AfterLoadTestCase};

#[test]
fn given_after_load_when_files_load_then_only_committed_files_are_deleted_or_moved_and_never_overwrite()
-> Result<(), String> {
    let test_cases = [
        AfterLoadTestCase {
            description: "keep (the default) leaves every file and reports no after_load",
            settings: "write_strategy = \"append\"\n",
            linked: &[],
            steps: &[AfterLoadStep {
                files: &[("drop/a.csv", "id,v\n1,a\n")],
                expected_exit: 0,
                expected_report: r#"{"after_load":null,"rows":1,"status":"loaded"}"#,
            }],
            expected_files: &["drop/a.csv=id,v\\n1,a\\n"],
        },
        AfterLoadTestCase {
            description: "delete removes each loaded file, and a later file of the same name is a new one",
            settings: "write_strategy = \"append\"\nafter_load = \"delete\"\n",
            linked: &[],
            steps: &[
                AfterLoadStep {
                    files: &[("drop/a.csv", "id,v\n1,a\n"), ("drop/b.csv", "id,v\n2,b\n")],
                    expected_exit: 0,
                    expected_report: r#"{"after_load":{"action":"delete","failed":[],"files":2,"status":"done"},"rows":2,"status":"loaded"}"#,
                },
                AfterLoadStep {
                    files: &[("drop/a.csv", "id,v\n3,cc\n")],
                    expected_exit: 0,
                    expected_report: r#"{"after_load":{"action":"delete","failed":[],"files":1,"status":"done"},"rows":1,"status":"loaded"}"#,
                },
            ],
            expected_files: &[],
        },
        AfterLoadTestCase {
            description: "move_to moves each loaded file below it, keeping its key",
            settings: "write_strategy = \"append\"\nafter_load = { move_to = \"done\" }\n",
            linked: &[],
            steps: &[AfterLoadStep {
                files: &[
                    ("drop/a.csv", "id,v\n1,a\n"),
                    ("drop/2026/b.csv", "id,v\n2,b\n"),
                ],
                expected_exit: 0,
                expected_report: r#"{"after_load":{"action":"move","failed":[],"files":2,"status":"done"},"rows":2,"status":"loaded"}"#,
            }],
            expected_files: &["done/2026/b.csv=id,v\\n2,b\\n", "done/a.csv=id,v\\n1,a\\n"],
        },
        AfterLoadTestCase {
            description: "a move onto an existing file leaves both, warns in the report, and the load still counts",
            settings: "write_strategy = \"append\"\nafter_load = { move_to = \"done\" }\n",
            linked: &[],
            steps: &[AfterLoadStep {
                files: &[("done/a.csv", "old"), ("drop/a.csv", "id,v\n1,a\n")],
                expected_exit: 0,
                expected_report: r#"{"after_load":{"action":"move","failed":["<root>/drop/a.csv"],"files":0,"status":"failed"},"rows":1,"status":"loaded"}"#,
            }],
            expected_files: &["done/a.csv=old", "drop/a.csv=id,v\\n1,a\\n"],
        },
        AfterLoadTestCase {
            description: "a move to another volume copies, checks the size, then deletes",
            settings: "write_strategy = \"append\"\nafter_load = { move_to = \"archive\" }\n",
            linked: &[("archive", "/dev/shm")],
            steps: &[AfterLoadStep {
                files: &[("drop/a.csv", "id,v\n1,a\n")],
                expected_exit: 0,
                expected_report: r#"{"after_load":{"action":"move","failed":[],"files":1,"status":"done"},"rows":1,"status":"loaded"}"#,
            }],
            expected_files: &["archive/a.csv=id,v\\n1,a\\n"],
        },
        AfterLoadTestCase {
            description: "a failed load deletes nothing",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nafter_load = \"delete\"\n",
            linked: &[],
            steps: &[AfterLoadStep {
                files: &[("drop/a.csv", "id,v\n1,a\n1,b\n")],
                expected_exit: 1,
                expected_report: r#"{"after_load":null,"rows":null,"status":"failed"}"#,
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
