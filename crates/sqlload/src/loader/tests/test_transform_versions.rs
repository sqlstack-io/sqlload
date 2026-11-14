use crate::loader::tests::helpers::older_count;
use crate::loader::tests::test_types::OlderFilesTestCase;
use crate::run_log::models::RunEvent;

#[test]
fn given_a_tables_transform_records_when_planning_then_the_files_an_earlier_version_loaded_are_counted()
-> Result<(), String> {
    let test_cases = [
        OlderFilesTestCase {
            description: "files from an earlier version stay counted after a later load under the current one",
            records: &[
                (RunEvent::Run, "transform", &["a.csv", "b.csv"], Some("f@1")),
                (RunEvent::Run, "transform", &["c.csv"], Some("f@2")),
            ],
            expected_older: 2,
        },
        OlderFilesTestCase {
            description: "a file merged again under the current version counts by its newest record, whatever its own version",
            records: &[
                (
                    RunEvent::Run,
                    "files source merge",
                    &["a.csv|1-100"],
                    Some("f@1"),
                ),
                (
                    RunEvent::Run,
                    "files source merge",
                    &["b.csv|1-100"],
                    Some("f@1"),
                ),
                (
                    RunEvent::Run,
                    "files source merge",
                    &["a.csv|2-200"],
                    Some("f@2"),
                ),
            ],
            expected_older: 1,
        },
        OlderFilesTestCase {
            description: "a load that rewrote the table whole leaves only its own files",
            records: &[
                (RunEvent::Run, "transform", &["a.csv", "b.csv"], Some("f@1")),
                (
                    RunEvent::Run,
                    "transform, table rewritten",
                    &["a.csv", "b.csv"],
                    Some("f@2"),
                ),
            ],
            expected_older: 0,
        },
        OlderFilesTestCase {
            description: "a reset (a merge table created again) clears what came before",
            records: &[
                (RunEvent::Run, "files source merge", &["a.csv"], Some("f@1")),
                (
                    RunEvent::Reset,
                    "table created; files merged before no longer count",
                    &[],
                    None,
                ),
                (RunEvent::Run, "files source merge", &["a.csv"], Some("f@2")),
            ],
            expected_older: 0,
        },
        OlderFilesTestCase {
            description: "records without a transform version count nothing",
            records: &[(RunEvent::Run, "files source merge", &["a.csv"], None)],
            expected_older: 0,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            older_count(&test_case)?,
            test_case.expected_older,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
