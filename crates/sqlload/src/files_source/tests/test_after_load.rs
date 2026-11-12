use crate::files_source::models::AfterLoad;
use crate::files_source::tests::helpers::{block_on, local_roots, run_after_load};
use crate::files_source::tests::test_types::AfterLoadTestCase;

#[test]
fn given_committed_files_when_after_load_acts_then_each_is_deleted_or_moved_and_none_overwritten() {
    let test_cases = [
        AfterLoadTestCase {
            description: "kept files stay and nothing is reported",
            after_load: AfterLoad::Keep,
            files: &[("events/a.csv", "a")],
            loaded: &["a.csv"],
            expected_report: "none",
            expected_files: &["events/a.csv=a"],
        },
        AfterLoadTestCase {
            description: "only the loaded files are deleted, nested ones too",
            after_load: AfterLoad::Delete,
            files: &[
                ("events/a.csv", "a"),
                ("events/sub/b.csv", "b"),
                ("events/c.csv", "c"),
            ],
            loaded: &["a.csv", "sub/b.csv"],
            expected_report: "delete done 2 0",
            expected_files: &["events/c.csv=c"],
        },
        AfterLoadTestCase {
            description: "moved files keep their key below move_to",
            after_load: AfterLoad::MoveTo("loaded/".to_owned()),
            files: &[
                ("events/2026/a.csv", "a"),
                ("events/b.csv", "b"),
                ("events/c.csv", "c"),
            ],
            loaded: &["2026/a.csv", "b.csv"],
            expected_report: "move done 2 0",
            expected_files: &["events/c.csv=c", "loaded/2026/a.csv=a", "loaded/b.csv=b"],
        },
        AfterLoadTestCase {
            description: "a file already at the destination is never overwritten, and both stay",
            after_load: AfterLoad::MoveTo("loaded".to_owned()),
            files: &[
                ("events/a.csv", "new"),
                ("events/b.csv", "b"),
                ("loaded/a.csv", "old"),
            ],
            loaded: &["a.csv", "b.csv"],
            expected_report: "move failed 1 1",
            expected_files: &["events/a.csv=new", "loaded/a.csv=old", "loaded/b.csv=b"],
        },
    ];
    for test_case in &test_cases {
        for root in local_roots() {
            let actual = block_on(run_after_load(&root, test_case));
            assert_eq!(
                actual,
                Ok((
                    test_case.expected_report.to_owned(),
                    test_case
                        .expected_files
                        .iter()
                        .map(|file| (*file).to_owned())
                        .collect()
                )),
                "{} at {}",
                test_case.description,
                root.1
            );
        }
    }
}
