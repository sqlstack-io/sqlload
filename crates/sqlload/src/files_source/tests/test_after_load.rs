use crate::files_source::models::AfterLoad;
use crate::files_source::tests::helpers::{
    block_on, local, local_roots, resolved_move_to, run_after_load,
};
use crate::files_source::tests::test_types::{AfterLoadTestCase, MoveToTestCase};
use crate::store_paths::models::StoreKind;

#[test]
fn given_committed_files_when_after_load_acts_then_each_is_deleted_or_moved_and_none_overwritten() {
    let test_cases = [
        AfterLoadTestCase {
            description: "kept files stay and nothing is reported",
            after_load: AfterLoad::Keep,
            files: &[("events/a.csv", "a")],
            loaded: &["a.csv"],
            rewritten: &[],
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
            rewritten: &[],
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
            rewritten: &[],
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
            rewritten: &[],
            expected_report: "move failed 1 1",
            expected_files: &["events/a.csv=new", "loaded/a.csv=old", "loaded/b.csv=b"],
        },
        AfterLoadTestCase {
            description: "a file rewritten after it was read stays where it is, since its new rows are not loaded",
            after_load: AfterLoad::Delete,
            files: &[("events/a.csv", "a"), ("events/b.csv", "b")],
            loaded: &["a.csv", "b.csv"],
            rewritten: &[("events/a.csv", "a, longer now")],
            expected_report: "delete failed 1 1",
            expected_files: &["events/a.csv=a, longer now"],
        },
        AfterLoadTestCase {
            description: "a rewritten file is not moved either",
            after_load: AfterLoad::MoveTo("loaded".to_owned()),
            files: &[("events/a.csv", "a"), ("events/b.csv", "b")],
            loaded: &["a.csv", "b.csv"],
            rewritten: &[("events/a.csv", "a, longer now")],
            expected_report: "move failed 1 1",
            expected_files: &["events/a.csv=a, longer now", "loaded/b.csv=b"],
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

#[test]
fn given_move_to_when_resolved_as_path_is_then_a_folder_overlapping_path_is_refused_however_written()
 {
    let s3 = StoreKind::S3 {
        bucket: "b".to_owned(),
        region: None,
        endpoint: None,
        profile: None,
    };
    let test_cases = [
        MoveToTestCase {
            description: "a sibling folder",
            store: local("/data"),
            path: "drop/",
            move_to: "done",
            expected_prefix: Ok("done/"),
        },
        MoveToTestCase {
            description: "a folder sharing the path's leading letters is outside it",
            store: local("/data"),
            path: "drop",
            move_to: "dropped",
            expected_prefix: Ok("dropped/"),
        },
        MoveToTestCase {
            description: "a folder inside path",
            store: local("/data"),
            path: "drop/",
            move_to: "drop/done",
            expected_prefix: Err(()),
        },
        MoveToTestCase {
            description: "path written as an absolute path below the root",
            store: local("/data"),
            path: "/data/drop/",
            move_to: "drop/done",
            expected_prefix: Err(()),
        },
        MoveToTestCase {
            description: "path written with ./",
            store: local("/data"),
            path: "./drop",
            move_to: "drop/done",
            expected_prefix: Err(()),
        },
        MoveToTestCase {
            description: "move_to differing from path only in case",
            store: local("/data"),
            path: "drop/",
            move_to: "Drop/done",
            expected_prefix: Err(()),
        },
        MoveToTestCase {
            description: "path written as a URL of the connection's bucket",
            store: s3.clone(),
            path: "s3://b/drop/",
            move_to: "drop/done",
            expected_prefix: Err(()),
        },
        MoveToTestCase {
            description: "move_to holding path, so files would move back inside it",
            store: s3.clone(),
            path: "arc/drop/",
            move_to: "arc",
            expected_prefix: Err(()),
        },
        MoveToTestCase {
            description: "the whole store",
            store: local("/data"),
            path: "drop/",
            move_to: "/data",
            expected_prefix: Err(()),
        },
        MoveToTestCase {
            description: "a .. segment",
            store: local("/data"),
            path: "drop/",
            move_to: "done/../drop",
            expected_prefix: Err(()),
        },
    ];
    for test_case in &test_cases {
        let actual = resolved_move_to(&test_case.store, test_case.path, test_case.move_to);
        assert_eq!(
            actual.as_deref().map_err(|_| ()),
            test_case.expected_prefix,
            "{}: {actual:?}",
            test_case.description
        );
    }
}
