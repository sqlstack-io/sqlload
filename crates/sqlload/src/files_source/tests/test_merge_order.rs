use crate::file_store::models::StoredFile;
use crate::files_source::main::merge_order::merge_order;
use crate::files_source::models::DataFile;
use crate::files_source::tests::test_types::MergeOrderTestCase;

#[test]
fn given_listed_files_when_ordering_a_merge_then_the_oldest_modified_comes_first_and_ties_go_by_path()
 {
    let test_cases = [
        MergeOrderTestCase {
            description: "modified time decides, not the name",
            files: &[("a.csv", 2_000), ("b.csv", 3_000), ("c.csv", 1_000)],
            expected_keys: &["c.csv", "a.csv", "b.csv"],
        },
        MergeOrderTestCase {
            description: "files modified at the same time go in path order",
            files: &[
                ("day=2/b.csv", 5_000),
                ("day=1/z.csv", 5_000),
                ("day=2/a.csv", 5_000),
            ],
            expected_keys: &["day=1/z.csv", "day=2/a.csv", "day=2/b.csv"],
        },
        MergeOrderTestCase {
            description: "ZIP members share their archive's time, so they go in member order after older files",
            files: &[
                ("x.zip/b.csv", 9_000),
                ("old.csv", 1_000),
                ("x.zip/a.csv", 9_000),
            ],
            expected_keys: &["old.csv", "x.zip/a.csv", "x.zip/b.csv"],
        },
    ];
    for test_case in test_cases {
        let files: Vec<DataFile> = test_case
            .files
            .iter()
            .map(|(key, modified_ms)| DataFile {
                file: StoredFile {
                    path: object_store::path::Path::from(*key),
                    relative_key: (*key).to_owned(),
                    size: 1,
                    etag: None,
                    modified_ms: *modified_ms,
                    member: None,
                },
                partitions: Vec::new(),
            })
            .collect();
        let ordered: Vec<String> = merge_order(files)
            .into_iter()
            .map(|data| data.file.relative_key)
            .collect();
        assert_eq!(
            ordered, test_case.expected_keys,
            "{}",
            test_case.description
        );
    }
}
