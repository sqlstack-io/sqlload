use crate::adapters::files::main::newest_load_files::newest_load_files;
use crate::adapters::files::tests::helpers::stored_at;
use crate::adapters::files::tests::test_types::NewestLoadTestCase;
use crate::file_store::models::StoredFile;

#[test]
fn given_a_partitions_files_when_picking_the_newest_load_then_only_its_files_are_read() {
    let test_cases = [
        NewestLoadTestCase {
            description: "the load with the latest file wins, with all its parts",
            files: &[
                ("day=1/part-aaa-0.parquet", 100),
                ("day=1/part-bbb-0.parquet", 300),
                ("day=1/part-bbb-1.parquet", 200),
                ("day=1/part-aaa-1.parquet", 150),
            ],
            expected_keys: &["day=1/part-bbb-0.parquet", "day=1/part-bbb-1.parquet"],
        },
        NewestLoadTestCase {
            description: "load ids with dashes are kept whole",
            files: &[
                ("part-0f1e-22-0.parquet", 5),
                ("part-0f1e-22-1.parquet", 5),
                ("part-0f1e-23-0.parquet", 4),
            ],
            expected_keys: &["part-0f1e-22-0.parquet", "part-0f1e-22-1.parquet"],
        },
        NewestLoadTestCase {
            description: "a file another tool wrote is a load of its own",
            files: &[("part-aaa-0.parquet", 1), ("export.parquet", 9)],
            expected_keys: &["export.parquet"],
        },
        NewestLoadTestCase {
            description: "loads tied at the latest time are all read, whatever their ids",
            files: &[
                ("part-ffff-0.parquet", 7),
                ("part-0000-0.parquet", 7),
                ("part-0000-1.parquet", 3),
                ("part-aaaa-0.parquet", 6),
            ],
            expected_keys: &[
                "part-0000-0.parquet",
                "part-0000-1.parquet",
                "part-ffff-0.parquet",
            ],
        },
        NewestLoadTestCase {
            description: "no files, no load",
            files: &[],
            expected_keys: &[],
        },
    ];
    for test_case in test_cases {
        let files: Vec<StoredFile> = test_case
            .files
            .iter()
            .map(|(key, modified)| stored_at(key, *modified))
            .collect();
        let keys: Vec<String> = newest_load_files(&files)
            .into_iter()
            .map(|file| file.relative_key)
            .collect();
        assert_eq!(keys, test_case.expected_keys, "{}", test_case.description);
    }
}
