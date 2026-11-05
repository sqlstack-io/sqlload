use crate::adapters::snowflake::tests::helpers::stale;
use crate::adapters::snowflake::tests::test_types::StaleFoldersTestCase;

#[test]
fn given_staged_files_when_cleaning_the_user_stage_then_only_folders_older_than_a_day_go() {
    let test_cases = [
        StaleFoldersTestCase {
            description: "a load folder whose files are all old is a failed run's",
            listed: &[
                (
                    "sqlload/old-load/00001/part-00001.parquet",
                    "Mon, 2 Nov 2026 09:00:00 GMT",
                ),
                (
                    "sqlload/old-load/00002/part-00001.parquet",
                    "Mon, 2 Nov 2026 09:05:00 GMT",
                ),
            ],
            expected_folders: &["sqlload/old-load/"],
        },
        StaleFoldersTestCase {
            description: "a recent file keeps its folder (a running load)",
            listed: &[
                (
                    "sqlload/busy/00001/part-00001.parquet",
                    "Mon, 2 Nov 2026 09:00:00 GMT",
                ),
                (
                    "sqlload/busy/00002/part-00001.parquet",
                    "Thu, 5 Nov 2026 09:00:00 GMT",
                ),
            ],
            expected_folders: &[],
        },
        StaleFoldersTestCase {
            description: "a time that cannot be read keeps the folder; other paths are never touched",
            listed: &[
                ("sqlload/odd/00001/part-00001.parquet", "yesterday"),
                ("other/file.parquet", "Mon, 2 Nov 2026 09:00:00 GMT"),
            ],
            expected_folders: &[],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            stale(test_case.listed),
            test_case.expected_folders,
            "{}",
            test_case.description
        );
    }
}
