use crate::column_types::models::ColumnType;
use crate::contract::tests::helpers::drift_messages;
use crate::contract::tests::test_types::FileDriftTestCase;

#[test]
fn given_every_files_columns_when_comparing_with_the_newest_load_then_each_drifting_column_is_one_finding_per_kind()
 {
    let test_cases = [
        FileDriftTestCase {
            description: "files that agree with the newest load, apart from SQLLoad's own columns, give nothing",
            live: vec![
                ("id", ColumnType::Int),
                ("_sqlload_load_id", ColumnType::Text),
            ],
            files: vec![
                ("a.parquet", vec![("id", ColumnType::Int)]),
                (
                    "b.parquet",
                    vec![
                        ("id", ColumnType::Int),
                        ("_sqlload_load_id", ColumnType::Text),
                    ],
                ),
            ],
            expected_messages: &[],
        },
        FileDriftTestCase {
            description: "another type, a missing column and an extra column are each named with their files",
            live: vec![("id", ColumnType::Int), ("code", ColumnType::Text)],
            files: vec![
                (
                    "day=1/a.parquet",
                    vec![
                        ("id", ColumnType::Int),
                        ("code", ColumnType::Int),
                        ("old", ColumnType::Bool),
                    ],
                ),
                ("day=2/b.parquet", vec![("id", ColumnType::Int)]),
                (
                    "day=3/c.parquet",
                    vec![("id", ColumnType::Int), ("code", ColumnType::Text)],
                ),
            ],
            expected_messages: &[
                "files_type_mismatch: column 'code' is text in the newest load but int in 1 file: day=1/a.parquet",
                "files_missing_column: column 'code' is absent from 1 file: day=2/b.parquet",
                "files_extra_column: column 'old' (bool) is in 1 file: day=1/a.parquet but not in the newest load",
            ],
        },
        FileDriftTestCase {
            description: "names are matched exactly, as Parquet readers match them, and long file lists are cut after three",
            live: vec![("ID", ColumnType::Int)],
            files: vec![
                ("1.parquet", vec![("id", ColumnType::Int)]),
                ("2.parquet", vec![("id", ColumnType::Int)]),
                ("3.parquet", vec![("id", ColumnType::Int)]),
                ("4.parquet", vec![("id", ColumnType::Int)]),
            ],
            expected_messages: &[
                "files_missing_column: column 'ID' is absent from 4 files: 1.parquet, 2.parquet, 3.parquet and 1 more",
                "files_extra_column: column 'id' (int) is in 4 files: 1.parquet, 2.parquet, 3.parquet and 1 more but not in the newest load",
            ],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            drift_messages(&test_case),
            test_case.expected_messages,
            "{}",
            test_case.description
        );
    }
}
