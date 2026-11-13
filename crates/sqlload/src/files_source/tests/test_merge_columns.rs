use crate::column_types::models::ColumnType;
use crate::files_source::_helpers::plan::columns::{
    ColumnMerge, SampledColumn, file_columns, merge_columns,
};
use crate::files_source::tests::test_types::MergeColumnsTestCase;

#[test]
fn given_int_and_float_in_different_files_when_merging_then_text_formats_widen_and_parquet_blocks()
{
    let test_cases = [
        MergeColumnsTestCase {
            description: "JSON and CSV: an older float file widens a newer int column",
            widen_numbers: true,
            newer: ColumnType::Int,
            older: ColumnType::Float,
            expected_type: ColumnType::Float,
            expected_problems: 0,
        },
        MergeColumnsTestCase {
            description: "JSON and CSV: an older int file fits a newer float column",
            widen_numbers: true,
            newer: ColumnType::Float,
            older: ColumnType::Int,
            expected_type: ColumnType::Float,
            expected_problems: 0,
        },
        MergeColumnsTestCase {
            description: "JSON and CSV: int and text still conflict",
            widen_numbers: true,
            newer: ColumnType::Int,
            older: ColumnType::Text,
            expected_type: ColumnType::Int,
            expected_problems: 1,
        },
        MergeColumnsTestCase {
            description: "Parquet: int and float conflict",
            widen_numbers: false,
            newer: ColumnType::Int,
            older: ColumnType::Float,
            expected_type: ColumnType::Int,
            expected_problems: 1,
        },
    ];
    for test_case in test_cases {
        let column = |column_type: ColumnType| {
            vec![SampledColumn {
                name: "v".to_owned(),
                column_type: Some(Ok(column_type)),
                direct_refusal: None,
                stored_read: crate::files_source::models::StoredRead::Plain,
                lists: None,
            }]
        };
        let merge = ColumnMerge {
            key: "a",
            replaced: &[],
            widen_numbers: test_case.widen_numbers,
        };
        let (merged, problems) =
            merge_columns(column(test_case.newer), column(test_case.older), merge);
        let types: Vec<Result<ColumnType, String>> = file_columns(merged)
            .into_iter()
            .map(|column| column.column_type)
            .collect();
        assert_eq!(
            types,
            vec![Ok(test_case.expected_type)],
            "{}",
            test_case.description
        );
        assert_eq!(
            problems.len(),
            test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
}
