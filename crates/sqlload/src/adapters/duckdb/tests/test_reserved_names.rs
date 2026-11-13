use crate::adapters::duckdb::_helpers::direct_checks::reserved_name;
use crate::adapters::duckdb::tests::test_types::ReservedNameTestCase;
use crate::column_types::models::ColumnType;
use crate::files_source::models::{FileColumn, StoredRead};

#[test]
fn given_a_file_column_when_duckdb_would_read_the_files_itself_then_its_reader_names_are_refused() {
    let test_cases = [
        ReservedNameTestCase {
            description: "a file_index column in any case would replace the index that picks each row's per-file values",
            column: "File_Index",
            expected_refused: true,
        },
        ReservedNameTestCase {
            description: "an ordinary column loads",
            column: "file_indexes",
            expected_refused: false,
        },
    ];
    for test_case in test_cases {
        let column = FileColumn {
            name: test_case.column.to_owned(),
            column_type: Ok(ColumnType::Int),
            direct_refusal: None,
            stored_read: StoredRead::default(),
            lists: None,
        };
        assert_eq!(
            reserved_name(&column).is_some(),
            test_case.expected_refused,
            "{}",
            test_case.description
        );
    }
}
