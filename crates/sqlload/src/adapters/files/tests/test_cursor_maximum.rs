use crate::adapters::files::main::cursor_maximum::cursor_maximum;
use crate::adapters::files::tests::helpers::text_footer;
use crate::adapters::files::tests::test_types::CursorMaximumTestCase;
use crate::column_types::models::ColumnType;

/// Longer than the 64 bytes Parquet truncates statistics to by default.
const LONG: &str = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz1";

#[test]
fn given_written_files_when_reading_the_cursor_maximum_then_long_text_is_exact()
-> Result<(), String> {
    let test_cases = [
        CursorMaximumTestCase {
            description: "short text",
            values: &["a", "c", "b"],
            expected_maximum: Some("c"),
        },
        CursorMaximumTestCase {
            description: "text longer than Parquet's default statistics truncation is kept whole",
            values: &["a", LONG],
            expected_maximum: Some(LONG),
        },
    ];
    for test_case in test_cases {
        let footer = text_footer(test_case.values)?;
        let maximum = cursor_maximum(&[footer], "code", &ColumnType::Text)?;
        assert_eq!(
            maximum.as_deref(),
            test_case.expected_maximum,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
