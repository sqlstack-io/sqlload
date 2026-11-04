use crate::column_types::models::ColumnType;
use crate::files_destination::main::output_problems::output_problems;
use crate::files_destination::models::{OutputCheck, PartitionBy, PartitionFolder};
use crate::files_destination::tests::test_types::OutputProblemsTestCase;
use crate::project::models::WriteStrategy;

#[test]
fn given_a_strategy_and_path_when_checking_then_unsupported_combinations_are_named() {
    let test_cases = [
        OutputProblemsTestCase {
            description: "merge needs a table format",
            strategy: WriteStrategy::Merge,
            cursor_type: Some(ColumnType::TimestampTz),
            partition: Some(PartitionBy::Date),
            expected_problems: "merge needs a table format",
        },
        OutputProblemsTestCase {
            description: "delete_insert needs a partition folder",
            strategy: WriteStrategy::DeleteInsert,
            cursor_type: Some(ColumnType::TimestampTz),
            partition: None,
            expected_problems: "delete_insert with a cursor_column writes partitions by the cursor",
        },
        OutputProblemsTestCase {
            description: "an incremental append needs a partition folder",
            strategy: WriteStrategy::Append,
            cursor_type: Some(ColumnType::Int),
            partition: None,
            expected_problems: "the path needs {cursor} or {cursor:date}",
        },
        OutputProblemsTestCase {
            description: "a partition folder needs a cursor",
            strategy: WriteStrategy::Replace,
            cursor_type: None,
            partition: Some(PartitionBy::Value),
            expected_problems: "needs an incremental strategy",
        },
        OutputProblemsTestCase {
            description: "a timestamp cursor partitions by date",
            strategy: WriteStrategy::DeleteInsert,
            cursor_type: Some(ColumnType::TimestampTz),
            partition: Some(PartitionBy::Value),
            expected_problems: "use {cursor:date} for timestamps), but updated_at is timestamp_tz",
        },
        OutputProblemsTestCase {
            description: "an int cursor has no date",
            strategy: WriteStrategy::DeleteInsert,
            cursor_type: Some(ColumnType::Int),
            partition: Some(PartitionBy::Date),
            expected_problems: "{cursor:date} needs a date or timestamp cursor, but updated_at is int",
        },
        OutputProblemsTestCase {
            description: "delete_insert on a timestamp by date is fine",
            strategy: WriteStrategy::DeleteInsert,
            cursor_type: Some(ColumnType::TimestampNtz),
            partition: Some(PartitionBy::Date),
            expected_problems: "",
        },
        OutputProblemsTestCase {
            description: "a full append or replace needs no partitions",
            strategy: WriteStrategy::Replace,
            cursor_type: None,
            partition: None,
            expected_problems: "",
        },
    ];
    for test_case in test_cases {
        let partition = test_case.partition.map(|by| PartitionFolder {
            prefix: "p=".to_owned(),
            suffix: String::new(),
            by,
        });
        let cursor_column = test_case.cursor_type.as_ref().map(|_| "updated_at");
        let problems = output_problems(&OutputCheck {
            strategy: test_case.strategy,
            cursor_column,
            cursor_type: test_case.cursor_type.as_ref(),
            partition: partition.as_ref(),
        })
        .join("; ");
        assert!(
            problems.contains(test_case.expected_problems)
                && problems.is_empty() == test_case.expected_problems.is_empty(),
            "{}: {problems}",
            test_case.description
        );
    }
}
