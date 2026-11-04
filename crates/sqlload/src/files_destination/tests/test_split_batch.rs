use crate::files_destination::_helpers::split::split_batch;
use crate::files_destination::models::PartitionBy;
use crate::files_destination::tests::helpers::instants_batch;
use crate::files_destination::tests::test_types::SplitBatchTestCase;

/// 2026-11-03 12:00 UTC.
const NOV_3_NOON: i64 = 1_793_707_200_000_000;
/// 2026-11-04 00:00 UTC.
const NOV_4: i64 = 1_793_750_400_000_000;

#[test]
fn given_rows_when_splitting_by_the_cursor_then_each_partition_gets_its_rows() -> Result<(), String>
{
    let test_cases = [
        SplitBatchTestCase {
            description: "timestamps split by their UTC date",
            instants: &[Some(NOV_4), Some(NOV_3_NOON), Some(NOV_4 - 1)],
            by: PartitionBy::Date,
            expected_parts: Ok(&[("2026-11-03", 2), ("2026-11-04", 1)]),
        },
        SplitBatchTestCase {
            description: "by value every distinct cursor value is a partition",
            instants: &[Some(NOV_4), Some(NOV_4)],
            by: PartitionBy::Value,
            expected_parts: Ok(&[("2026-11-04T00:00:00Z", 2)]),
        },
        SplitBatchTestCase {
            description: "a NULL cursor has no partition",
            instants: &[Some(NOV_4), None],
            by: PartitionBy::Date,
            expected_parts: Err("is NULL in some rows"),
        },
    ];
    for test_case in test_cases {
        let batch = instants_batch(test_case.instants)?;
        let parts = split_batch(&batch, "updated_at", test_case.by).map(|parts| {
            parts
                .iter()
                .map(|(value, part)| (value.clone(), part.num_rows()))
                .collect::<Vec<(String, usize)>>()
        });
        let expected: Option<Vec<(String, usize)>> = test_case.expected_parts.ok().map(|parts| {
            parts
                .iter()
                .map(|(value, rows)| ((*value).to_owned(), *rows))
                .collect()
        });
        assert_eq!(
            parts.as_ref().ok(),
            expected.as_ref(),
            "{}",
            test_case.description
        );
        assert!(
            parts
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_parts.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
