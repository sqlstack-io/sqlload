use crate::batch_plan::tests::helpers::in_order;
use crate::batch_plan::tests::test_types::ReadInOrderTestCase;

#[test]
fn given_reads_that_finish_out_of_order_when_taken_in_order_then_each_is_consumed_in_index_order_and_a_failure_stops_the_rest()
-> Result<(), String> {
    let test_cases = [
        ReadInOrderTestCase {
            description: "the last read finishes first, and every result is still consumed in index order",
            count: 4,
            limit: 4,
            failing_read: None,
            failing_consume: None,
            expected_outcome: (&[0, 1, 2, 3], (4, 4, 3), None),
        },
        ReadInOrderTestCase {
            description: "no more results are held at once than the limit, counting the one being consumed",
            count: 5,
            limit: 2,
            failing_read: None,
            failing_consume: None,
            expected_outcome: (&[0, 1, 2, 3, 4], (2, 2, 1), None),
        },
        ReadInOrderTestCase {
            description: "a failed middle read keeps only the results before it, though later reads finished first",
            count: 4,
            limit: 4,
            failing_read: Some(1),
            failing_consume: None,
            expected_outcome: (&[0], (4, 4, 3), Some("read 1 failed")),
        },
        ReadInOrderTestCase {
            description: "a failed consumption stops the results after it",
            count: 4,
            limit: 2,
            failing_read: None,
            failing_consume: Some(2),
            expected_outcome: (&[0, 1], (2, 2, 1), Some("commit 2 failed")),
        },
        ReadInOrderTestCase {
            description: "a limit of 0 reads one at a time",
            count: 3,
            limit: 0,
            failing_read: None,
            failing_consume: None,
            expected_outcome: (&[0, 1, 2], (1, 1, 0), None),
        },
    ];
    for test_case in test_cases {
        let (consumed, counts, error) = in_order(&test_case)?;
        assert_eq!(
            (consumed.as_slice(), counts, error.as_deref()),
            test_case.expected_outcome,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
