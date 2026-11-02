use std::time::{Duration, Instant};

use crate::batching::models::{BatchLimits, Batcher, BufferGauge};
use crate::batching::tests::helpers::row;
use crate::batching::tests::test_types::BatcherTestCase;

#[test]
fn given_buffered_rows_when_checking_limits_then_any_limit_makes_the_batch_due() {
    let test_cases = [
        BatcherTestCase {
            description: "below every limit the batch waits",
            rows: 2,
            row_bytes: 10,
            elapsed_secs: 1,
            other_streams_bytes: 0,
            expected_due: false,
        },
        BatcherTestCase {
            description: "the row limit flushes",
            rows: 5,
            row_bytes: 10,
            elapsed_secs: 1,
            other_streams_bytes: 0,
            expected_due: true,
        },
        BatcherTestCase {
            description: "the byte limit flushes",
            rows: 2,
            row_bytes: 600,
            elapsed_secs: 1,
            other_streams_bytes: 0,
            expected_due: true,
        },
        BatcherTestCase {
            description: "the age limit flushes",
            rows: 1,
            row_bytes: 10,
            elapsed_secs: 31,
            other_streams_bytes: 0,
            expected_due: true,
        },
        BatcherTestCase {
            description: "the process-wide cap flushes a stream early",
            rows: 1,
            row_bytes: 10,
            elapsed_secs: 1,
            other_streams_bytes: 5000,
            expected_due: true,
        },
    ];
    for test_case in &test_cases {
        let gauge = BufferGauge::default();
        gauge.add(test_case.other_streams_bytes);
        let limits = BatchLimits {
            max_bytes: 1000,
            max_rows: 5,
            max_age: Duration::from_secs(30),
            max_total_bytes: 4000,
        };
        let mut batcher = Batcher::new(limits, gauge.clone());
        let start = Instant::now();
        for offset in 0..test_case.rows {
            batcher.push(row(offset as i64, "x".repeat(test_case.row_bytes)), start);
        }
        let now = start + Duration::from_secs(test_case.elapsed_secs);
        assert_eq!(
            batcher.is_due(now),
            test_case.expected_due,
            "{}",
            test_case.description
        );
        let taken = batcher.take().map(|batch| batch.rows.len());
        assert_eq!(taken, Some(test_case.rows), "{}", test_case.description);
        assert_eq!(
            gauge.get(),
            test_case.other_streams_bytes,
            "{}",
            test_case.description
        );
    }
}
