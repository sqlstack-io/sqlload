use crate::batching::main::compress_chunks::compress_chunks;
use crate::batching::models::BatchRow;
use crate::batching::tests::helpers::noisy_rows;
use crate::batching::tests::test_types::{CompressChunksTestCase, OversizedRowTestCase};

#[test]
fn given_partition_rows_when_compressing_then_every_body_fits_the_limit() -> Result<(), String> {
    let test_cases = [
        CompressChunksTestCase {
            description: "small rows fit in one body",
            rows: 100,
            random_bytes: 50,
            limit: 1_000_000,
            expected_min_chunks: 1,
        },
        CompressChunksTestCase {
            description: "incompressible rows split into several bodies",
            rows: 200,
            random_bytes: 2000,
            limit: 100_000,
            expected_min_chunks: 3,
        },
    ];
    for test_case in &test_cases {
        let rows = noisy_rows(test_case.rows, test_case.random_bytes);
        let borrowed: Vec<&BatchRow> = rows.iter().collect();
        let chunks =
            compress_chunks(&borrowed, test_case.limit).map_err(|row| format!("{row:?}"))?;
        assert!(
            chunks.len() >= test_case.expected_min_chunks,
            "{}",
            test_case.description
        );
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.body.len() as u64 <= test_case.limit),
            "{}",
            test_case.description
        );
        let total: usize = chunks.iter().map(|chunk| chunk.rows).sum();
        assert_eq!(total, test_case.rows, "{}", test_case.description);
        let last = chunks.last().map(|chunk| chunk.last_offset);
        assert_eq!(
            last,
            Some(test_case.rows as i64 - 1),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_one_row_over_the_limit_when_compressing_then_it_is_reported() {
    let test_cases = [OversizedRowTestCase {
        description: "one incompressible row larger than the limit",
        random_bytes: 300_000,
        limit: 100_000,
        expected_offset: 0,
    }];
    for test_case in &test_cases {
        let rows = noisy_rows(1, test_case.random_bytes);
        let borrowed: Vec<&BatchRow> = rows.iter().collect();
        let oversized =
            compress_chunks(&borrowed, test_case.limit).expect_err(test_case.description);
        assert_eq!(
            oversized.offset, test_case.expected_offset,
            "{}",
            test_case.description
        );
        assert!(
            oversized.compressed_bytes > test_case.limit,
            "{}",
            test_case.description
        );
    }
}
