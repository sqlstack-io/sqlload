use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Int64Type};

use crate::postgres_source::_helpers::copy_decoder::CopyDecoder;
use crate::postgres_source::tests::helpers::{sample_columns, sample_stream};
use crate::postgres_source::tests::test_types::CopyDecoderTestCase;

#[test]
fn given_a_copy_stream_in_any_chunking_when_decoding_then_the_same_batch_comes_out()
-> Result<(), String> {
    let stream = sample_stream();
    let test_cases = [
        CopyDecoderTestCase {
            description: "the whole stream at once",
            chunk_size: stream.len(),
            expected_rows: 3,
        },
        CopyDecoderTestCase {
            description: "one byte at a time",
            chunk_size: 1,
            expected_rows: 3,
        },
        CopyDecoderTestCase {
            description: "seven bytes at a time, splitting fields",
            chunk_size: 7,
            expected_rows: 3,
        },
    ];
    for test_case in test_cases {
        let mut decoder = CopyDecoder::new("public.sample", sample_columns())
            .map_err(|error| error.to_string())?;
        for chunk in stream.chunks(test_case.chunk_size) {
            decoder.push(chunk).map_err(|error| error.to_string())?;
        }
        assert!(decoder.finished(), "{}", test_case.description);
        assert_eq!(
            decoder.rows(),
            test_case.expected_rows,
            "{}",
            test_case.description
        );
        let batch = decoder.take_batch().map_err(|error| error.to_string())?;
        let ids: Vec<i64> = batch
            .column(0)
            .as_primitive::<Int64Type>()
            .values()
            .to_vec();
        let names: Vec<Option<&str>> = batch.column(1).as_string::<i32>().iter().collect();
        let totals: Vec<Option<i128>> = batch
            .column(2)
            .as_primitive::<Decimal128Type>()
            .iter()
            .collect();
        assert_eq!(ids, [1, 2, 3], "{}", test_case.description);
        assert_eq!(
            names,
            [Some("first"), None, Some("third")],
            "{}",
            test_case.description
        );
        assert_eq!(
            totals,
            [Some(1234), None, Some(-50)],
            "{}",
            test_case.description
        );
    }
    Ok(())
}
