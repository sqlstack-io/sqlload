use crate::postgres_source::main::decode_copy::decode_copy;
use crate::postgres_source::tests::helpers::{sample_columns, sample_stream};
use crate::postgres_source::tests::test_types::DecodeCopyTestCase;

#[test]
fn given_an_in_memory_copy_stream_when_decoding_then_batches_close_at_either_limit() {
    let test_cases = [
        DecodeCopyTestCase {
            description: "no limit reached gives one batch",
            cut: 0,
            limits: (usize::MAX, usize::MAX),
            expected_result: Ok(&[3]),
        },
        DecodeCopyTestCase {
            description: "the row limit splits the stream",
            cut: 0,
            limits: (2, usize::MAX),
            expected_result: Ok(&[2, 1]),
        },
        DecodeCopyTestCase {
            description: "the byte limit splits the stream with rows to spare",
            cut: 0,
            limits: (usize::MAX, 1),
            expected_result: Ok(&[1, 1, 1]),
        },
        DecodeCopyTestCase {
            description: "a stream without its trailer fails",
            cut: 2,
            limits: (usize::MAX, usize::MAX),
            expected_result: Err(
                "malformed COPY data from public.sample: the COPY stream ended without its trailer",
            ),
        },
    ];
    for test_case in test_cases {
        let stream = sample_stream();
        let whole = &stream[..stream.len() - test_case.cut];
        let rows = decode_copy(
            "public.sample",
            sample_columns(),
            whole.chunks(5),
            test_case.limits,
        )
        .map(|batches| {
            batches
                .iter()
                .map(|batch| batch.num_rows())
                .collect::<Vec<_>>()
        })
        .map_err(|error| error.to_string());
        assert_eq!(
            rows.as_deref().map_err(String::as_str),
            test_case.expected_result,
            "{}",
            test_case.description
        );
    }
}
