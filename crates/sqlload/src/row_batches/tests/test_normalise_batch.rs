use crate::row_batches::main::normalise_batch::normalise_batch;
use crate::row_batches::tests::helpers::{all_null, nested, render, widths};
use crate::row_batches::tests::test_types::NormaliseBatchTestCase;

#[test]
fn given_arrow_batches_when_normalising_then_types_match_inferred_rows_and_nested_is_json() {
    let test_cases = [
        NormaliseBatchTestCase {
            description: "narrow ints and floats widen to 64 bits, large strings are strings, timestamps are UTC microseconds, u64 beyond i64 is text",
            batch: widths,
            expected_batch: "i:Int64,f:Float64,s:Utf8,u:Int64,big:Utf8,t:Timestamp(µs, \"UTC\") / 1|0.5|a|1|1|2023-11-14T22:13:20.123456Z; NULL|1.25|b|2|18446744073709551615|1970-01-01T00:00:00Z",
        },
        NormaliseBatchTestCase {
            description: "lists and structs become JSON text in field order; a null row is NULL",
            batch: nested,
            expected_batch: "l:Utf8(json),r:Utf8(json) / [1,2]|{\"k\":\"x\\\"y\"}; NULL|{\"k\":null}",
        },
        NormaliseBatchTestCase {
            description: "a null column stays null until the schema settles",
            batch: all_null,
            expected_batch: "n:Null / NULL; NULL",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            normalise_batch(&(test_case.batch)()).map(|batch| render(&batch)),
            Ok(test_case.expected_batch.to_owned()),
            "{}",
            test_case.description
        );
    }
}
