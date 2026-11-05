use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};

use crate::row_batches::main::text_rows::text_rows;
use crate::row_batches::tests::test_types::TextRowsTestCase;

#[test]
fn given_a_batch_when_turning_it_into_text_then_each_value_reads_back_and_nulls_stay_null()
-> Result<(), String> {
    let column = |array: ArrayRef| array;
    let test_cases = [
        TextRowsTestCase {
            description: "an integer and its NULL",
            column: column(Arc::new(Int64Array::from(vec![Some(-7), None]))),
            expected_values: &[Some("-7"), None],
        },
        TextRowsTestCase {
            description: "text keeps an empty value apart from NULL",
            column: column(Arc::new(StringArray::from(vec![Some(""), None]))),
            expected_values: &[Some(""), None],
        },
        TextRowsTestCase {
            description: "binary is hex",
            column: column(Arc::new(BinaryArray::from_vec(vec![&[0xde, 0xad][..]]))),
            expected_values: &[Some("dead")],
        },
        TextRowsTestCase {
            description: "booleans and floats",
            column: column(Arc::new(BooleanArray::from(vec![true]))),
            expected_values: &[Some("true")],
        },
        TextRowsTestCase {
            description: "a float in full",
            column: column(Arc::new(Float64Array::from(vec![1.5]))),
            expected_values: &[Some("1.5")],
        },
        TextRowsTestCase {
            description: "an instant in RFC 3339",
            column: column(Arc::new(
                TimestampMicrosecondArray::from(vec![1_500_000]).with_timezone("UTC"),
            )),
            expected_values: &[Some("1970-01-01T00:00:01.500Z")],
        },
    ];
    for test_case in test_cases {
        let batch = RecordBatch::try_from_iter([("value", test_case.column)])
            .map_err(|error| error.to_string())?;
        assert_eq!(
            text_rows(&batch)?.into_iter().flatten().collect::<Vec<_>>(),
            test_case
                .expected_values
                .iter()
                .map(|value| value.map(str::to_owned))
                .collect::<Vec<_>>(),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
