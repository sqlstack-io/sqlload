use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Decimal128Type;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use crate::odbc_source::_helpers::convert::converted;
use crate::odbc_source::tests::test_types::ConvertedDecimalTestCase;

#[test]
fn given_decimal_text_when_converting_then_it_loads_exactly_or_the_read_fails() {
    let test_cases = [
        ConvertedDecimalTestCase {
            description: "digits within the scale load as written",
            text: "-2.5",
            expected_value: Some(-2_500_000),
        },
        ConvertedDecimalTestCase {
            description: "zeros past the scale lose nothing",
            text: "1.1234560000",
            expected_value: Some(1_123_456),
        },
        ConvertedDecimalTestCase {
            description: "an exponent moving digits inside the scale",
            text: "12e-6",
            expected_value: Some(12),
        },
        ConvertedDecimalTestCase {
            description: "a digit past the scale fails rather than rounding",
            text: "1.123456789",
            expected_value: None,
        },
        ConvertedDecimalTestCase {
            description: "a tiny value that would round to zero fails",
            text: "0.0000001",
            expected_value: None,
        },
        ConvertedDecimalTestCase {
            description: "an exponent moving a digit past the scale fails",
            text: "15e-7",
            expected_value: None,
        },
    ];
    let schema = Arc::new(Schema::new(vec![Field::new(
        "n",
        DataType::Decimal128(28, 6),
        true,
    )]));
    for test_case in test_cases {
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![test_case.text]))],
        )
        .map_err(|error| error.to_string());
        let loaded = input
            .and_then(|batch| converted(&batch, &schema))
            .ok()
            .map(|batch| batch.column(0).as_primitive::<Decimal128Type>().value(0));
        assert_eq!(
            loaded, test_case.expected_value,
            "{}",
            test_case.description
        );
    }
}
