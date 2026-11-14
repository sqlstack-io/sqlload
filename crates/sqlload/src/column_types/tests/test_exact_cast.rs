use std::sync::Arc;

use arrow_array::{Decimal128Array, Float32Array, Float64Array, Int64Array, StringArray};
use arrow_cast::display::array_value_to_string;
use arrow_schema::DataType;

use crate::column_types::main::exact_cast::exact_cast;
use crate::column_types::tests::test_types::ExactCastTestCase;

#[test]
fn given_a_pinned_narrower_type_when_casting_then_exact_values_convert_and_lost_digits_fail() {
    let test_cases = [
        ExactCastTestCase {
            description: "a float at a large scale converts, though Arrow's float arithmetic is inexact there",
            input: Arc::new(Float64Array::from(vec![437_548_739.228_1])),
            to: DataType::Decimal128(38, 9),
            expected: Ok("437548739.228100000"),
        },
        ExactCastTestCase {
            description: "a whole float converts at scale 18",
            input: Arc::new(Float64Array::from(vec![3_110_620.0])),
            to: DataType::Decimal128(38, 18),
            expected: Ok("3110620.000000000000000000"),
        },
        ExactCastTestCase {
            description: "negative zero converts to an integer",
            input: Arc::new(Float64Array::from(vec![-0.0])),
            to: DataType::Int64,
            expected: Ok("0"),
        },
        ExactCastTestCase {
            description: "a float32 converts by its own shortest text",
            input: Arc::new(Float32Array::from(vec![0.1])),
            to: DataType::Decimal128(10, 2),
            expected: Ok("0.10"),
        },
        ExactCastTestCase {
            description: "zero written with a negative exponent needs no places",
            input: Arc::new(StringArray::from(vec!["0E-10"])),
            to: DataType::Decimal128(10, 2),
            expected: Ok("0.00"),
        },
        ExactCastTestCase {
            description: "a float with more places than the scale fails",
            input: Arc::new(Float64Array::from(vec![1.0, 12.345])),
            to: DataType::Decimal128(10, 2),
            expected: Err("holds 12.345, which decimal(10,2) can't hold without losing digits"),
        },
        ExactCastTestCase {
            description: "a fractional float into an integer fails",
            input: Arc::new(Float64Array::from(vec![2.5])),
            to: DataType::Int64,
            expected: Err("holds 2.5, which int64 can't hold without losing digits"),
        },
        ExactCastTestCase {
            description: "decimal text with an exponent needing more places fails",
            input: Arc::new(StringArray::from(vec!["125E-3"])),
            to: DataType::Decimal128(10, 2),
            expected: Err("holds 125E-3, which decimal(10,2) can't hold without losing digits"),
        },
        ExactCastTestCase {
            description: "a decimal with more places than a smaller scale fails",
            input: Arc::new(
                Decimal128Array::from(vec![12_345])
                    .with_precision_and_scale(10, 3)
                    .expect("valid decimal"),
            ),
            to: DataType::Decimal128(10, 2),
            expected: Err("holds 12.345, which decimal(10,2) can't hold without losing digits"),
        },
        ExactCastTestCase {
            description: "an integer that fits a narrower integer converts",
            input: Arc::new(Int64Array::from(vec![-7])),
            to: DataType::Int16,
            expected: Ok("-7"),
        },
    ];
    for test_case in test_cases {
        let converted = exact_cast(&test_case.input, &test_case.to)
            .map(|array| array_value_to_string(&array, 0).expect("printable value"));
        match test_case.expected {
            Ok(expected) => assert_eq!(
                converted.as_deref(),
                Ok(expected),
                "{}",
                test_case.description
            ),
            Err(expected) => {
                let error = converted.expect_err(test_case.description);
                assert!(
                    error.contains(expected),
                    "{}: {error}",
                    test_case.description
                );
            }
        }
    }
}
