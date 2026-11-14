use std::sync::Arc;

use arrow_array::{Decimal128Array, Float32Array, Float64Array, Int64Array, StringArray};
use arrow_cast::display::array_value_to_string;
use arrow_schema::{DataType, Field};

use crate::column_types::main::exact_cast::exact_cast;
use crate::column_types::tests::helpers::float_lists;
use crate::column_types::tests::test_types::{ExactCastErrorTestCase, ExactCastTestCase};

#[test]
fn given_a_pinned_narrower_type_when_every_value_fits_exactly_then_it_converts() {
    let test_cases = [
        ExactCastTestCase {
            description: "a float at a large scale converts, though Arrow's float arithmetic is inexact there",
            input: Arc::new(Float64Array::from(vec![437_548_739.228_1])),
            to: DataType::Decimal128(38, 9),
            expected_value: "437548739.228100000",
        },
        ExactCastTestCase {
            description: "a whole float converts at scale 18",
            input: Arc::new(Float64Array::from(vec![3_110_620.0])),
            to: DataType::Decimal128(38, 18),
            expected_value: "3110620.000000000000000000",
        },
        ExactCastTestCase {
            description: "negative zero converts to an integer",
            input: Arc::new(Float64Array::from(vec![-0.0])),
            to: DataType::Int64,
            expected_value: "0",
        },
        ExactCastTestCase {
            description: "a float32 converts by its own shortest text",
            input: Arc::new(Float32Array::from(vec![0.1])),
            to: DataType::Decimal128(10, 2),
            expected_value: "0.10",
        },
        ExactCastTestCase {
            description: "zero written with a negative exponent needs no places",
            input: Arc::new(StringArray::from(vec!["0E-10"])),
            to: DataType::Decimal128(10, 2),
            expected_value: "0.00",
        },
        ExactCastTestCase {
            description: "an integer that fits a narrower integer converts",
            input: Arc::new(Int64Array::from(vec![-7])),
            to: DataType::Int16,
            expected_value: "-7",
        },
        ExactCastTestCase {
            description: "a sliced list is held only to the elements of its own rows",
            input: float_lists(&[&[1.5], &[2.0]]).slice(1, 1),
            to: DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            expected_value: "[2]",
        },
        ExactCastTestCase {
            description: "a list of whole floats converts to a list of integers",
            input: float_lists(&[&[1.0, 2.0]]),
            to: DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            expected_value: "[1, 2]",
        },
    ];
    for test_case in test_cases {
        let converted = exact_cast(&test_case.input, &test_case.to)
            .map(|array| array_value_to_string(&array, 0).expect("printable value"));
        assert_eq!(
            converted.as_deref(),
            Ok(test_case.expected_value),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_pinned_narrower_type_when_a_value_would_lose_digits_then_the_cast_fails_naming_it() {
    let test_cases = [
        ExactCastErrorTestCase {
            description: "a float with more places than the scale fails",
            input: Arc::new(Float64Array::from(vec![1.0, 12.345])),
            to: DataType::Decimal128(10, 2),
            expected_message: "holds 12.345, which decimal(10,2) can't hold without losing digits",
        },
        ExactCastErrorTestCase {
            description: "a fractional float into an integer fails",
            input: Arc::new(Float64Array::from(vec![2.5])),
            to: DataType::Int64,
            expected_message: "holds 2.5, which int64 can't hold without losing digits",
        },
        ExactCastErrorTestCase {
            description: "decimal text with an exponent needing more places fails",
            input: Arc::new(StringArray::from(vec!["125E-3"])),
            to: DataType::Decimal128(10, 2),
            expected_message: "holds 125E-3, which decimal(10,2) can't hold without losing digits",
        },
        ExactCastErrorTestCase {
            description: "a decimal with more places than a smaller scale fails",
            input: Arc::new(
                Decimal128Array::from(vec![12_345])
                    .with_precision_and_scale(10, 3)
                    .expect("valid decimal"),
            ),
            to: DataType::Decimal128(10, 2),
            expected_message: "holds 12.345, which decimal(10,2) can't hold without losing digits",
        },
        ExactCastErrorTestCase {
            description: "a list element with a fraction into a list of integers fails",
            input: float_lists(&[&[2.0, 1.5]]),
            to: DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            expected_message: "an element holds 1.5, which int64 can't hold without losing digits",
        },
    ];
    for test_case in test_cases {
        let error = exact_cast(&test_case.input, &test_case.to).expect_err(test_case.description);
        assert!(
            error.contains(test_case.expected_message),
            "{}: {error}",
            test_case.description
        );
    }
}
