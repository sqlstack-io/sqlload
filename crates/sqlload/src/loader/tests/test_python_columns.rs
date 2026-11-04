use crate::column_types::models::ColumnType;
use crate::loader::_helpers::python::fitting_type;
use crate::loader::tests::helpers::{crossed_schema, inferred_columns, integer_code};
use crate::loader::tests::test_types::{FittingTypeTestCase, InferredTableTestCase};

#[test]
fn given_an_inferred_and_a_live_type_when_choosing_then_the_live_type_wins_when_the_values_fit() {
    let test_cases = [
        FittingTypeTestCase {
            description: "ints fit a text column",
            inferred: ColumnType::Int,
            live: Some(ColumnType::Text),
            expected_type: ColumnType::Text,
        },
        FittingTypeTestCase {
            description: "ints fit a float column",
            inferred: ColumnType::Int,
            live: Some(ColumnType::Float),
            expected_type: ColumnType::Float,
        },
        FittingTypeTestCase {
            description: "floats do not fit an int column, so planning decides about widening",
            inferred: ColumnType::Float,
            live: Some(ColumnType::Int),
            expected_type: ColumnType::Float,
        },
        FittingTypeTestCase {
            description: "text does not fit a timestamp column",
            inferred: ColumnType::Text,
            live: Some(ColumnType::TimestampTz),
            expected_type: ColumnType::Text,
        },
        FittingTypeTestCase {
            description: "no live column keeps the inferred type",
            inferred: ColumnType::Json,
            live: None,
            expected_type: ColumnType::Json,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            fitting_type(test_case.inferred, test_case.live),
            test_case.expected_type,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_crossed_batches_when_inferring_the_table_then_columns_follow_the_data_and_the_live_table()
{
    let test_cases = [
        InferredTableTestCase {
            description: "Arrow types map to portable types; metadata columns are not data; declared columns no row had are kept",
            schema: crossed_schema,
            declared: &["total"],
            live: &[],
            expected_columns: "id:int,score:float,code:text,at:timestamp_tz,meta:json,total:text",
        },
        InferredTableTestCase {
            description: "a value that fits the live column takes its type",
            schema: integer_code,
            declared: &[],
            live: &[("CODE", ColumnType::Text)],
            expected_columns: "code:text",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            inferred_columns(&test_case),
            test_case.expected_columns,
            "{}",
            test_case.description
        );
    }
}
