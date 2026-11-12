use crate::adapters::contract::main::bound_parameter::bound_parameter;
use crate::adapters::contract::models::BoundParameter;
use crate::adapters::contract::tests::helpers::{date_parameter, timestamp_parameter};
use crate::adapters::contract::tests::test_types::BoundParameterTestCase;
use crate::column_types::models::ColumnType;

#[test]
fn given_a_cursor_bound_when_binding_it_for_an_odbc_source_then_text_dates_and_wall_clock_times_keep_their_type_and_every_digit()
-> Result<(), String> {
    let test_cases = [
        BoundParameterTestCase {
            description: "the millisecond maximum a cursor resumes from keeps its milliseconds exactly",
            value: "2026-01-31 23:59:59.997000",
            column_type: ColumnType::TimestampNtz,
            expected_parameter: Some(timestamp_parameter("2026-01-31 23:59:59.997")?),
        },
        BoundParameterTestCase {
            description: "a microsecond bound keeps all six digits",
            value: "2026-01-31 23:59:59.000001",
            column_type: ColumnType::TimestampNtz,
            expected_parameter: Some(timestamp_parameter("2026-01-31 23:59:59.000001")?),
        },
        BoundParameterTestCase {
            description: "a bound written with a T and no fraction reads the same",
            value: "2026-02-01T08:30:00",
            column_type: ColumnType::TimestampNtz,
            expected_parameter: Some(timestamp_parameter("2026-02-01 08:30:00")?),
        },
        BoundParameterTestCase {
            description: "a date is a date",
            value: "2026-02-01",
            column_type: ColumnType::Date,
            expected_parameter: Some(date_parameter("2026-02-01")?),
        },
        BoundParameterTestCase {
            description: "a text bound is text, whatever it holds",
            value: "o'brien\\",
            column_type: ColumnType::Varchar(40),
            expected_parameter: Some(BoundParameter::Text("o'brien\\".to_owned())),
        },
        BoundParameterTestCase {
            description: "a zoned timestamp is not bound, since an ODBC timestamp carries no offset",
            value: "2026-01-31 23:59:59.997000+00",
            column_type: ColumnType::TimestampTz,
            expected_parameter: None,
        },
        BoundParameterTestCase {
            description: "a value that is not a timestamp is not bound as one",
            value: "2026-01-31'} OR 1=1 --",
            column_type: ColumnType::TimestampNtz,
            expected_parameter: None,
        },
        BoundParameterTestCase {
            description: "a date cursor given a timestamp is not bound",
            value: "2026-02-01 00:00:00",
            column_type: ColumnType::Date,
            expected_parameter: None,
        },
        BoundParameterTestCase {
            description: "a year an ODBC date can't hold is not bound",
            value: "+10000-01-01",
            column_type: ColumnType::Date,
            expected_parameter: None,
        },
        BoundParameterTestCase {
            description: "an integer cursor's bound is a number, not a parameter",
            value: "42",
            column_type: ColumnType::Int,
            expected_parameter: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            bound_parameter(test_case.value, &test_case.column_type),
            test_case.expected_parameter,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
