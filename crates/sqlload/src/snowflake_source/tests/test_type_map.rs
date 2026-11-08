use crate::column_types::models::ColumnType;
use crate::snowflake_source::_helpers::type_map::{loaded_name, natural_type, unload_expression};
use crate::snowflake_source::tests::test_types::{
    LoadedNameTestCase, NaturalTypeTestCase, UnloadExpressionTestCase,
};

#[test]
fn given_a_snowflake_type_when_mapping_then_it_gets_the_portable_type_that_holds_every_value() {
    let test_cases = [
        NaturalTypeTestCase {
            description: "an 18-digit integer fits a 64-bit integer",
            type_text: "NUMBER(18,0)",
            expected_type: Ok(ColumnType::Int),
        },
        NaturalTypeTestCase {
            description: "a 19-digit integer does not, so it stays a decimal",
            type_text: "NUMBER(19,0)",
            expected_type: Ok(ColumnType::Decimal {
                precision: 19,
                scale: 0,
            }),
        },
        NaturalTypeTestCase {
            description: "INTEGER is NUMBER(38,0) in Snowflake",
            type_text: "INTEGER",
            expected_type: Ok(ColumnType::Decimal {
                precision: 38,
                scale: 0,
            }),
        },
        NaturalTypeTestCase {
            description: "a scaled number keeps its precision and scale",
            type_text: "NUMBER(38,10)",
            expected_type: Ok(ColumnType::Decimal {
                precision: 38,
                scale: 10,
            }),
        },
        NaturalTypeTestCase {
            description: "a bounded varchar keeps its length",
            type_text: "VARCHAR(40)",
            expected_type: Ok(ColumnType::Varchar(40)),
        },
        NaturalTypeTestCase {
            description: "the default varchar length is unbounded text",
            type_text: "VARCHAR(16777216)",
            expected_type: Ok(ColumnType::Text),
        },
        NaturalTypeTestCase {
            description: "a local-time-zone timestamp is an instant",
            type_text: "TIMESTAMP_LTZ(9)",
            expected_type: Ok(ColumnType::TimestampTz),
        },
        NaturalTypeTestCase {
            description: "a wall-clock timestamp stays wall-clock",
            type_text: "TIMESTAMP_NTZ(9)",
            expected_type: Ok(ColumnType::TimestampNtz),
        },
        NaturalTypeTestCase {
            description: "semi-structured values load as JSON",
            type_text: "VARIANT",
            expected_type: Ok(ColumnType::Json),
        },
        NaturalTypeTestCase {
            description: "a geography must be pinned",
            type_text: "GEOGRAPHY",
            expected_type: Err("GEOGRAPHY has no portable type; pin one, such as \"text\""),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            natural_type(test_case.type_text).map_err(|error| error.to_string()),
            test_case.expected_type.map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_column_when_unloading_then_times_and_timestamps_are_whole_microseconds_and_never_text() {
    let test_cases = [
        UnloadExpressionTestCase {
            description: "a zoned timestamp unloads as epoch microseconds, which are UTC whatever the offset",
            type_text: "TIMESTAMP_TZ(9)",
            output: ColumnType::TimestampTz,
            expected_expression: Some("DATE_PART(EPOCH_MICROSECOND, \"T\")::NUMBER(18,0)"),
        },
        UnloadExpressionTestCase {
            description: "a local-time-zone timestamp too",
            type_text: "TIMESTAMP_LTZ(9)",
            output: ColumnType::TimestampTz,
            expected_expression: Some("DATE_PART(EPOCH_MICROSECOND, \"T\")::NUMBER(18,0)"),
        },
        UnloadExpressionTestCase {
            description: "a time unloads as microseconds since midnight",
            type_text: "TIME(9)",
            output: ColumnType::Time,
            expected_expression: Some(
                "DATEDIFF(MICROSECOND, '00:00:00'::TIME, \"T\")::NUMBER(18,0)",
            ),
        },
        UnloadExpressionTestCase {
            description: "a zoned timestamp read as a date takes the UTC date",
            type_text: "TIMESTAMP_TZ(9)",
            output: ColumnType::Date,
            expected_expression: Some("CONVERT_TIMEZONE('UTC', \"T\")::DATE"),
        },
        UnloadExpressionTestCase {
            description: "a variant unloads as its JSON text",
            type_text: "VARIANT",
            output: ColumnType::Json,
            expected_expression: Some("TO_JSON(\"T\")"),
        },
        UnloadExpressionTestCase {
            description: "a pinned decimal is cast in Snowflake, so rounding happens there",
            type_text: "NUMBER(38,10)",
            output: ColumnType::Decimal {
                precision: 20,
                scale: 4,
            },
            expected_expression: Some("\"T\"::NUMBER(20,4)"),
        },
        UnloadExpressionTestCase {
            description: "text cannot be read as a timestamp",
            type_text: "VARCHAR(40)",
            output: ColumnType::TimestampTz,
            expected_expression: None,
        },
        UnloadExpressionTestCase {
            description: "a scaled number cannot be read as an integer",
            type_text: "NUMBER(10,2)",
            output: ColumnType::Int,
            expected_expression: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            unload_expression(test_case.type_text, &test_case.output, "\"T\"").as_deref(),
            test_case.expected_expression,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_snowflake_column_name_when_loading_then_only_plain_upper_case_names_fold_to_lower_case()
{
    let test_cases = [
        LoadedNameTestCase {
            description: "a plain upper-case name folds",
            name: "EVENT_ID$2",
            expected_name: "event_id$2",
        },
        LoadedNameTestCase {
            description: "a quoted mixed-case name is kept",
            name: "MixedCase",
            expected_name: "MixedCase",
        },
        LoadedNameTestCase {
            description: "a quoted lower-case name is kept",
            name: "amount",
            expected_name: "amount",
        },
        LoadedNameTestCase {
            description: "a name with a space is kept",
            name: "ORDER ID",
            expected_name: "ORDER ID",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            loaded_name(test_case.name),
            test_case.expected_name,
            "{}",
            test_case.description
        );
    }
}
