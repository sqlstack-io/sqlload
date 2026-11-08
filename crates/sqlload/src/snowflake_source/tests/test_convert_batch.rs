use crate::column_types::models::ColumnType;
use crate::snowflake_source::tests::helpers::convert_values;
use crate::snowflake_source::tests::test_types::ConvertBatchTestCase;

#[test]
fn given_unloaded_values_when_converting_then_they_are_exact_and_a_value_that_does_not_fit_fails() {
    let test_cases = [
        ConvertBatchTestCase {
            description: "a zoned timestamp is exact to the microsecond, before the epoch too",
            unloaded: (18, 0),
            values: &[Some(1_794_112_872_123_456), Some(-1), None],
            output: ColumnType::TimestampTz,
            expected_values: Ok(vec![
                "2026-11-08T04:41:12.123456Z",
                "1969-12-31T23:59:59.999999Z",
                "",
            ]),
        },
        ConvertBatchTestCase {
            description: "a wall-clock timestamp has no zone",
            unloaded: (18, 0),
            values: &[Some(1_794_112_872_123_456)],
            output: ColumnType::TimestampNtz,
            expected_values: Ok(vec!["2026-11-08T04:41:12.123456"]),
        },
        ConvertBatchTestCase {
            description: "a time is microseconds since midnight",
            unloaded: (18, 0),
            values: &[Some(45_296_789_012)],
            output: ColumnType::Time,
            expected_values: Ok(vec!["12:34:56.789012"]),
        },
        ConvertBatchTestCase {
            description: "a 38-digit decimal keeps every digit",
            unloaded: (38, 10),
            values: &[Some(-12_345_678_901_234_567_890_123_456_789_012_345_678)],
            output: ColumnType::Decimal {
                precision: 38,
                scale: 10,
            },
            expected_values: Ok(vec!["-1234567890123456789012345678.9012345678"]),
        },
        ConvertBatchTestCase {
            description: "an integer wider than 64 bits fails rather than becoming NULL",
            unloaded: (38, 0),
            values: &[Some(100_000_000_000_000_000_000)],
            output: ColumnType::Int,
            expected_values: Err("value"),
        },
    ];
    for test_case in test_cases {
        let converted = convert_values(test_case.unloaded, test_case.values, &test_case.output)
            .map_err(|error| error.split(": ").next().unwrap_or_default().to_owned());
        assert_eq!(
            converted,
            test_case
                .expected_values
                .map(|values| values.into_iter().map(str::to_owned).collect())
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
