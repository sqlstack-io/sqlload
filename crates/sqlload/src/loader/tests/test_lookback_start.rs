use crate::column_types::models::ColumnType;
use crate::loader::_helpers::table_plan::lookback_start;
use crate::loader::tests::test_types::LookbackStartTestCase;

#[test]
fn given_an_integer_lookback_when_counting_back_from_the_destination_maximum_then_the_start_stays_within_the_cursor_type()
-> Result<(), String> {
    let test_cases = [
        LookbackStartTestCase {
            description: "counts back the lookback's units",
            maximum: "2026",
            units: 1,
            cursor_type: ColumnType::Int,
            expected_start: Ok("2025"),
        },
        LookbackStartTestCase {
            description: "a smallint maximum at its lowest value stays there instead of leaving the type",
            maximum: "-32768",
            units: 1,
            cursor_type: ColumnType::SmallInt,
            expected_start: Ok("-32768"),
        },
        LookbackStartTestCase {
            description: "a smallint maximum near its lowest value stops at it",
            maximum: "-32760",
            units: 100,
            cursor_type: ColumnType::SmallInt,
            expected_start: Ok("-32768"),
        },
        LookbackStartTestCase {
            description: "an int maximum at its lowest value stays there",
            maximum: "-9223372036854775808",
            units: 5,
            cursor_type: ColumnType::Int,
            expected_start: Ok("-9223372036854775808"),
        },
        LookbackStartTestCase {
            description: "a maximum that isn't an integer can't be counted back from",
            maximum: "2026.5",
            units: 1,
            cursor_type: ColumnType::Int,
            expected_start: Err(
                "cursor maximum \"2026.5\" is not an integer, so lookback = 1 can't count back from it",
            ),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            lookback_start(test_case.maximum, test_case.units, &test_case.cursor_type).as_deref(),
            test_case.expected_start.map_err(str::to_owned).as_deref(),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
