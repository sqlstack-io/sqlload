use crate::batch_plan::tests::helpers::bound_text;
use crate::batch_plan::tests::test_types::BoundArrayTestCase;
use crate::column_types::models::ColumnType;

#[test]
fn given_a_cursor_bound_when_building_it_then_it_takes_the_cursor_columns_arrow_type()
-> Result<(), String> {
    let test_cases = [
        BoundArrayTestCase {
            description: "decimal",
            text: "12.5",
            cursor_type: ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            expected_value: "Decimal128(18, 2) 12.50",
        },
        BoundArrayTestCase {
            description: "smallint",
            text: "3",
            cursor_type: ColumnType::SmallInt,
            expected_value: "Int16 3",
        },
        BoundArrayTestCase {
            description: "int",
            text: "42",
            cursor_type: ColumnType::Int,
            expected_value: "Int64 42",
        },
        BoundArrayTestCase {
            description: "timestamp with a time zone, as the destination's maximum",
            text: "2026-01-02 09:00:00.000000+00",
            cursor_type: ColumnType::TimestampTz,
            expected_value: "Timestamp(µs, \"UTC\") 2026-01-02T09:00:00Z",
        },
        BoundArrayTestCase {
            description: "date",
            text: "2026-01-02",
            cursor_type: ColumnType::Date,
            expected_value: "Date32 2026-01-02",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            bound_text(&test_case)?,
            test_case.expected_value,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
