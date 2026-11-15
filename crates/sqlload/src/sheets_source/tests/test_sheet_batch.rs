use crate::column_types::models::ColumnType;
use crate::sheets_source::_helpers::cells::parse_grid;
use crate::sheets_source::main::sheet_batch::sheet_batch;
use crate::sheets_source::models::HeaderRule;
use crate::sheets_source::tests::helpers::{blank, error, flag, number, rows_text, sheet, text};
use crate::sheets_source::tests::test_types::SheetBatchTestCase;

#[test]
fn given_a_tab_when_reading_its_rows_then_each_cell_loads_as_its_value_in_the_column_type() {
    let typed = || {
        sheet(
            "Locations",
            (0, 0),
            vec![
                vec![
                    text("Code"),
                    text("Opened"),
                    text("Active"),
                    text("Capacity"),
                    text("Updated At"),
                    text("Opens"),
                    text("Ref"),
                    text("Share"),
                    text("Mixed"),
                ],
                vec![
                    text("L1"),
                    number(46037.0, "2026-01-15", "DATE"),
                    flag(true),
                    number(120.0, "120", ""),
                    number(46334.395833333336, "2026-11-08 09:30:00", "DATE_TIME"),
                    number(0.3854166666666667, "09:15", "TIME"),
                    text("00123"),
                    number(0.125, "12.50%", "PERCENT"),
                    number(10.0, "10", ""),
                ],
                vec![
                    text("L2"),
                    blank(),
                    flag(false),
                    number(-5.0, "-5", ""),
                    number(36526.0, "2000-01-01 00:00:00", "DATE_TIME"),
                    blank(),
                    text("007"),
                    number(0.5, "50%", "PERCENT"),
                    flag(true),
                ],
            ],
        )
    };
    let test_cases = [
        SheetBatchTestCase {
            description: "dates, timestamps and times from serial numbers, percentages as fractions, numbers stored as text with their zeros, blanks NULL, and a text column as the sheet shows each value",
            sheet: typed(),
            rule: HeaderRule::default(),
            columns: vec![
                ("Code", ColumnType::Text),
                ("Opened", ColumnType::Date),
                ("Active", ColumnType::Bool),
                ("Capacity", ColumnType::Int),
                ("Updated At", ColumnType::TimestampNtz),
                ("Opens", ColumnType::Time),
                ("Ref", ColumnType::Text),
                ("Share", ColumnType::Float),
                ("Mixed", ColumnType::Text),
            ],
            expected_result: Ok(
                "L1|2026-01-15|true|120|2026-11-08T09:30:00|09:15:00|00123|0.125|10;L2|NULL|false|-5|2000-01-01T00:00:00|NULL|007|0.5|TRUE",
            ),
        },
        SheetBatchTestCase {
            description: "a pinned type converts each cell, matched by header ignoring case: a date into a timestamp at midnight, a whole number into a decimal",
            sheet: typed(),
            rule: HeaderRule::default(),
            columns: vec![
                ("opened", ColumnType::TimestampNtz),
                (
                    "capacity",
                    ColumnType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                ),
            ],
            expected_result: Ok("2026-01-15T00:00:00|120.00;NULL|-5.00"),
        },
        SheetBatchTestCase {
            description: "a cell that does not convert to its pinned type is named",
            sheet: typed(),
            rule: HeaderRule::default(),
            columns: vec![("Code", ColumnType::Int)],
            expected_result: Err(
                "Locations!A2 holds \"L1\", which does not load as int; pin a type in columns that holds it",
            ),
        },
        SheetBatchTestCase {
            description: "an error cell fails the read, named",
            sheet: sheet(
                "Errors",
                (0, 0),
                vec![vec![text("ratio")], vec![error("#N/A")]],
            ),
            rule: HeaderRule::default(),
            columns: vec![("ratio", ColumnType::Float)],
            expected_result: Err("Errors!A2 holds the error #N/A"),
        },
        SheetBatchTestCase {
            description: "a formula's \"\" in a column pinned to float loads NULL, as a cell the answer leaves out does",
            sheet: sheet(
                "Months",
                (0, 0),
                vec![
                    vec![text("month"), text("rate")],
                    vec![text("Jan"), number(0.5, "0.5", "")],
                    vec![text("Feb"), text("")],
                    vec![text("Mar")],
                ],
            ),
            rule: HeaderRule::default(),
            columns: vec![("month", ColumnType::Text), ("rate", ColumnType::Float)],
            expected_result: Ok("Jan|0.5;Feb|NULL;Mar|NULL"),
        },
        SheetBatchTestCase {
            description: "_sqlload_source_row is each row's number in the sheet: a range starting at A3 starts at 3, and the skipped empty row leaves a gap",
            sheet: sheet(
                "Blocks",
                (2, 0),
                vec![
                    vec![text("M1"), number(5.0, "5", "")],
                    vec![blank(), number(6.0, "6", "")],
                    vec![blank(), blank()],
                    vec![text("M2"), number(7.0, "7", "")],
                ],
            ),
            rule: HeaderRule {
                header: false,
                ..HeaderRule::default()
            },
            columns: vec![
                ("column_1", ColumnType::Text),
                ("_sqlload_source_row", ColumnType::Int),
            ],
            expected_result: Ok("M1|3;NULL|4;M2|6"),
        },
        SheetBatchTestCase {
            description: "a column renamed in the sheet since plan fails rather than loading NULLs",
            sheet: typed(),
            rule: HeaderRule::default(),
            columns: vec![("Gone", ColumnType::Text)],
            expected_result: Err(
                "Locations: column Gone is no longer in the header; run plan again",
            ),
        },
    ];
    for test_case in test_cases {
        let columns: Vec<(String, ColumnType)> = test_case
            .columns
            .iter()
            .map(|(name, column_type)| ((*name).to_owned(), column_type.clone()))
            .collect();
        let seen = sheet_batch(&parse_grid(&test_case.sheet), &test_case.rule, &columns)
            .and_then(|batch| rows_text(&batch));
        assert_eq!(
            seen,
            test_case
                .expected_result
                .map(str::to_owned)
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
