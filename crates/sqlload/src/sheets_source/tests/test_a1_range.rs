use crate::sheets_source::main::a1_range::a1_range;
use crate::sheets_source::tests::test_types::A1RangeTestCase;

#[test]
fn given_an_a1_range_when_reading_it_then_its_sheet_columns_and_rows_come_out_as_google_sheets_reads_them()
 {
    let forms = "write columns and optional rows, after an optional sheet, as in \"A:C\", \"Prices!A3:C\" or \"'Q4 Report'!B3:H10\"";
    let test_cases = [
        A1RangeTestCase {
            description: "columns only: the first sheet, every row",
            range: "A:C",
            expected_range: Ok("None 0..=2 rows 1..=None"),
        },
        A1RangeTestCase {
            description: "a sheet and an open-ended row range",
            range: "Prices!A3:C",
            expected_range: Ok("Some(\"Prices\") 0..=2 rows 3..=None"),
        },
        A1RangeTestCase {
            description: "a quoted sheet with a space and a doubled quote, two-letter columns, lower case",
            range: "'Q4 ''Final'' Report'!b3:ab10",
            expected_range: Ok("Some(\"Q4 'Final' Report\") 1..=27 rows 3..=Some(10)"),
        },
        A1RangeTestCase {
            description: "a row only on the last corner starts at row 1",
            range: "A:C5",
            expected_range: Ok("None 0..=2 rows 1..=Some(5)"),
        },
        A1RangeTestCase {
            description: "a single corner is not a range",
            range: "B3",
            expected_range: Err("needs two corners joined by :"),
        },
        A1RangeTestCase {
            description: "columns backwards",
            range: "C:A",
            expected_range: Err("ends at a column left of where it starts"),
        },
        A1RangeTestCase {
            description: "rows backwards",
            range: "A10:C3",
            expected_range: Err("ends at a row above where it starts"),
        },
        A1RangeTestCase {
            description: "an unclosed quote",
            range: "'Q4 Report!A:C",
            expected_range: Err("has a sheet name without its closing quote"),
        },
        A1RangeTestCase {
            description: "past Excel's last column",
            range: "A:XFE",
            expected_range: Err("has a last corner that is not a column and an optional row"),
        },
        A1RangeTestCase {
            description: "row 0",
            range: "A0:C",
            expected_range: Err("has a first corner that is not a column and an optional row"),
        },
    ];
    for test_case in test_cases {
        let read = a1_range(test_case.range).map(|range| {
            format!(
                "{:?} {}..={} rows {}..={:?}",
                range.sheet, range.first_column, range.last_column, range.first_row, range.last_row
            )
        });
        assert_eq!(
            read,
            test_case
                .expected_range
                .map(str::to_owned)
                .map_err(|why| format!("range {:?} {why}; {forms}", test_case.range)),
            "{}",
            test_case.description
        );
    }
}
