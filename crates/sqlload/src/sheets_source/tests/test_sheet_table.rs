use crate::sheets_source::_helpers::cells::parse_grid;
use crate::sheets_source::main::sheet_table::sheet_table;
use crate::sheets_source::models::HeaderRule;
use crate::sheets_source::tests::helpers::{blank, error, flag, number, sheet, text};
use crate::sheets_source::tests::test_types::SheetTableTestCase;

#[test]
fn given_a_tab_when_reading_it_as_a_table_then_columns_are_named_by_the_header_and_typed_by_every_cell()
 {
    let header = HeaderRule::default();
    let test_cases = [
        SheetTableTestCase {
            description: "dates, timestamps and times by their format, whole and fractional numbers together as float, a whole percentage as float, a number stored as text kept as text, a ragged row padded and an empty row skipped",
            sheet: sheet(
                "Locations",
                (0, 0),
                vec![
                    vec![
                        text("Code"),
                        text("Opened"),
                        text("Active"),
                        text("Capacity"),
                        text("Ratio"),
                        text("Updated At"),
                        text("Opens"),
                        text("Ref"),
                        text("Share"),
                    ],
                    vec![
                        text("L1"),
                        number(46037.0, "2026-01-15", "DATE"),
                        flag(true),
                        number(120.0, "120", ""),
                        number(0.25, "0.25", ""),
                        number(46334.395833333336, "2026-11-08 09:30:00", "DATE_TIME"),
                        number(0.3333333333333333, "08:00", "TIME"),
                        text("00123"),
                        number(1.0, "100%", "PERCENT"),
                    ],
                    vec![
                        text("L2"),
                        number(46022.0, "2025-12-31", "DATE"),
                        flag(false),
                        number(80.0, "80", ""),
                        number(2.0, "2", ""),
                    ],
                    vec![blank(), blank()],
                    vec![text("L3")],
                ],
            ),
            rule: header,
            expected: Ok((
                "Code:text,Opened:date,Active:bool,Capacity:int,Ratio:float,Updated At:timestamp_ntz,Opens:time,Ref:text,Share:float",
                3,
            )),
            expected_warnings: &[],
        },
        SheetTableTestCase {
            description: "a column mixing numbers, text and booleans loads as text, with a warning naming it; a date with a timestamp is a timestamp",
            sheet: sheet(
                "Codes",
                (0, 0),
                vec![
                    vec![text("mixed"), text("when")],
                    vec![
                        number(10.0, "10", ""),
                        number(46037.0, "2026-01-15", "DATE"),
                    ],
                    vec![
                        text("a value"),
                        number(46037.5, "2026-01-15 12:00:00", "DATE_TIME"),
                    ],
                    vec![flag(true)],
                ],
            ),
            rule: header,
            expected: Ok(("mixed:text,when:timestamp_ntz", 3)),
            expected_warnings: &[
                "Codes: column mixed holds int and text and bool values, so it loads as text as the sheet shows it; pin a type in columns to load it as another",
            ],
        },
        SheetTableTestCase {
            description: "the header in a later row of a range that starts below the top: the rows above are skipped, and cells right of the header warned about by cell",
            sheet: sheet(
                "Report",
                (1, 1),
                vec![
                    vec![text("A report")],
                    vec![text("Item"), text("Qty")],
                    vec![text("alpha"), number(1.0, "1", ""), text("stray")],
                ],
            ),
            rule: HeaderRule {
                header: true,
                header_row: 2,
            },
            expected: Ok(("Item:text,Qty:int", 1)),
            expected_warnings: &[
                "Report: cells right of the header, such as Report!D4, are not loaded; name their column in the header to load them",
            ],
        },
        SheetTableTestCase {
            description: "without a header, columns are numbered up to the widest row and every row loads",
            sheet: sheet(
                "Plain",
                (0, 0),
                vec![
                    vec![text("a"), number(1.0, "1", "")],
                    vec![text("b"), number(2.0, "2", ""), text("x")],
                ],
            ),
            rule: HeaderRule {
                header: false,
                header_row: 1,
            },
            expected: Ok(("column_1:text,column_2:int,column_3:text", 2)),
            expected_warnings: &[],
        },
        SheetTableTestCase {
            description: "a header-only tab is a table of text columns without rows",
            sheet: sheet("Header Only", (0, 0), vec![vec![text("id"), text("name")]]),
            rule: header,
            expected: Ok(("id:text,name:text", 0)),
            expected_warnings: &[],
        },
        SheetTableTestCase {
            description: "an error cell makes its column one that cannot load, naming the cell",
            sheet: sheet(
                "Errors",
                (0, 0),
                vec![
                    vec![text("id"), text("ratio")],
                    vec![number(1.0, "1", ""), error("#DIV/0!")],
                ],
            ),
            rule: header,
            expected: Ok((
                "id:int,ratio:error Errors!B2 holds the error #DIV/0!; fix the formula, or leave the row out of the range",
                1,
            )),
            expected_warnings: &[],
        },
        SheetTableTestCase {
            description: "a blank header cell between named ones and a name used twice ignoring case are refused by cell, the tab's title quoted",
            sheet: sheet(
                "Blank Header",
                (0, 0),
                vec![
                    vec![text("id"), blank(), text("ID")],
                    vec![number(1.0, "1", "")],
                ],
            ),
            rule: header,
            expected: Err(
                "'Blank Header'!B1: the header cell is blank; name the column, or narrow the range; 'Blank Header'!C1: the header \"ID\" names an earlier column too (ignoring case); rename one",
            ),
            expected_warnings: &[],
        },
        SheetTableTestCase {
            description: "without a header, a range with no cells from its first row has no columns to load",
            sheet: sheet("Plain", (0, 0), vec![vec![text("a")], vec![text("b")]]),
            rule: HeaderRule {
                header: false,
                header_row: 3,
            },
            expected: Err(
                "Plain: the range has no cells from its row 3; check the range and header_row",
            ),
            expected_warnings: &[],
        },
        SheetTableTestCase {
            description: "an empty tab has no header to name columns with",
            sheet: sheet("Empty", (0, 0), Vec::new()),
            rule: header,
            expected: Err(
                "Empty: the range has no header in its row 1; set header_row, or header = false",
            ),
            expected_warnings: &[],
        },
    ];
    for test_case in test_cases {
        let read = sheet_table(&parse_grid(&test_case.sheet), test_case.rule);
        let seen = read.as_ref().map(|table| {
            let columns: Vec<String> = table
                .columns
                .iter()
                .map(|column| {
                    let kind = column
                        .column_type
                        .as_ref()
                        .map_or_else(|reason| format!("error {reason}"), ToString::to_string);
                    format!("{}:{kind}", column.name)
                })
                .collect();
            (columns.join(","), table.rows)
        });
        let warnings: Vec<String> = read
            .as_ref()
            .map(|table| table.warnings.clone())
            .unwrap_or_default();
        assert_eq!(
            seen.map_err(|problems| problems.join("; ")),
            test_case
                .expected
                .map(|(columns, rows)| (columns.to_owned(), rows))
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
        assert_eq!(
            warnings, test_case.expected_warnings,
            "{}",
            test_case.description
        );
    }
}
