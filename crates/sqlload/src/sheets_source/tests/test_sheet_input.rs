use crate::sheets_source::_helpers::cells::parse_grid;
use crate::sheets_source::main::sheet_input::sheet_input;
use crate::sheets_source::models::HeaderRule;
use crate::sheets_source::tests::helpers::{error, kpis, rows_text, sheet, text};
use crate::sheets_source::tests::test_types::SheetInputTestCase;

#[test]
fn given_a_range_with_a_transform_when_reading_its_input_then_empty_texts_are_null_and_numbers_in_text_columns_their_values()
 {
    let test_cases = [
        SheetInputTestCase {
            description: "rows above header_row are skipped; a formula's \"\" is NULL and types nothing, so Jan and Mar are float; a percentage among text is its value, 0.9, not 90.0%; a blank row is left out",
            sheet: kpis(),
            rule: HeaderRule {
                header_row: 2,
                ..HeaderRule::default()
            },
            expected_result: Ok((
                "ID:Utf8,KPI:Utf8,Period 1:Utf8,Period 2:Utf8,Jan:Float64,Feb:Float64,Mar:Float64",
                "NULL|Customer service|NULL|NULL|NULL|NULL|NULL;NULL|Targets reviewed quarterly|note: P1 excludes returns|NULL|NULL|NULL|NULL;K1|On-time delivery|0.9|NULL|0.95|0.8|NULL;NULL|Delivered on time|9|NULL|19.0|8.0|NULL;NULL|Delivered|10|0|20.0|10.0|0.0;K2|First contact resolution|0.5|0.6|NULL|0.75|1.0;NULL|Escalations|1|n/a - system outage|NULL|0.0|NULL;K3|Average handle time (min)|6.5|7|6.25|NULL|5.5",
            )),
        },
        SheetInputTestCase {
            description: "an error cell still fails the read, named",
            sheet: sheet(
                "Errors",
                (0, 0),
                vec![vec![text("ratio")], vec![error("#DIV/0!")]],
            ),
            rule: HeaderRule::default(),
            expected_result: Err(
                "Errors!A2 holds the error #DIV/0!; fix the formula, or leave the row out of the range",
            ),
        },
    ];
    for test_case in test_cases {
        let seen = sheet_input(&parse_grid(&test_case.sheet), &test_case.rule).and_then(|batch| {
            let schema = batch.schema();
            let fields: Vec<String> = schema
                .fields()
                .iter()
                .map(|field| format!("{}:{}", field.name(), field.data_type()))
                .collect();
            Ok((fields.join(","), rows_text(&batch)?))
        });
        assert_eq!(
            seen,
            test_case
                .expected_result
                .map(|(fields, rows)| (fields.to_owned(), rows.to_owned()))
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
