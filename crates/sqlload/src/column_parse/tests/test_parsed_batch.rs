use chrono_tz::Tz;

use crate::column_parse::main::parsed_batch::parsed_batch;
use crate::column_parse::tests::helpers::{
    EAM, dates, eam_rows, epochs, flags, instants, one_bad_file_row, one_bad_row, parses, rendered,
    wall_clock,
};
use crate::column_parse::tests::test_types::ParsedBatchTestCase;

#[test]
fn given_a_batch_and_its_parses_when_parsing_then_named_columns_load_as_utc_instants_or_dates() {
    let test_cases = [
        ParsedBatchTestCase {
            description: "mixed ISO and US formats in Paris, across the March clock change, load as UTC instants; other columns stay; NULL stays NULL",
            parses: EAM,
            batch: eam_rows,
            rows_before: 0,
            expected_outcome: Ok(
                "id:Int64,updated:Timestamp(µs, \"UTC\") / 1|2026-03-29T00:15:00Z; 2|2026-03-29T00:30:00Z; 3|2026-03-29T01:30:00Z; 4|2026-03-28T23:00:00Z; 5|NULL",
            ),
        },
        ParsedBatchTestCase {
            description: "a value no format reads fails, naming its row in the read, the column and the value",
            parses: EAM,
            batch: one_bad_row,
            rows_before: 10,
            expected_outcome: Err(
                "row 12: column updated holds \"31.03.2026\", which is not a timestamp in any of the formats iso, \"%m/%d/%Y %H:%M:%S\", \"%m/%d/%Y %H:%M\", \"%m/%d/%Y\"; fix the value, or set on_error = \"null\" in its parse to load NULL",
            ),
        },
        ParsedBatchTestCase {
            description: "in a files source's batch, the error names the file and its row there",
            parses: r#"{"updated": {"date": "%m/%d/%Y"}}"#,
            batch: one_bad_file_row,
            rows_before: 0,
            expected_outcome: Err(
                "data/a.csv row 7: column updated holds \"31.03.2026\", which is not a date in the format \"%m/%d/%Y\"; fix the value, or set on_error = \"null\" in its parse to load NULL",
            ),
        },
        ParsedBatchTestCase {
            description: "with on_error null, that value loads NULL",
            parses: r#"{"updated": {"timestamp": "iso", "on_error": "null"}}"#,
            batch: one_bad_row,
            rows_before: 0,
            expected_outcome: Ok("updated:Timestamp(µs, \"UTC\") / 2026-03-29T00:15:00Z; NULL"),
        },
        ParsedBatchTestCase {
            description: "epoch formats read integer and float columns",
            parses: r#"{"ms": "epoch_ms", "s": "epoch_s"}"#,
            batch: epochs,
            rows_before: 0,
            expected_outcome: Ok(
                "ms:Timestamp(µs, \"UTC\"),s:Timestamp(µs, \"UTC\") / 2023-11-14T22:13:20.123Z|2023-11-14T22:13:20.500Z; NULL|1970-01-01T00:00:00Z",
            ),
        },
        ParsedBatchTestCase {
            description: "date formats in turn give a date column",
            parses: r#"{"due": {"date": ["%m/%d/%Y", "iso"]}}"#,
            batch: dates,
            rows_before: 0,
            expected_outcome: Ok("due:Date32 / 2026-12-31; 2026-01-02"),
        },
        ParsedBatchTestCase {
            description: "instants already (text with an offset the crossing typed) stay as they are, as tz would not apply to them",
            parses: EAM,
            batch: instants,
            rows_before: 0,
            expected_outcome: Ok("updated:Timestamp(µs, \"UTC\") / 1970-01-01T00:00:00Z"),
        },
        ParsedBatchTestCase {
            description: "a column of times without a zone (a Python datetime) has no text to parse",
            parses: r#"{"updated": {"timestamp": "iso"}}"#,
            batch: wall_clock,
            rows_before: 0,
            expected_outcome: Err(
                "column updated is already a timestamp or a date, so parse has no text to read; drop its parse",
            ),
        },
        ParsedBatchTestCase {
            description: "a column of booleans holds no times",
            parses: r#"{"updated": {"timestamp": "iso"}}"#,
            batch: flags,
            rows_before: 0,
            expected_outcome: Err(
                "column updated holds Boolean values; parse reads text, or numbers with an epoch format",
            ),
        },
    ];
    for test_case in test_cases {
        let parsed = parsed_batch(
            &(test_case.batch)(),
            &parses(test_case.parses),
            Tz::UTC,
            test_case.rows_before,
        );
        assert_eq!(
            parsed.map(|batch| rendered(&batch)),
            test_case
                .expected_outcome
                .map(str::to_owned)
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
