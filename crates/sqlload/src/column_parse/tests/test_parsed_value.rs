use chrono_tz::Tz;

use crate::column_parse::main::column_parse::column_parse;
use crate::column_parse::main::parsed_value::parsed_value;
use crate::column_parse::models::RawTime;
use crate::column_parse::tests::helpers::{day, instant};
use crate::column_parse::tests::test_types::ParsedValueTestCase;

/// EAM's formats: ISO, then three US wall-clock forms.
const EAM: &str = r#"{"timestamp": ["iso", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"], "tz": "Europe/Paris"}"#;

#[test]
fn given_a_value_and_a_columns_formats_when_parsing_then_the_first_format_that_reads_it_wins_in_its_zone()
 {
    let test_cases = [
        ParsedValueTestCase {
            description: "ISO with Z is that instant",
            setting: EAM,
            raw: RawTime::Text("2026-03-29T00:15:00Z"),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2026-03-29T00:15:00Z")),
        },
        ParsedValueTestCase {
            description: "ISO without an offset is a wall-clock time in tz: 10:00 in Paris in January is 09:00 UTC",
            setting: EAM,
            raw: RawTime::Text("2026-01-15T10:00:00"),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2026-01-15T09:00:00Z")),
        },
        ParsedValueTestCase {
            description: "ISO with a space, no seconds",
            setting: EAM,
            raw: RawTime::Text("2026-01-15 10:00"),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2026-01-15T09:00:00Z")),
        },
        ParsedValueTestCase {
            description: "tz is ignored for a value with an offset: ISO with a fraction and +0100",
            setting: r#"{"timestamp": "iso", "tz": "Asia/Tokyo"}"#,
            raw: RawTime::Text("2026-01-15T10:00:00.5+0100"),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2026-01-15T09:00:00.5Z")),
        },
        ParsedValueTestCase {
            description: "tz is ignored for a strftime format reading an offset",
            setting: r#"{"timestamp": "%Y-%m-%d %H:%M %z", "tz": "Europe/Paris"}"#,
            raw: RawTime::Text("2026-01-15 10:00 +0000"),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2026-01-15T10:00:00Z")),
        },
        ParsedValueTestCase {
            description: "an ISO date alone under timestamp is midnight in tz",
            setting: EAM,
            raw: RawTime::Text("2026-07-01"),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2026-06-30T22:00:00Z")),
        },
        ParsedValueTestCase {
            description: "a US time before Paris's clocks go forward is CET, +01:00",
            setting: EAM,
            raw: RawTime::Text("03/29/2026 01:30:00"),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2026-03-29T00:30:00Z")),
        },
        ParsedValueTestCase {
            description: "a US time after they go forward is CEST, +02:00, read by the third format",
            setting: EAM,
            raw: RawTime::Text("03/29/2026 03:30"),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2026-03-29T01:30:00Z")),
        },
        ParsedValueTestCase {
            description: "a US date alone is midnight in tz",
            setting: EAM,
            raw: RawTime::Text("03/29/2026"),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2026-03-28T23:00:00Z")),
        },
        ParsedValueTestCase {
            description: "a time in the hour the clocks skip doesn't exist, so it fails rather than guess",
            setting: EAM,
            raw: RawTime::Text("03/29/2026 02:30"),
            zone: Tz::UTC,
            expected_outcome: Err(
                "a time that does not exist in Europe/Paris, whose clocks go forward past it",
            ),
        },
        ParsedValueTestCase {
            description: "a time the clocks go back over happens twice; the earlier, CEST, is taken",
            setting: EAM,
            raw: RawTime::Text("10/25/2026 02:30"),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2026-10-25T00:30:00Z")),
        },
        ParsedValueTestCase {
            description: "without tz, the table's zone places the time",
            setting: r#"{"timestamp": ["%Y-%m-%d %H:%M"]}"#,
            raw: RawTime::Text("2026-01-15 10:00"),
            zone: Tz::America__New_York,
            expected_outcome: Ok(instant("2026-01-15T15:00:00Z")),
        },
        ParsedValueTestCase {
            description: "the first format that reads a value wins: 03/04 is the 3rd of April when %d/%m comes first",
            setting: r#"{"date": ["%d/%m/%Y", "%m/%d/%Y"]}"#,
            raw: RawTime::Text("03/04/2026"),
            zone: Tz::UTC,
            expected_outcome: Ok(day("2026-04-03")),
        },
        ParsedValueTestCase {
            description: "a later format reads what an earlier one can't",
            setting: r#"{"date": ["%d/%m/%Y", "%m/%d/%Y"]}"#,
            raw: RawTime::Text("12/31/2026"),
            zone: Tz::UTC,
            expected_outcome: Ok(day("2026-12-31")),
        },
        ParsedValueTestCase {
            description: "an epoch format among text ones reads a number, and iso the text",
            setting: r#"{"timestamp": ["epoch_ms", "iso"]}"#,
            raw: RawTime::Int(1_700_000_000_123),
            zone: Tz::UTC,
            expected_outcome: Ok(instant("2023-11-14T22:13:20.123Z")),
        },
        ParsedValueTestCase {
            description: "an epoch word alone reads numeric text",
            setting: r#""epoch_s""#,
            raw: RawTime::Text("1700000000"),
            zone: Tz::Europe__Paris,
            expected_outcome: Ok(instant("2023-11-14T22:13:20Z")),
        },
        ParsedValueTestCase {
            description: "an epoch past the year 9999 doesn't convert",
            setting: r#""epoch_ms""#,
            raw: RawTime::Int(1_700_000_000_123_456),
            zone: Tz::UTC,
            expected_outcome: Err(
                "which is not a number of milliseconds since 1970-01-01 UTC, in the years 1 to 9999",
            ),
        },
        ParsedValueTestCase {
            description: "a value no format reads names them all",
            setting: r#"{"timestamp": ["iso", "%m/%d/%Y"]}"#,
            raw: RawTime::Text("31.03.2026"),
            zone: Tz::UTC,
            expected_outcome: Err(
                "which is not a timestamp in any of the formats iso, \"%m/%d/%Y\"",
            ),
        },
        ParsedValueTestCase {
            description: "a number is not text a strftime format reads",
            setting: r#"{"timestamp": "%Y%m%d"}"#,
            raw: RawTime::Int(20_260_329),
            zone: Tz::UTC,
            expected_outcome: Err("which is not a timestamp in the format \"%Y%m%d\""),
        },
        ParsedValueTestCase {
            description: "under date, iso reads a date alone, not a time",
            setting: r#"{"date": "iso"}"#,
            raw: RawTime::Text("2026-01-15T10:00"),
            zone: Tz::UTC,
            expected_outcome: Err("which is not an ISO 8601 date"),
        },
    ];
    for test_case in test_cases {
        let setting: serde_json::Value =
            serde_json::from_str(test_case.setting).expect(test_case.description);
        let parse = column_parse(&setting).expect(test_case.description);
        assert_eq!(
            parsed_value(test_case.raw, &parse, test_case.zone),
            test_case.expected_outcome.map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
