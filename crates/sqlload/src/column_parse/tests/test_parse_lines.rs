use chrono_tz::Tz;

use crate::column_parse::main::parse_lines::parse_lines;
use crate::column_parse::tests::helpers::parses;
use crate::column_parse::tests::test_types::ParseLinesTestCase;

const COLUMNS: &str = r#"{
    "updated": {"timestamp": ["iso", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y"], "tz": "Europe/Paris"},
    "created": "epoch_ms",
    "due_date": {"date": "%m/%d/%Y", "on_error": "null"},
    "seen_at": {"timestamp": "%Y-%m-%d %H:%M"},
    "sent_at": {"timestamp": "%Y-%m-%d %H:%M %z"}
}"#;

#[test]
fn given_parsed_columns_when_planning_then_each_shows_its_formats_and_the_zone_of_times_without_an_offset_with_where_it_comes_from()
 {
    let test_cases = [
        ParseLinesTestCase {
            description: "without timezone, a time without an offset is UTC by default; its own tz wins; epochs and offsets need no zone",
            parses: COLUMNS,
            timezone: None,
            expected_lines: &[
                "created   timestamp_tz from epoch_ms",
                "due_date  date from \"%m/%d/%Y\"; a value no format reads loads NULL",
                "seen_at   timestamp_tz from \"%Y-%m-%d %H:%M\"; times without an offset in UTC (the default, as neither tz nor timezone is set)",
                "sent_at   timestamp_tz from \"%Y-%m-%d %H:%M %z\"",
                "updated   timestamp_tz from iso, \"%m/%d/%Y %H:%M:%S\", \"%m/%d/%Y\"; times without an offset in Europe/Paris (its tz)",
            ],
        },
        ParseLinesTestCase {
            description: "the timezone setting, with where it is set, places a column without tz",
            parses: COLUMNS,
            timezone: Some((Tz::America__New_York, "the source's timezone")),
            expected_lines: &[
                "created   timestamp_tz from epoch_ms",
                "due_date  date from \"%m/%d/%Y\"; a value no format reads loads NULL",
                "seen_at   timestamp_tz from \"%Y-%m-%d %H:%M\"; times without an offset in America/New_York (the source's timezone)",
                "sent_at   timestamp_tz from \"%Y-%m-%d %H:%M %z\"",
                "updated   timestamp_tz from iso, \"%m/%d/%Y %H:%M:%S\", \"%m/%d/%Y\"; times without an offset in Europe/Paris (its tz)",
            ],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            parse_lines(&parses(test_case.parses), test_case.timezone),
            test_case.expected_lines,
            "{}",
            test_case.description
        );
    }
}
