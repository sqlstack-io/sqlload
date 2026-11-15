use chrono_tz::Tz;

use crate::column_parse::main::column_parse::column_parse;
use crate::column_parse::models::{BadValue, ColumnParse, EpochUnit, ParseKind, TimeFormat};
use crate::column_parse::tests::test_types::ColumnParseTestCase;

#[test]
fn given_a_columns_parse_as_written_when_reading_it_then_it_has_its_formats_or_a_problem_naming_why_not()
 {
    let test_cases = [
        ColumnParseTestCase {
            description: "an epoch word is a timestamp read from that epoch",
            setting: r#""epoch_ms""#,
            expected_outcome: Ok(ColumnParse {
                kind: ParseKind::Timestamp,
                formats: vec![TimeFormat::Epoch(EpochUnit::Milliseconds)],
                tz: None,
                on_error: BadValue::Fail,
            }),
        },
        ColumnParseTestCase {
            description: "a list of formats in order, with tz and on_error",
            setting: r#"{"timestamp": ["iso", "epoch_s", "%m/%d/%Y"], "tz": "Europe/Paris", "on_error": "null"}"#,
            expected_outcome: Ok(ColumnParse {
                kind: ParseKind::Timestamp,
                formats: vec![
                    TimeFormat::Iso,
                    TimeFormat::Epoch(EpochUnit::Seconds),
                    TimeFormat::Strftime("%m/%d/%Y".to_owned()),
                ],
                tz: Some(Tz::Europe__Paris),
                on_error: BadValue::Null,
            }),
        },
        ColumnParseTestCase {
            description: "one format as text",
            setting: r#"{"date": "%d/%m/%Y"}"#,
            expected_outcome: Ok(ColumnParse {
                kind: ParseKind::Date,
                formats: vec![TimeFormat::Strftime("%d/%m/%Y".to_owned())],
                tz: None,
                on_error: BadValue::Fail,
            }),
        },
        ColumnParseTestCase {
            description: "another word",
            setting: r#""timestamp""#,
            expected_outcome: Err(
                "needs \"epoch_s\", \"epoch_ms\" or \"epoch_us\", or timestamp or date with a format or a list of formats (and tz and on_error), not \"timestamp\"",
            ),
        },
        ColumnParseTestCase {
            description: "neither timestamp nor date",
            setting: r#"{"tz": "Europe/Paris"}"#,
            expected_outcome: Err(
                "needs timestamp or date, with the formats to try, such as timestamp = [\"iso\", \"%d/%m/%Y %H:%M\"]",
            ),
        },
        ColumnParseTestCase {
            description: "both",
            setting: r#"{"timestamp": "iso", "date": "iso"}"#,
            expected_outcome: Err("takes timestamp or date, not both"),
        },
        ColumnParseTestCase {
            description: "an unknown key",
            setting: r#"{"timestamp": "iso", "zone": "UTC"}"#,
            expected_outcome: Err("takes timestamp or date, with tz and on_error, not zone"),
        },
        ColumnParseTestCase {
            description: "an empty list of formats",
            setting: r#"{"timestamp": []}"#,
            expected_outcome: Err(
                "takes a format or a list of formats, such as [\"iso\", \"%m/%d/%Y %H:%M\"], not []",
            ),
        },
        ColumnParseTestCase {
            description: "a zone that is not an IANA name",
            setting: r#"{"timestamp": "iso", "tz": "Europe/Pari"}"#,
            expected_outcome: Err(
                "tz \"Europe/Pari\" is not an IANA time zone such as \"Europe/Paris\" or \"America/New_York\"",
            ),
        },
        ColumnParseTestCase {
            description: "tz on a date",
            setting: r#"{"date": "iso", "tz": "Europe/Paris"}"#,
            expected_outcome: Err(
                "tz is for timestamps; a date has no time of day to place in a zone",
            ),
        },
        ColumnParseTestCase {
            description: "an epoch format under date",
            setting: r#"{"date": ["iso", "epoch_ms"]}"#,
            expected_outcome: Err("epoch_ms reads an instant; list it under timestamp, not date"),
        },
        ColumnParseTestCase {
            description: "a format without a directive, such as a misspelt iso",
            setting: r#"{"timestamp": "ISO"}"#,
            expected_outcome: Err(
                "format \"ISO\" has no directive; write \"iso\", an epoch format such as \"epoch_ms\", or a strftime format such as \"%d/%m/%Y\"",
            ),
        },
        ColumnParseTestCase {
            description: "%f, which reads nanoseconds here",
            setting: r#"{"timestamp": "%H:%M:%S.%f"}"#,
            expected_outcome: Err(
                "format \"%H:%M:%S.%f\" has %f, which reads nanoseconds here; write %.f for a fraction after a dot, as in \"%H:%M:%S%.f\" for 08:05:00.123456",
            ),
        },
        ColumnParseTestCase {
            description: "%s, an instant a time zone would shift",
            setting: r#"{"timestamp": ["iso", "%s"], "tz": "Europe/Paris"}"#,
            expected_outcome: Err(
                "format \"%s\" has %s, seconds since the epoch, which a time zone would shift; write \"epoch_s\"",
            ),
        },
        ColumnParseTestCase {
            description: "an unknown directive",
            setting: r#"{"date": "%d/%Q"}"#,
            expected_outcome: Err(
                "format \"%d/%Q\" has a directive that is not a strftime one such as %Y, %m, %d, %H, %M, %S or %z",
            ),
        },
        ColumnParseTestCase {
            description: "an unknown on_error",
            setting: r#"{"date": "iso", "on_error": "skip"}"#,
            expected_outcome: Err("on_error must be \"fail\" or \"null\", not \"skip\""),
        },
    ];
    for test_case in test_cases {
        let setting: serde_json::Value =
            serde_json::from_str(test_case.setting).expect(test_case.description);
        assert_eq!(
            column_parse(&setting),
            test_case.expected_outcome.map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
