use crate::loader::tests::helpers::python_spec;
use crate::loader::tests::test_types::TableParsesTestCase;

#[test]
fn given_a_table_s_parse_and_timezone_when_resolving_then_columns_take_their_loaded_names_and_bad_settings_fail_plan()
 {
    let test_cases = [
        TableParsesTestCase {
            description: "a parse keyed by the source's spelling names the loaded column, and timezone is the table's",
            settings: r#"{"parse": {"Updated At": {"timestamp": "iso"}}, "timezone": "Europe/Paris"}"#,
            expected_outcome: Ok("updated_at|Europe/Paris|table"),
        },
        TableParsesTestCase {
            description: "a column pinned in columns cannot be parsed too, as parse gives its type",
            settings: r#"{"columns": {"updated": "text"}, "parse": {"updated": "epoch_ms"}}"#,
            expected_outcome: Err(
                "parse updated: the column is also pinned in columns, and parse gives its type; drop the pin",
            ),
        },
        TableParsesTestCase {
            description: "a parse of an unknown shape names the shapes it takes",
            settings: r#"{"parse": {"updated": "epoch"}}"#,
            expected_outcome: Err(
                "parse updated: needs \"epoch_s\", \"epoch_ms\" or \"epoch_us\", or timestamp or date with a format or a list of formats (and tz and on_error), not \"epoch\"",
            ),
        },
        TableParsesTestCase {
            description: "a timezone that is not an IANA zone fails",
            settings: r#"{"timezone": "Europe/Pari"}"#,
            expected_outcome: Err(
                "timezone: \"Europe/Pari\" is not an IANA time zone such as \"Europe/Paris\" or \"America/New_York\"",
            ),
        },
    ];
    for test_case in test_cases {
        let found = python_spec(test_case.settings).map(|spec| {
            let zone = spec
                .timezone
                .map(|(zone, origin)| format!("{zone}|{origin}"))
                .unwrap_or_default();
            let parsed: Vec<&str> = spec.parses.keys().map(String::as_str).collect();
            format!("{}|{zone}", parsed.join(","))
        });
        assert_eq!(
            found.as_deref().map_err(|problem| test_case
                .expected_outcome
                .err()
                .is_some_and(|expected| problem.contains(&expected.escape_debug().to_string()))),
            test_case.expected_outcome.map_err(|_| true),
            "{}: {found:?}",
            test_case.description
        );
    }
}
