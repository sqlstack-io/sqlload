use crate::odbc_source::_helpers::parameters::timestamp_text;
use crate::odbc_source::tests::test_types::TimestampTextTestCase;

#[test]
fn given_a_timestamp_bound_for_a_legacy_sql_server_datetime_when_writing_it_as_text_then_it_has_the_column_s_digits_unless_that_would_cut_it()
-> Result<(), String> {
    let test_cases = [
        TimestampTextTestCase {
            description: "a millisecond bound keeps its three digits, which datetime converts to the tick it read",
            time: "2026-01-31 23:59:59.123",
            digits: 3,
            expected_text: "2026-01-31T23:59:59.123",
        },
        TimestampTextTestCase {
            description: "a whole second still has three digits",
            time: "2026-02-01 00:00:00",
            digits: 3,
            expected_text: "2026-02-01T00:00:00.000",
        },
        TimestampTextTestCase {
            description: "a leading zero in the fraction is kept",
            time: "2026-01-31 23:59:59.007",
            digits: 3,
            expected_text: "2026-01-31T23:59:59.007",
        },
        TimestampTextTestCase {
            description: "a microsecond bound keeps six digits rather than being cut, so the database refuses it",
            time: "2026-01-31 23:59:59.123456",
            digits: 3,
            expected_text: "2026-01-31T23:59:59.123456",
        },
        TimestampTextTestCase {
            description: "seven digits, as a bound the driver can't describe goes",
            time: "2026-01-31 23:59:59.123456",
            digits: 7,
            expected_text: "2026-01-31T23:59:59.1234560",
        },
        TimestampTextTestCase {
            description: "no digits for a whole second",
            time: "2026-01-31 23:59:00",
            digits: 0,
            expected_text: "2026-01-31T23:59:00",
        },
    ];
    for test_case in test_cases {
        let time = chrono::NaiveDateTime::parse_from_str(test_case.time, "%Y-%m-%d %H:%M:%S%.f")
            .map_err(|error| error.to_string())?;
        assert_eq!(
            timestamp_text(&time, test_case.digits),
            test_case.expected_text,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
