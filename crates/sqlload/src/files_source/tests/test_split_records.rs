use crate::files_source::tests::helpers::{flatten_json, read_csv, split_json};
use crate::files_source::tests::test_types::{
    FlattenJsonTestCase, SplitJsonTestCase, TrackCsvTestCase,
};

#[test]
fn given_json_bytes_in_chunks_when_splitting_then_objects_keep_their_lines_and_long_records_fail() {
    let test_cases = [
        SplitJsonTestCase {
            description: "ndjson objects split across chunks",
            by_line: true,
            max_record_bytes: 64,
            text: "{\"a\":1}\n\n{\"a\":2}",
            chunk_bytes: 3,
            expected_lines: Ok(&[1, 3]),
        },
        SplitJsonTestCase {
            description: "an ndjson line longer than the limit fails, even before its line break",
            by_line: true,
            max_record_bytes: 8,
            text: "{\"a\":1}\n{\"a\":\"long\"",
            chunk_bytes: 4,
            expected_lines: Err("line 2: a record is longer than 8 bytes"),
        },
        SplitJsonTestCase {
            description: "a json object longer than the limit fails on the line it starts",
            by_line: false,
            max_record_bytes: 8,
            text: "[\n{\"a\":\n\"long\"}]",
            chunk_bytes: 5,
            expected_lines: Err("line 2: a record is longer than 8 bytes"),
        },
        SplitJsonTestCase {
            description: "a trailing comma in the array fails",
            by_line: false,
            max_record_bytes: 64,
            text: "[{\"a\":1},\n]",
            chunk_bytes: 64,
            expected_lines: Err("line 2: unexpected ']' after a trailing comma"),
        },
    ];
    for test_case in test_cases {
        let lines = split_json(&test_case);
        assert_eq!(
            lines.as_deref().ok(),
            test_case.expected_lines.ok(),
            "{}",
            test_case.description
        );
        assert!(
            lines
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_lines.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_csv_bytes_in_chunks_when_reading_then_the_first_record_and_open_quotes_are_found() {
    let test_cases = [
        TrackCsvTestCase {
            description: "a header split across chunks is read once",
            max_record_bytes: 64,
            text: "id,\"na\"\"me\"\n1,x\n",
            chunk_bytes: 2,
            expected_first: Ok("id|na\"me"),
        },
        TrackCsvTestCase {
            description: "a header longer than the limit fails",
            max_record_bytes: 8,
            text: "a,b,c,d,e,f\n1\n",
            chunk_bytes: 3,
            expected_first: Err("line 1: a record is longer than 8 bytes"),
        },
        TrackCsvTestCase {
            description: "a record longer than the limit fails with its line",
            max_record_bytes: 8,
            text: "a\n\n1\n\"123456789\"\n",
            chunk_bytes: 64,
            expected_first: Err("line 3: a record is longer than 8 bytes"),
        },
        TrackCsvTestCase {
            description: "a doubled quote before the closing quote at the end of the file is closed",
            max_record_bytes: 64,
            text: "a\n\"x\"\"\"",
            chunk_bytes: 64,
            expected_first: Ok("a"),
        },
        TrackCsvTestCase {
            description: "a quote inside an unquoted field is a plain character",
            max_record_bytes: 64,
            text: "a\nx\"y\n",
            chunk_bytes: 64,
            expected_first: Ok("a"),
        },
        TrackCsvTestCase {
            description: "a quoted field open at the end fails with the line of its record",
            max_record_bytes: 64,
            text: "a,b\r\n1,\"x\r\n2,y",
            chunk_bytes: 4,
            expected_first: Err("line 2: unterminated quoted field"),
        },
    ];
    for test_case in test_cases {
        let first = read_csv(&test_case);
        assert_eq!(
            first.as_deref().ok(),
            test_case.expected_first.ok(),
            "{}",
            test_case.description
        );
        assert!(
            first
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_first.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_json_objects_with_nested_objects_when_flattening_as_dlt_then_keys_become_parent_key_fields_in_place()
 {
    let test_cases = [
        FlattenJsonTestCase {
            description: "objects flatten in place at every level; an empty object or a NULL inside one leaves nothing; a list stays",
            text: r#"{"id":1,"c":{"n":"a","addr":{"city":"x"},"gone":null,"e":{}},"tags":[1],"z":2}"#,
            expected_flattened: Ok(
                r#"id=1|c__n="a"|c__addr__city="x"|tags=[1]|z=2 made c,c__addr,c__addr__city,c__e,c__n"#,
            ),
        },
        FlattenJsonTestCase {
            description: "a NULL at the top level stays a field",
            text: r#"{"c":null}"#,
            expected_flattened: Ok("c=null made "),
        },
        FlattenJsonTestCase {
            description: "a flattened name the record already has is an error naming it",
            text: r#"{"c__n":1,"c":{"n":2}}"#,
            expected_flattened: Err(
                "line 1: naming = \"dlt\" flattens an object into the column c__n",
            ),
        },
    ];
    for test_case in test_cases {
        let flattened = flatten_json(&test_case);
        assert_eq!(
            flattened.as_deref().ok(),
            test_case.expected_flattened.ok(),
            "{}",
            test_case.description
        );
        assert!(
            flattened
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_flattened.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
