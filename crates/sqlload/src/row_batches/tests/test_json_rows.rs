use crate::row_batches::models::JsonLayout;
use crate::row_batches::tests::helpers::read_json;
use crate::row_batches::tests::test_types::JsonRowsTestCase;

#[test]
fn given_json_bytes_when_reading_rows_then_columns_are_typed_by_the_files_rules() {
    let test_cases = [
        JsonRowsTestCase {
            description: "ndjson: ints, floats, bools, timestamps and text are inferred; nested values are compact JSON in source order",
            layout: JsonLayout::Lines,
            path: None,
            batch_rows: 10,
            text: "{\"id\":1,\"f\":1.5,\"ok\":true,\"at\":\"2026-01-02T03:04:05+01:00\",\"s\":\"a\",\"n\":{\"z\": 1, \"a\": [1, \"x y\"]}}\n\n{\"id\":2,\"f\":2,\"ok\":false,\"at\":null,\"s\":\"b\\\"c\"}\n",
            expected_batches: Ok(&[
                "id:Int64,f:Float64,ok:Boolean,at:Timestamp(µs, \"UTC\"),s:Utf8,n:Utf8(json) / 1|1.5|true|2026-01-02T02:04:05Z|a|{\"z\":1,\"a\":[1,\"x y\"]}; 2|2.0|false|NULL|b\"c|NULL",
            ]),
        },
        JsonRowsTestCase {
            description: "mixed types make text; integers beyond 64 bits are text; a repeated key keeps its last value",
            layout: JsonLayout::Lines,
            path: None,
            batch_rows: 10,
            text: "{\"v\":1,\"big\":18446744073709551616,\"k\":1,\"k\":2}\n{\"v\":\"x\",\"big\":1}",
            expected_batches: Ok(&["v:Utf8,big:Utf8,k:Int64 / 1|18446744073709551616|2; x|1|NULL"]),
        },
        JsonRowsTestCase {
            description: "a document array splits into batches of batch_rows; a column only some batches have appears in those",
            layout: JsonLayout::Document,
            path: None,
            batch_rows: 2,
            text: "[{\"a\":1},{\"a\":2},{\"a\":3,\"b\":null}]",
            expected_batches: Ok(&["a:Int64 / 1; 2", "a:Int64,b:Null / 3|NULL"]),
        },
        JsonRowsTestCase {
            description: "objects one after another, and a path into a document",
            layout: JsonLayout::Document,
            path: Some("data.items"),
            batch_rows: 10,
            text: "{\"next\":null,\"data\":{\"items\":[{\"a\":1},{\"a\":2}]}}",
            expected_batches: Ok(&["a:Int64 / 1; 2"]),
        },
        JsonRowsTestCase {
            description: "a null at the path is no rows",
            layout: JsonLayout::Document,
            path: Some("items"),
            batch_rows: 10,
            text: "{\"items\":null}",
            expected_batches: Ok(&[]),
        },
        JsonRowsTestCase {
            description: "a missing key on the path names it",
            layout: JsonLayout::Document,
            path: Some("data.items"),
            batch_rows: 10,
            text: "{\"data\":{}}",
            expected_batches: Err("path data.items: data has no key \"items\""),
        },
        JsonRowsTestCase {
            description: "a row that is not an object fails, naming its line",
            layout: JsonLayout::Lines,
            path: None,
            batch_rows: 10,
            text: "{\"a\":1}\n[1]",
            expected_batches: Err("line 2: invalid type: sequence, expected a JSON object"),
        },
        JsonRowsTestCase {
            description: "malformed JSON fails, naming its line",
            layout: JsonLayout::Document,
            path: None,
            batch_rows: 10,
            text: "[{\"a\":1},\n{\"a\":}]",
            expected_batches: Err("line 2: expected value"),
        },
        JsonRowsTestCase {
            description: "a document that is neither objects nor an array fails",
            layout: JsonLayout::Document,
            path: None,
            batch_rows: 10,
            text: "42",
            expected_batches: Err("expected a JSON array of objects"),
        },
    ];
    for test_case in test_cases {
        let batches = read_json(&test_case);
        assert_eq!(
            batches.as_deref().ok(),
            test_case
                .expected_batches
                .ok()
                .map(|batches| batches
                    .iter()
                    .map(|batch| batch.to_string())
                    .collect::<Vec<_>>())
                .as_deref(),
            "{}",
            test_case.description
        );
        assert!(
            batches
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_batches.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
