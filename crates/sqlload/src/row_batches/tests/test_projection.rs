use crate::row_batches::models::JsonLayout;
use crate::row_batches::tests::helpers::read_projected;
use crate::row_batches::tests::test_types::ProjectionTestCase;

#[test]
fn given_fields_when_reading_json_rows_then_only_those_values_load_under_their_names() {
    let test_cases = [
        ProjectionTestCase {
            description: "renames, a nested path and a key with a dash load in fields order; other keys are dropped",
            layout: JsonLayout::Lines,
            fields: Some(&[
                ("id", "$.id", false),
                ("account_code", "$.code", false),
                ("updated_at", "$.updated-at", false),
                ("city", "$.address.city", false),
            ]),
            keep_record: None,
            text: "{\"code\":\"A1\",\"id\":7,\"address\":{\"city\":\"Leeds\",\"zip\":\"LS1\"},\"updated-at\":\"2026-01-02T03:04:05Z\",\"noise\":1}\n",
            expected_batches: Ok(&[
                "id:Int64,account_code:Utf8,updated_at:Timestamp(µs, \"UTC\"),city:Utf8 / 7|A1|2026-01-02T03:04:05Z|Leeds",
            ]),
        },
        ProjectionTestCase {
            description: "a missing key, a path through a scalar and a null all load NULL",
            layout: JsonLayout::Document,
            fields: Some(&[("id", "$.id", false), ("city", "$.address.city", false)]),
            keep_record: None,
            text: "[{\"id\":1,\"address\":\"none\"},{\"id\":2},{\"id\":3,\"address\":{\"city\":null}},{\"id\":4,\"address\":{\"city\":\"York\"}}]",
            expected_batches: Ok(&["id:Int64,city:Utf8 / 1|NULL; 2|NULL; 3|NULL; 4|York"]),
        },
        ProjectionTestCase {
            description: "keep_record adds the whole object as compact JSON, keys in source order",
            layout: JsonLayout::Lines,
            fields: Some(&[("id", "$.id", false)]),
            keep_record: Some("raw_json"),
            text: "{\"z\": 1, \"id\": 5, \"tags\": [\"a\", \"b c\"]}\n",
            expected_batches: Ok(&[
                "id:Int64,raw_json:Utf8(json) / 5|{\"z\":1,\"id\":5,\"tags\":[\"a\",\"b c\"]}",
            ]),
        },
        ProjectionTestCase {
            description: "a nested object projected whole is JSON",
            layout: JsonLayout::Lines,
            fields: Some(&[("address", "$.address", false)]),
            keep_record: None,
            text: "{\"address\":{\"city\":\"Leeds\"}}\n",
            expected_batches: Ok(&["address:Utf8(json) / {\"city\":\"Leeds\"}"]),
        },
        ProjectionTestCase {
            description: "a required field without a value fails, naming the row and the path",
            layout: JsonLayout::Lines,
            fields: Some(&[("id", "$.id", true)]),
            keep_record: None,
            text: "{\"id\":1}\n{\"id\":null}\n",
            expected_batches: Err(
                "line 2: row 2: field id needs a value at $.id, and the row has none",
            ),
        },
        ProjectionTestCase {
            description: "a path without $ is refused",
            layout: JsonLayout::Lines,
            fields: Some(&[("id", "id", false)]),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "field path \"id\" is not a path such as \"$.id\" or \"$.address.city\"",
            ),
        },
        ProjectionTestCase {
            description: "an empty key is refused",
            layout: JsonLayout::Lines,
            fields: Some(&[("id", "$.a..b", false)]),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "field path \"$.a..b\" is not a path such as \"$.id\" or \"$.address.city\"",
            ),
        },
        ProjectionTestCase {
            description: "keep_record clashing with a field is refused",
            layout: JsonLayout::Lines,
            fields: Some(&[("raw", "$.raw", false)]),
            keep_record: Some("RAW"),
            text: "{}\n",
            expected_batches: Err("keep_record \"RAW\" is also a field's name; pick another"),
        },
        ProjectionTestCase {
            description: "keep_record without fields is refused",
            layout: JsonLayout::Lines,
            fields: None,
            keep_record: Some("raw"),
            text: "{}\n",
            expected_batches: Err(
                "keep_record needs fields: name the columns to load beside the whole record",
            ),
        },
        ProjectionTestCase {
            description: "no fields and no keep_record is refused",
            layout: JsonLayout::Lines,
            fields: Some(&[]),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields is empty, so no column would load; name at least one, or set keep_record",
            ),
        },
        ProjectionTestCase {
            description: "a row that is not an object still fails",
            layout: JsonLayout::Lines,
            fields: Some(&[("id", "$.id", false)]),
            keep_record: None,
            text: "[1]\n",
            expected_batches: Err("line 1: expected a JSON object"),
        },
    ];
    for test_case in test_cases {
        let batches = read_projected(&test_case);
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
        assert_eq!(
            batches.err().unwrap_or_default(),
            test_case.expected_batches.err().unwrap_or_default(),
            "{}",
            test_case.description
        );
    }
}
