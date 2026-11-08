use crate::row_batches::main::projection::projection;
use crate::row_batches::main::projection_problem::projection_problem;
use crate::row_batches::models::JsonLayout;
use crate::row_batches::models::ProjectionRules;
use crate::row_batches::tests::helpers::read_projected;
use crate::row_batches::tests::test_types::{ProjectionProblemTestCase, ProjectionTestCase};

#[test]
fn given_fields_when_reading_json_rows_then_only_those_values_load_under_their_names() {
    let test_cases = [
        ProjectionTestCase {
            description: "renames, a nested path and a key with a dash load in fields order; other keys are dropped",
            layout: JsonLayout::Lines,
            batch_rows: 10,
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
            batch_rows: 10,
            fields: Some(&[("id", "$.id", false), ("city", "$.address.city", false)]),
            keep_record: None,
            text: "[{\"id\":1,\"address\":\"none\"},{\"id\":2},{\"id\":3,\"address\":{\"city\":null}},{\"id\":4,\"address\":{\"city\":\"York\"}}]",
            expected_batches: Ok(&["id:Int64,city:Utf8 / 1|NULL; 2|NULL; 3|NULL; 4|York"]),
        },
        ProjectionTestCase {
            description: "keep_record adds the whole object as compact JSON, keys in source order",
            layout: JsonLayout::Lines,
            batch_rows: 10,
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
            batch_rows: 10,
            fields: Some(&[("address", "$.address", false)]),
            keep_record: None,
            text: "{\"address\":{\"city\":\"Leeds\"}}\n",
            expected_batches: Ok(&["address:Utf8(json) / {\"city\":\"Leeds\"}"]),
        },
        ProjectionTestCase {
            description: "a required field without a value fails, naming the row and the path",
            layout: JsonLayout::Lines,
            batch_rows: 10,
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
            batch_rows: 10,
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
            batch_rows: 10,
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
            batch_rows: 10,
            fields: Some(&[("raw", "$.raw", false)]),
            keep_record: Some("RAW"),
            text: "{}\n",
            expected_batches: Err("keep_record \"RAW\" is also a field's name; pick another"),
        },
        ProjectionTestCase {
            description: "keep_record without fields is refused",
            layout: JsonLayout::Lines,
            batch_rows: 10,
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
            batch_rows: 10,
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
            batch_rows: 10,
            fields: Some(&[("id", "$.id", false)]),
            keep_record: None,
            text: "[1]\n",
            expected_batches: Err("line 1: expected a JSON object"),
        },
        ProjectionTestCase {
            description: "a required field names the row counted across batches, not within one",
            layout: JsonLayout::Lines,
            batch_rows: 2,
            fields: Some(&[("id", "$.id", true)]),
            keep_record: None,
            text: "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n{}\n",
            expected_batches: Err(
                "line 4: row 4: field id needs a value at $.id, and the row has none",
            ),
        },
        ProjectionTestCase {
            description: "a repeated key keeps its last copy, below it too; an escaped key matches its text",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(&[
                ("city", "$.address.city", false),
                ("zip", "$.address.zip", false),
                ("dash", "$.a-b", false),
            ]),
            keep_record: None,
            text: "{\"address\":{\"city\":\"Leeds\",\"zip\":\"LS1\"},\"address\":{\"city\":\"York\"},\"a\\u002db\":7}\n",
            expected_batches: Ok(&["city:Utf8,zip:Null,dash:Int64 / York|NULL|7"]),
        },
        ProjectionTestCase {
            description: "many fields under one object and the object itself all load from one row",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(&[
                ("all", "$", false),
                ("x", "$.a.b.x", false),
                ("y", "$.a.b.y", false),
                ("b", "$.a.b", false),
            ]),
            keep_record: None,
            text: "{\"a\":{\"b\":{\"y\":2,\"x\":1}}}\n",
            expected_batches: Ok(&[
                "all:Utf8(json),x:Int64,y:Int64,b:Utf8(json) / {\"a\":{\"b\":{\"y\":2,\"x\":1}}}|1|2|{\"y\":2,\"x\":1}",
            ]),
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

#[test]
fn given_a_tables_rules_when_checking_its_projection_then_the_cursor_must_load_and_masks_must_hold()
{
    let test_cases = [
        ProjectionProblemTestCase {
            description: "a cursor matches a field ignoring case, as the loader matches columns",
            fields: &["id"],
            keep_record: None,
            cursor_column: Some("ID"),
            flatten: &[],
            masked: &[],
            expected_problem: None,
        },
        ProjectionProblemTestCase {
            description: "a cursor that is no field fails",
            fields: &["id"],
            keep_record: None,
            cursor_column: Some("updated_at"),
            flatten: &[],
            masked: &[],
            expected_problem: Some(
                "the table's cursor_column updated_at is not among the fields; add it to fields",
            ),
        },
        ProjectionProblemTestCase {
            description: "a cursor a flattened field makes loads",
            fields: &["customer"],
            keep_record: None,
            cursor_column: Some("Customer__Updated_At"),
            flatten: &["customer"],
            masked: &[],
            expected_problem: None,
        },
        ProjectionProblemTestCase {
            description: "a flattened field no longer loads under its own name",
            fields: &["customer"],
            keep_record: None,
            cursor_column: Some("customer"),
            flatten: &["customer"],
            masked: &[],
            expected_problem: Some(
                "the table's cursor_column customer is not among the fields; add it to fields",
            ),
        },
        ProjectionProblemTestCase {
            description: "a flatten entry in another case flattens nothing, so the field loads under its own name",
            fields: &["customer"],
            keep_record: None,
            cursor_column: Some("customer__updated_at"),
            flatten: &["Customer"],
            masked: &[],
            expected_problem: Some(
                "the table's cursor_column customer__updated_at is not among the fields; add it to fields",
            ),
        },
        ProjectionProblemTestCase {
            description: "keep_record can be the cursor",
            fields: &[],
            keep_record: Some("raw"),
            cursor_column: Some("raw"),
            flatten: &[],
            masked: &[],
            expected_problem: None,
        },
        ProjectionProblemTestCase {
            description: "keep_record on a masked table fails",
            fields: &["email"],
            keep_record: Some("raw"),
            cursor_column: None,
            flatten: &[],
            masked: &["email"],
            expected_problem: Some(
                "keep_record would hold the masked columns (email) unmasked; drop keep_record or the mask",
            ),
        },
    ];
    for test_case in test_cases {
        let fields: Vec<(String, String, bool)> = test_case
            .fields
            .iter()
            .map(|name| ((*name).to_owned(), format!("$.{name}"), false))
            .collect();
        let flatten: Vec<String> = test_case
            .flatten
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let masked: Vec<String> = test_case
            .masked
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let rules = ProjectionRules {
            cursor_column: test_case.cursor_column,
            flatten: &flatten,
            masked: &masked,
        };
        let problem = projection(Some(&fields), test_case.keep_record)
            .ok()
            .flatten()
            .and_then(|projected| projection_problem(&projected, rules));
        assert_eq!(
            problem.as_deref(),
            test_case.expected_problem,
            "{}",
            test_case.description
        );
    }
}
