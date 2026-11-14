use crate::load_naming::models::Naming;
use crate::row_batches::main::projection::projection;
use crate::row_batches::main::projection_problem::projection_problem;
use crate::row_batches::models::JsonLayout;
use crate::row_batches::models::ProjectionRules;
use crate::row_batches::models::{BadValue, FieldFrom, FieldSpec};
use crate::row_batches::tests::helpers::read_projected;
use crate::row_batches::tests::test_types::{ProjectionProblemTestCase, ProjectionTestCase};

#[test]
fn given_fields_when_reading_json_rows_then_only_those_values_load_under_their_names() {
    let test_cases = [
        ProjectionTestCase {
            description: "parse turns epoch seconds, millisecond text and microseconds into UTC timestamps",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"s","path":"$.s","parse":{"kind":"epoch_s"}},{"name":"ms","path":"$.ms","parse":{"kind":"epoch_ms"}},{"name":"us","path":"$.us","parse":{"kind":"epoch_us"}}]"#,
            ),
            keep_record: None,
            text: "{\"s\":1700000000,\"ms\":\"1700000000123\",\"us\":1700000000123456}\n{\"s\":null}\n",
            expected_batches: Ok(&[
                "s:Timestamp(µs, \"UTC\"),ms:Timestamp(µs, \"UTC\"),us:Timestamp(µs, \"UTC\") / 2023-11-14T22:13:20Z|2023-11-14T22:13:20.123Z|2023-11-14T22:13:20.123456Z; NULL|NULL|NULL",
            ]),
        },
        ProjectionTestCase {
            description: "parse reads text with a format as a wall-clock timestamp, an instant when it has an offset, or a date",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"at","path":"$.at","parse":{"kind":"timestamp","format":"%d/%m/%Y %H:%M"}},{"name":"inst","path":"$.at_z","parse":{"kind":"timestamp","format":"%d/%m/%Y %H:%M %z"}},{"name":"day","path":"$.day","parse":{"kind":"date","format":"%d/%m/%Y"}}]"#,
            ),
            keep_record: None,
            text: "{\"at\":\"14/11/2026 08:05\",\"at_z\":\"14/11/2026 08:05 +0100\",\"day\":\"31/12/2026\"}\n",
            expected_batches: Ok(&[
                "at:Timestamp(µs),inst:Timestamp(µs, \"UTC\"),day:Date32 / 2026-11-14T08:05:00|2026-11-14T07:05:00Z|2026-12-31",
            ]),
        },
        ProjectionTestCase {
            description: "a value parse can't read fails the read, naming the row, field, value and format",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"day","path":"$.day","parse":{"kind":"date","format":"%d/%m/%Y"}}]"#,
            ),
            keep_record: None,
            text: "{\"day\":\"31/12/2026\"}\n{\"day\":\"2026-12-31\"}\n",
            expected_batches: Err(
                "line 2: row 2: field day holds \"2026-12-31\", which is not a date in the format \"%d/%m/%Y\"; fix the value, or set on_error=\"null\" on its sl.field to load NULL",
            ),
        },
        ProjectionTestCase {
            description: "with on_error null, a value parse can't read loads NULL",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"ms","path":"$.ms","parse":{"kind":"epoch_ms"},"on_error":"null"}]"#,
            ),
            keep_record: None,
            text: "{\"ms\":\"soon\"}\n{\"ms\":0}\n",
            expected_batches: Ok(&["ms:Timestamp(µs, \"UTC\") / NULL; 1970-01-01T00:00:00Z"]),
        },
        ProjectionTestCase {
            description: "an epoch past the year 9999 doesn't convert, so it loads NULL rather than turning the column into text",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"ms","path":"$.ms","parse":{"kind":"epoch_ms"},"on_error":"null"}]"#,
            ),
            keep_record: None,
            text: "{\"ms\":1700000000123456}\n{\"ms\":1700000000123}\n",
            expected_batches: Ok(&["ms:Timestamp(µs, \"UTC\") / NULL; 2023-11-14T22:13:20.123Z"]),
        },
        ProjectionTestCase {
            description: "%#z reads Z as UTC, and %.f a fraction of the seconds",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"at","path":"$.at","parse":{"kind":"timestamp","format":"%Y-%m-%dT%H:%M:%S%.f%#z"}}]"#,
            ),
            keep_record: None,
            text: "{\"at\":\"2026-11-14T08:05:00.123456Z\"}\n{\"at\":\"2026-11-14T08:05:00.5+01:00\"}\n",
            expected_batches: Ok(&[
                "at:Timestamp(µs, \"UTC\") / 2026-11-14T08:05:00.123456Z; 2026-11-14T07:05:00.500Z",
            ]),
        },
        ProjectionTestCase {
            description: "%f, which reads nanoseconds here, is refused before any row",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"at","path":"$.at","parse":{"kind":"timestamp","format":"%H:%M:%S.%f"}}]"#,
            ),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"at\"]: parse format \"%H:%M:%S.%f\" has %f, which reads nanoseconds here; write %.f for a fraction after a dot, as in \"%H:%M:%S%.f\" for 08:05:00.123456",
            ),
        },
        ProjectionTestCase {
            description: "a padded %-f reads nanoseconds too and is refused the same way",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"at","path":"$.at","parse":{"kind":"timestamp","format":"%H:%M:%S.%-f"}}]"#,
            ),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"at\"]: parse format \"%H:%M:%S.%-f\" has %f, which reads nanoseconds here; write %.f for a fraction after a dot, as in \"%H:%M:%S%.f\" for 08:05:00.123456",
            ),
        },
        ProjectionTestCase {
            description: "a format with an unknown directive is refused before any row",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"day","path":"$.day","parse":{"kind":"date","format":"%d/%Q"}}]"#,
            ),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"day\"]: parse format \"%d/%Q\" has a directive that is not a strftime one such as %Y, %m, %d, %H, %M, %S or %z",
            ),
        },
        ProjectionTestCase {
            description: "renames, a nested path and a key with a dash load in fields order; other keys are dropped",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"id","path":"$.id"},{"name":"account_code","path":"$.code"},{"name":"updated_at","path":"$.updated-at"},{"name":"city","path":"$.address.city"}]"#,
            ),
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
            fields: Some(
                r#"[{"name":"id","path":"$.id"},{"name":"city","path":"$.address.city"}]"#,
            ),
            keep_record: None,
            text: "[{\"id\":1,\"address\":\"none\"},{\"id\":2},{\"id\":3,\"address\":{\"city\":null}},{\"id\":4,\"address\":{\"city\":\"York\"}}]",
            expected_batches: Ok(&["id:Int64,city:Utf8 / 1|NULL; 2|NULL; 3|NULL; 4|York"]),
        },
        ProjectionTestCase {
            description: "keep_record adds the whole object as compact JSON, keys in source order",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"id","path":"$.id"}]"#),
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
            fields: Some(r#"[{"name":"address","path":"$.address"}]"#),
            keep_record: None,
            text: "{\"address\":{\"city\":\"Leeds\"}}\n",
            expected_batches: Ok(&["address:Utf8(json) / {\"city\":\"Leeds\"}"]),
        },
        ProjectionTestCase {
            description: "a required field without a value fails, naming the row and the path",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"id","path":"$.id","required":true}]"#),
            keep_record: None,
            text: "{\"id\":1}\n{\"id\":null}\n",
            expected_batches: Err(
                "line 2: row 2: field id needs a value at $.id, and the row has none",
            ),
        },
        ProjectionTestCase {
            description: "[n] reads a list's item and [-1] its last, through nested lists too; past either end, on a value that isn't a list, or under a missing key it loads NULL",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"first","path":"$.items[0].to"},{"name":"last","path":"$.items[-1].to"},{"name":"cell","path":"$.grid[1][0]"},{"name":"past","path":"$.items[2].to"},{"name":"before","path":"$.items[-3].to"},{"name":"item","path":"$.items[1]"},{"name":"huge","path":"$.items[99999999999999999999999]"}]"#,
            ),
            keep_record: None,
            text: "{\"items\":[{\"to\":\"a\"},{\"to\":\"b\"}],\"grid\":[[1,2],[3,4]]}\n{\"items\":{\"to\":\"x\"},\"grid\":[]}\n{}\n",
            expected_batches: Ok(&[
                "first:Utf8,last:Utf8,cell:Int64,past:Null,before:Null,item:Utf8(json),huge:Null / a|b|3|NULL|NULL|{\"to\":\"b\"}|NULL; NULL|NULL|NULL|NULL|NULL|NULL|NULL; NULL|NULL|NULL|NULL|NULL|NULL|NULL",
            ]),
        },
        ProjectionTestCase {
            description: "a required field whose index is past the end of the list fails, naming the row, field and path",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"to","path":"$.items[1].to","required":true}]"#),
            keep_record: None,
            text: "{\"items\":[{\"to\":\"a\"},{\"to\":\"b\"}]}\n{\"items\":[{\"to\":\"a\"}]}\n",
            expected_batches: Err(
                "line 2: row 2: field to needs a value at $.items[1].to, and the row has none",
            ),
        },
        ProjectionTestCase {
            description: "a path without $ is refused, naming the field",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"id","path":"id"}]"#),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"id\"]: field path \"id\" is not a path such as \"$.id\", \"$.address.city\" or \"$.items[0].id\"",
            ),
        },
        ProjectionTestCase {
            description: "an empty key is refused",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"id","path":"$.a.[0]"}]"#),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"id\"]: field path \"$.a.[0]\" is not a path such as \"$.id\", \"$.address.city\" or \"$.items[0].id\"",
            ),
        },
        ProjectionTestCase {
            description: "a filter is refused, naming the field and its path",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"to_status","path":"$.items[?(@.x)]"}]"#),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"to_status\"]: field path \"$.items[?(@.x)]\" uses the filter [?(@.x)], which field paths don't support; they take dotted names and [n] or [-1] indexes, such as \"$.items[0].id\"",
            ),
        },
        ProjectionTestCase {
            description: "a slice is refused, naming the field and its path",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"to_status","path":"$.items[0:2]"}]"#),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"to_status\"]: field path \"$.items[0:2]\" uses the slice [0:2], which field paths don't support; they take dotted names and [n] or [-1] indexes, such as \"$.items[0].id\"",
            ),
        },
        ProjectionTestCase {
            description: "recursive descent is refused, naming the field and its path",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"to_status","path":"$..id"}]"#),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"to_status\"]: field path \"$..id\" uses recursive descent (..), which field paths don't support; they take dotted names and [n] or [-1] indexes, such as \"$.items[0].id\"",
            ),
        },
        ProjectionTestCase {
            description: "recursive descent below a key is refused, naming the field and its path",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"to_status","path":"$.a..b"}]"#),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"to_status\"]: field path \"$.a..b\" uses recursive descent (..), which field paths don't support; they take dotted names and [n] or [-1] indexes, such as \"$.items[0].id\"",
            ),
        },
        ProjectionTestCase {
            description: "a wildcard index is refused, naming the field and its path",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"to_status","path":"$.items[*].id"}]"#),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"to_status\"]: field path \"$.items[*].id\" uses the wildcard [*], which one column can't hold; take one item with [n] or [-1], or select the list's items as rows",
            ),
        },
        ProjectionTestCase {
            description: "a wildcard key is refused, naming the field and its path",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"to_status","path":"$.items.*"}]"#),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"to_status\"]: field path \"$.items.*\" uses the wildcard .*, which one column can't hold; take one item with [n] or [-1], or select the list's items as rows",
            ),
        },
        ProjectionTestCase {
            description: "a quoted key is refused, naming the field and its path",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"to_status","path":"$['id']"}]"#),
            keep_record: None,
            text: "{}\n",
            expected_batches: Err(
                "fields[\"to_status\"]: field path \"$['id']\" uses the bracketed key ['id'], which field paths don't support; they take dotted names and [n] or [-1] indexes, such as \"$.items[0].id\"",
            ),
        },
        ProjectionTestCase {
            description: "keep_record clashing with a field is refused",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(r#"[{"name":"raw","path":"$.raw"}]"#),
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
            fields: Some(r#"[]"#),
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
            fields: Some(r#"[{"name":"id","path":"$.id"}]"#),
            keep_record: None,
            text: "[1]\n",
            expected_batches: Err("line 1: expected a JSON object"),
        },
        ProjectionTestCase {
            description: "a required field names the row counted across batches, not within one",
            layout: JsonLayout::Lines,
            batch_rows: 2,
            fields: Some(r#"[{"name":"id","path":"$.id","required":true}]"#),
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
            fields: Some(
                r#"[{"name":"city","path":"$.address.city"},{"name":"zip","path":"$.address.zip"},{"name":"dash","path":"$.a-b"}]"#,
            ),
            keep_record: None,
            text: "{\"address\":{\"city\":\"Leeds\",\"zip\":\"LS1\"},\"address\":{\"city\":\"York\"},\"a\\u002db\":7}\n",
            expected_batches: Ok(&["city:Utf8,zip:Null,dash:Int64 / York|NULL|7"]),
        },
        ProjectionTestCase {
            description: "many fields under one object and the object itself all load from one row",
            layout: JsonLayout::Lines,
            batch_rows: 10,
            fields: Some(
                r#"[{"name":"all","path":"$"},{"name":"x","path":"$.a.b.x"},{"name":"y","path":"$.a.b.y"},{"name":"b","path":"$.a.b"}]"#,
            ),
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
            naming: Naming::Sqlload,
            expected_problem: None,
        },
        ProjectionProblemTestCase {
            description: "under dlt naming a field loads the cursor when its loaded name is the cursor's",
            fields: &["id", "UpdatedAt"],
            keep_record: None,
            cursor_column: Some("updated_at"),
            flatten: &[],
            masked: &[],
            naming: Naming::Dlt,
            expected_problem: None,
        },
        ProjectionProblemTestCase {
            description: "under dlt naming a flattened object's keys load the cursor by their loaded names",
            fields: &["Meta"],
            keep_record: None,
            cursor_column: Some("meta__updated_at"),
            flatten: &["Meta"],
            masked: &[],
            naming: Naming::Dlt,
            expected_problem: None,
        },
        ProjectionProblemTestCase {
            description: "under SQLLoad's naming a field spelled otherwise is no cursor",
            fields: &["id", "UpdatedAt"],
            keep_record: None,
            cursor_column: Some("updated_at"),
            flatten: &[],
            masked: &[],
            naming: Naming::Sqlload,
            expected_problem: Some(
                "the table's cursor_column updated_at is not among the fields; add it to fields",
            ),
        },
        ProjectionProblemTestCase {
            description: "a cursor that is no field fails",
            fields: &["id"],
            keep_record: None,
            cursor_column: Some("updated_at"),
            flatten: &[],
            masked: &[],
            naming: Naming::Sqlload,
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
            naming: Naming::Sqlload,
            expected_problem: None,
        },
        ProjectionProblemTestCase {
            description: "a flattened field no longer loads under its own name",
            fields: &["customer"],
            keep_record: None,
            cursor_column: Some("customer"),
            flatten: &["customer"],
            masked: &[],
            naming: Naming::Sqlload,
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
            naming: Naming::Sqlload,
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
            naming: Naming::Sqlload,
            expected_problem: None,
        },
        ProjectionProblemTestCase {
            description: "keep_record on a masked table fails",
            fields: &["email"],
            keep_record: Some("raw"),
            cursor_column: None,
            flatten: &[],
            masked: &["email"],
            naming: Naming::Sqlload,
            expected_problem: Some(
                "keep_record would hold the masked columns (email) unmasked; drop keep_record or the mask",
            ),
        },
    ];
    for test_case in test_cases {
        let fields: Vec<FieldSpec> = test_case
            .fields
            .iter()
            .map(|name| FieldSpec {
                name: (*name).to_owned(),
                path: format!("$.{name}"),
                required: false,
                parse: None,
                on_error: BadValue::Fail,
                from: FieldFrom::Row,
            })
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
            naming: (test_case.naming, None),
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
