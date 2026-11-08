use crate::column_types::models::ColumnType;
use crate::files_source::models::{CsvOptions, SourceFormat};
use crate::files_source::tests::helpers::{
    block_on, csv_encoding, encoding, read_rows, record_encoding,
};
use crate::files_source::tests::test_types::ReadTextFilesTestCase;

#[test]
fn given_json_and_csv_files_when_reading_then_rows_convert_strictly_and_errors_name_the_line()
-> Result<(), String> {
    let test_cases = [
        ReadTextFilesTestCase {
            description: "ndjson: nested values stay JSON text with keys in source order, missing keys and nulls are NULL, blank lines are skipped",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[(
                "a.ndjson",
                "{\"id\":1,\"meta\":{\"k\":[1,2],\"b\": 1},\"note\":\"x\"}\n\n{\"id\":2,\"note\":null}\r\n{\"id\":3,\"meta\":{},\"note\":\"y\"}",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|{\"k\":[1,2],\"b\":1}|x\n2|NULL|NULL\n3|{}|y"),
        },
        ReadTextFilesTestCase {
            description: "a pinned type converts every value; a key repeated in one object keeps its last value",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[(
                "a.ndjson.gz",
                "{\"id\":1,\"id\":7,\"v\":2}\n{\"id\":2,\"v\":2.5}\n",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[("id", ColumnType::Text)],
            expected_rows: Ok("7|2.0\n2|2.5"),
        },
        ReadTextFilesTestCase {
            description: "a value that does not convert to a pinned type fails with its line, in any batch",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("a.ndjson", "{\"id\":1}\n{\"id\":2}\n{\"id\":\"three\"}\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[("id", ColumnType::Int)],
            expected_rows: Err("a.ndjson: line 3: id: cannot convert to Int64"),
        },
        ReadTextFilesTestCase {
            description: "json reads objects over several lines, and in a top-level array",
            encoding: encoding(SourceFormat::Json),
            files: &[
                ("a.json", "{\"id\": 1,\n \"s\": \"}{\\\"\"}\n{\"id\": 2}"),
                (
                    "b.json",
                    "[\n  {\"id\": 3, \"s\": \"x\"},\n  {\"id\": 4}\n]\n",
                ),
            ],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|}{\"\n2|NULL\n3|x\n4|NULL"),
        },
        ReadTextFilesTestCase {
            description: "json: a missing comma between array elements names the line",
            encoding: encoding(SourceFormat::Json),
            files: &[("a.json", "[\n{\"id\":1}\n{\"id\":2}]")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.json: line 3: unexpected '{' between array elements"),
        },
        ReadTextFilesTestCase {
            description: "ndjson: an object split over lines is malformed",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("a.ndjson", "{\"id\":1}\n{\"id\":\n2}\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.ndjson: line 2: EOF while parsing"),
        },
        ReadTextFilesTestCase {
            description: "csv: quoted fields hold delimiters and doubled quotes; empty fields are NULL",
            encoding: encoding(SourceFormat::Csv),
            files: &[(
                "a.csv",
                "id,name,ok\r\n1,\"a,b\",true\r\n2,\"say \"\"hi\"\"\",FALSE\r\n3,,\r\n",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|a,b|true\n2|say \"hi\"|false\n3|NULL|NULL"),
        },
        ReadTextFilesTestCase {
            description: "csv: files with different headers are matched by column name",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.csv", "id\n1\n"), ("b.csv.gz", "name,id\nx,2\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("NULL|1\nx|2"),
        },
        ReadTextFilesTestCase {
            description: "csv: a value that does not convert names its line, counting the header",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.csv", "id\n1\n2\nx\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[("id", ColumnType::Int)],
            expected_rows: Err("a.csv: line 4: id: cannot convert to Int64"),
        },
        ReadTextFilesTestCase {
            description: "csv: a record with the wrong number of fields names its line",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.csv", "id,name\n1,a\n2\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.csv: incorrect number of fields for line 3, expected 2 got 1"),
        },
        ReadTextFilesTestCase {
            description: "csv: no header, a custom quote, and no NULL markers keeps empty text",
            encoding: csv_encoding(CsvOptions {
                header: false,
                quote: '\'',
                null_values: Vec::new(),
                ..CsvOptions::default()
            }),
            files: &[("a.csv", "'a,b',\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("a,b|"),
        },
        ReadTextFilesTestCase {
            description: "csv: a quoted field the file never closes is an error, not one field holding the rest of the file",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.csv", "a,b\n1,\"abc\n2,x\n3,y\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.csv: line 2: unterminated quoted field"),
        },
        ReadTextFilesTestCase {
            description: "csv: a file cut off inside a quoted field is an error",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.csv", "a\n\n\"x\"\n\"ab")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.csv: line 3: unterminated quoted field"),
        },
        ReadTextFilesTestCase {
            description: "csv: a closed quote at the end of the file is fine",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.csv", "a\n\"x\"\"\"")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("x\""),
        },
        ReadTextFilesTestCase {
            description: "csv: one record without a header or a final line break is read",
            encoding: csv_encoding(CsvOptions {
                header: false,
                ..CsvOptions::default()
            }),
            files: &[("a.csv", "1,2")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|2"),
        },
        ReadTextFilesTestCase {
            description: "ndjson: only the last value of a key repeated in one object is checked against the inferred type, as Snowflake keeps only that one",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("b.ndjson", "{\"id\":1}\n")],
            unsampled: &[("a.ndjson", "{\"id\":\"x\",\"id\":2}\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("2\n1"),
        },
        ReadTextFilesTestCase {
            description: "an inferred int column rejects a leading zero outside the sample",
            encoding: encoding(SourceFormat::Csv),
            files: &[("b.csv", "id\n1\n")],
            unsampled: &[("a.csv", "id\n2\n007\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Err(
                "a.csv: line 3: id: \"007\" does not fit int, the type inferred from sampled values",
            ),
        },
        ReadTextFilesTestCase {
            description: "ndjson: a key repeated in a nested object keeps its last value, in the place of its first",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[(
                "a.ndjson",
                "{\"id\":1,\"meta\":{\"b\":1,\"a\":[{\"x\":1,\"x\":2}],\"b\":3}}\n",
            )],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1|{\"b\":3,\"a\":[{\"x\":2}]}"),
        },
        ReadTextFilesTestCase {
            description: "a nested value outside the sample that does not fit the inferred type is shown in the error",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("b.ndjson", "{\"id\":1}\n")],
            unsampled: &[("a.ndjson", "{\"id\":2}\n{\"id\":{\"k\": 1}}\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Err(
                "a.ndjson: line 2: id: {\"k\":1} does not fit int, the type inferred from sampled values",
            ),
        },
        ReadTextFilesTestCase {
            description: "an inferred bool column takes only true and false",
            encoding: encoding(SourceFormat::Csv),
            files: &[("b.csv", "ok\ntrue\n")],
            unsampled: &[("a.csv", "ok\nyes\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.csv: line 2: ok: \"yes\" does not fit bool"),
        },
        ReadTextFilesTestCase {
            description: "an inferred timestamp column needs an offset",
            encoding: encoding(SourceFormat::Csv),
            files: &[("b.csv", "at\n2026-11-04T10:00:00Z\n")],
            unsampled: &[("a.csv", "at\n2026-11-04\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.csv: line 2: at: \"2026-11-04\" does not fit timestamp_tz"),
        },
        ReadTextFilesTestCase {
            description: "an inferred float column rejects infinity",
            encoding: encoding(SourceFormat::Csv),
            files: &[("b.csv", "v\n1.5\n")],
            unsampled: &[("a.csv", "v\ninf\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.csv: line 2: v: \"inf\" does not fit float"),
        },
        ReadTextFilesTestCase {
            description: "an inferred JSON int column rejects a string holding a number",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("b.ndjson", "{\"id\":1}\n")],
            unsampled: &[("a.ndjson", "{\"id\":2}\n{\"id\":\"12\"}\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.ndjson: line 2: id: \"12\" does not fit int"),
        },
        ReadTextFilesTestCase {
            description: "a pinned column keeps the lenient conversion",
            encoding: encoding(SourceFormat::Csv),
            files: &[("b.csv", "ok\ntrue\n")],
            unsampled: &[("a.csv", "ok\nyes\n")],
            exclude: &[],
            pinned: &[("ok", ColumnType::Bool)],
            expected_rows: Ok("true\ntrue"),
        },
        ReadTextFilesTestCase {
            description: "a JSON key outside the sampled schema is an error",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("b.ndjson", "{\"id\":1}\n")],
            unsampled: &[("a.ndjson", "{\"id\":2,\"late\":true}\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Err(
                "a.ndjson: line 1: key late is not in the sampled schema; pin it in columns or add it to exclude_columns",
            ),
        },
        ReadTextFilesTestCase {
            description: "a key exclude_columns matches is dropped even outside the sample",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("b.ndjson", "{\"id\":1}\n")],
            unsampled: &[("a.ndjson", "{\"id\":2,\"late\":true}\n")],
            exclude: &["la*"],
            pinned: &[],
            expected_rows: Ok("2\n1"),
        },
        ReadTextFilesTestCase {
            description: "a CSV header outside the sampled schema is an error",
            encoding: encoding(SourceFormat::Csv),
            files: &[("b.csv", "id\n1\n")],
            unsampled: &[("a.csv", "id,late\n2,x\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.csv: line 1: header late is not in the sampled schema"),
        },
        ReadTextFilesTestCase {
            description: "json: a trailing comma in the top-level array is an error",
            encoding: encoding(SourceFormat::Json),
            files: &[("a.json", "[{\"a\":1},]")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.json: line 1: unexpected ']' after a trailing comma"),
        },
        ReadTextFilesTestCase {
            description: "json: an empty top-level array has no rows",
            encoding: encoding(SourceFormat::Json),
            files: &[("a.json", "[{\"a\":1}]"), ("b.json", "[ ]")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("1"),
        },
        ReadTextFilesTestCase {
            description: "record shape, ndjson: each object whole as compact JSON, numbers keeping their digits, keys in file order (a repeated key once, with its last value where it last appears), keys outside any sample kept",
            encoding: record_encoding(SourceFormat::Ndjson, CsvOptions::default()),
            files: &[(
                "a.ndjson.gz",
                "{\"b\": 1, \"a\": {\"x\": [1, 2.50]}, \"b\": \"q\\\"\\u00e9\"}\n\n{\"z\": null}\n{}\n",
            )],
            unsampled: &[("b.ndjson", "{\"late\": true}\n")],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok(
                "{\"a\":{\"x\":[1,2.50]},\"b\":\"q\\\"\u{e9}\"}\n{\"z\":null}\n{}\n{\"late\":true}",
            ),
        },
        ReadTextFilesTestCase {
            description: "record shape, json: objects in a top-level array; a record that is not an object names its line",
            encoding: record_encoding(SourceFormat::Json, CsvOptions::default()),
            files: &[("a.json", "[\n{\"id\": 1},\n[1, 2]\n]")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.json: line 3: "),
        },
        ReadTextFilesTestCase {
            description: "record shape, csv: each line an array of its fields as text, NULL values as null, headers skipped unchecked even when files differ",
            encoding: record_encoding(SourceFormat::Csv, CsvOptions::default()),
            files: &[
                ("a.csv", "id,name\n1,\"a,b\"\n2,\n"),
                ("b.csv.gz", "x,x\n\"say \"\"hi\"\"\",007\n"),
            ],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Ok("[\"1\",\"a,b\"]\n[\"2\",null]\n[\"say \\\"hi\\\"\",\"007\"]"),
        },
        ReadTextFilesTestCase {
            description: "record shape, csv without a header: the first line is a record, and a line with a different number of fields names its line",
            encoding: record_encoding(
                SourceFormat::Csv,
                CsvOptions {
                    header: false,
                    ..CsvOptions::default()
                },
            ),
            files: &[("a.csv", "1,2\n3\n")],
            unsampled: &[],
            exclude: &[],
            pinned: &[],
            expected_rows: Err("a.csv: incorrect number of fields for line 2, expected 2 got 1"),
        },
    ];
    for test_case in test_cases {
        let rows = block_on(async { Ok(read_rows(&test_case).await) })?;
        assert_eq!(
            rows.as_deref().ok(),
            test_case.expected_rows.ok(),
            "{}: {rows:?}",
            test_case.description
        );
        assert!(
            rows.as_ref().err().is_none_or(
                |error| error.contains(test_case.expected_rows.err().unwrap_or_default())
            ),
            "{}: {rows:?}",
            test_case.description
        );
    }
    Ok(())
}
