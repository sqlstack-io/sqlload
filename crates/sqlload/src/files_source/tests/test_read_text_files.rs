use crate::column_types::models::ColumnType;
use crate::files_source::models::{CsvOptions, SourceFormat};
use crate::files_source::tests::helpers::{block_on, csv_encoding, encoding, read_rows};
use crate::files_source::tests::test_types::ReadTextFilesTestCase;

#[test]
fn given_json_and_csv_files_when_reading_then_rows_convert_strictly_and_errors_name_the_line()
-> Result<(), String> {
    let test_cases = [
        ReadTextFilesTestCase {
            description: "ndjson: nested values stay JSON text, missing keys and nulls are NULL, blank lines are skipped",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[(
                "a.ndjson",
                "{\"id\":1,\"meta\":{\"k\":[1,2]},\"note\":\"x\"}\n\n{\"id\":2,\"note\":null}\r\n{\"id\":3,\"meta\":{},\"note\":\"y\"}",
            )],
            pinned: &[],
            expected_rows: Ok("1|{\"k\":[1,2]}|x\n2|NULL|NULL\n3|{}|y"),
        },
        ReadTextFilesTestCase {
            description: "a pinned type converts every value; a key repeated in one object keeps its last value",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[(
                "a.ndjson.gz",
                "{\"id\":1,\"id\":7,\"v\":2}\n{\"id\":2,\"v\":2.5}\n",
            )],
            pinned: &[("id", ColumnType::Text)],
            expected_rows: Ok("7|2.0\n2|2.5"),
        },
        ReadTextFilesTestCase {
            description: "a value that does not convert to a pinned type fails with its line, in any batch",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("a.ndjson", "{\"id\":1}\n{\"id\":2}\n{\"id\":\"three\"}\n")],
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
            pinned: &[],
            expected_rows: Ok("1|}{\"\n2|NULL\n3|x\n4|NULL"),
        },
        ReadTextFilesTestCase {
            description: "json: a missing comma between array elements names the line",
            encoding: encoding(SourceFormat::Json),
            files: &[("a.json", "[\n{\"id\":1}\n{\"id\":2}]")],
            pinned: &[],
            expected_rows: Err("a.json: line 3: unexpected '{' between array elements"),
        },
        ReadTextFilesTestCase {
            description: "ndjson: an object split over lines is malformed",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("a.ndjson", "{\"id\":1}\n{\"id\":\n2}\n")],
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
            pinned: &[],
            expected_rows: Ok("1|a,b|true\n2|say \"hi\"|false\n3|NULL|NULL"),
        },
        ReadTextFilesTestCase {
            description: "csv: files with different headers are matched by column name",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.csv", "id\n1\n"), ("b.csv.gz", "name,id\nx,2\n")],
            pinned: &[],
            expected_rows: Ok("NULL|1\nx|2"),
        },
        ReadTextFilesTestCase {
            description: "csv: a value that does not convert names its line, counting the header",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.csv", "id\n1\n2\nx\n")],
            pinned: &[("id", ColumnType::Int)],
            expected_rows: Err("a.csv: line 4: id: cannot convert to Int64"),
        },
        ReadTextFilesTestCase {
            description: "csv: a record with the wrong number of fields names its line",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.csv", "id,name\n1,a\n2\n")],
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
            pinned: &[],
            expected_rows: Ok("a,b|"),
        },
    ];
    for test_case in test_cases {
        let rows = block_on(async {
            Ok(read_rows(test_case.files, &test_case.encoding, test_case.pinned).await)
        })?;
        assert_eq!(
            rows.as_deref().ok(),
            test_case.expected_rows.ok(),
            "{}",
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
