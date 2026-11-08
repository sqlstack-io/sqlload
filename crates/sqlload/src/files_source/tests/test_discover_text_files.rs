use crate::files_source::main::discover_files::discover_files;
use crate::files_source::models::{CsvOptions, PathValues, SourceFormat};
use crate::files_source::tests::helpers::{
    block_on, column_list, csv_encoding, encoding, folder_with,
};
use crate::files_source::tests::test_types::{DiscoverTextFilesTestCase, SkippedFilesTestCase};

#[test]
fn given_json_and_csv_files_when_discovering_then_types_are_inferred_conservatively()
-> Result<(), String> {
    let test_cases = [
        DiscoverTextFilesTestCase {
            description: "JSON values: ints, mixed int and float, mixed kinds, nested values, and NULL-only keys",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[(
                "a.ndjson",
                concat!(
                    r#"{"id":1,"price":1,"name":"a","flag":true,"meta":{"k":1},"tags":[1],"empty":null}"#,
                    "\n",
                    r#"{"id":2,"price":1.5,"name":3,"flag":false,"meta":{"k":2},"empty":null}"#,
                    "\n"
                ),
            )],
            expected_columns: Ok(
                "id:int,price:float,name:text,flag:bool,meta:json,tags:json,empty:text",
            ),
            expected_problems: "",
        },
        DiscoverTextFilesTestCase {
            description: "JSON timestamps need RFC 3339 with an offset in every value; integers beyond 64 bits are text",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[(
                "a.jsonl",
                concat!(
                    r#"{"at":"2026-11-04T10:00:00+01:00","local":"2026-11-04T10:00:00","big":12345678901234567890,"small":-5}"#,
                    "\n",
                    r#"{"at":"2026-11-04T11:00:00Z","local":"2026-11-04T11:00:00Z","big":1,"small":0}"#,
                ),
            )],
            expected_columns: Ok("at:timestamp_tz,local:text,big:text,small:int"),
            expected_problems: "",
        },
        DiscoverTextFilesTestCase {
            description: "booleans mixed with numbers, and JSON mixed with strings, are text",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[(
                "a.ndjson",
                "{\"a\":true,\"b\":{\"x\":1}}\n{\"a\":1,\"b\":\"x\"}\n",
            )],
            expected_columns: Ok("a:text,b:text"),
            expected_problems: "",
        },
        DiscoverTextFilesTestCase {
            description: "CSV fields: numbers without leading zeros, booleans in any case, RFC 3339 timestamps",
            encoding: encoding(SourceFormat::Csv),
            files: &[(
                "a.csv",
                "id,amount,code,flag,at,note,scale\n1,2.5,007,true,2026-11-04T10:00:00Z,,1e3\n2,3,010,FALSE,2026-11-05T00:00:00Z,x,-2\n",
            )],
            expected_columns: Ok(
                "id:int,amount:float,code:text,flag:bool,at:timestamp_tz,note:text,scale:float",
            ),
            expected_problems: "",
        },
        DiscoverTextFilesTestCase {
            description: "a CSV without a header numbers its columns; a delimiter and NULL marker can be set",
            encoding: csv_encoding(CsvOptions {
                delimiter: ';',
                header: false,
                null_values: vec!["NA".to_owned()],
                ..CsvOptions::default()
            }),
            files: &[("a.csv", "1;\"x;y\";NA\n2;z;NA\n")],
            expected_columns: Ok("column_1:int,column_2:text,column_3:text"),
            expected_problems: "",
        },
        DiscoverTextFilesTestCase {
            description: "newer files come first, and a column only older files have is added after",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[
                ("day=1/a.ndjson", "{\"id\":1,\"old\":true}\n"),
                ("day=2/a.ndjson.gz", "{\"id\":2,\"extra\":\"x\"}\n"),
            ],
            expected_columns: Ok("id:int,extra:text,old:bool"),
            expected_problems: "",
        },
        DiscoverTextFilesTestCase {
            description: "a column NULL in every sampled value of a newer file takes an older file's type",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("a.ndjson", "{\"v\":1}\n"), ("b.ndjson", "{\"v\":null}\n")],
            expected_columns: Ok("v:int"),
            expected_problems: "",
        },
        DiscoverTextFilesTestCase {
            description: "a column whose type differs between files is a problem asking for a pinned type",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("a.ndjson", "{\"v\":1}\n"), ("b.ndjson", "{\"v\":\"x\"}\n")],
            expected_columns: Ok("v:text"),
            expected_problems: "column v is text in newer files but int in a.ndjson; pin a type that holds both",
        },
        DiscoverTextFilesTestCase {
            description: "JSON and CSV: int in one file and float in another make float",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("a.ndjson", "{\"v\":1.5}\n"), ("b.ndjson", "{\"v\":1}\n")],
            expected_columns: Ok("v:float"),
            expected_problems: "",
        },
        DiscoverTextFilesTestCase {
            description: "names that differ only in case are a problem naming both",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.csv", "Id,name\n1,x\n"), ("b.csv", "id,name\n2,y\n")],
            expected_columns: Ok("id:int,name:text,Id:int"),
            expected_problems: "columns id and Id differ only in case, which most warehouses cannot hold apart; rename one in the files",
        },
        DiscoverTextFilesTestCase {
            description: "json reads a top-level array of objects spanning lines",
            encoding: encoding(SourceFormat::Json),
            files: &[("a.json", "[\n  {\"a\": 1},\n  {\"a\": 2.5}\n]\n")],
            expected_columns: Ok("a:float"),
            expected_problems: "",
        },
        DiscoverTextFilesTestCase {
            description: "a malformed record names its file and line",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("a.ndjson", "{\"a\":1}\n\n{\"a\":\n")],
            expected_columns: Err("a.ndjson: line 3: EOF while parsing"),
            expected_problems: "",
        },
        DiscoverTextFilesTestCase {
            description: "a folder without files of the format is an error naming the suffixes",
            encoding: encoding(SourceFormat::Csv),
            files: &[("a.ndjson", "{\"a\":1}\n")],
            expected_columns: Err("no .csv, .csv.gz, .csv.zst files found"),
            expected_problems: "",
        },
    ];
    for test_case in test_cases {
        let found = block_on(async {
            let (folder, root) = folder_with(test_case.files).await?;
            let table = discover_files(&folder, &PathValues::default(), &test_case.encoding)
                .await
                .map_err(|error| error.to_string());
            let _ = std::fs::remove_dir_all(&root);
            Ok(table)
        })?;
        let columns: Result<String, String> = found.as_ref().map(column_list).map_err(Clone::clone);
        let problems: String = found
            .as_ref()
            .map(|table| table.problems.join(";"))
            .unwrap_or_default();
        assert_eq!(
            columns.as_deref().ok(),
            test_case.expected_columns.ok(),
            "{}",
            test_case.description
        );
        assert!(
            columns
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_columns.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
        assert_eq!(
            problems, test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_files_of_other_formats_when_discovering_then_they_are_counted_as_skipped()
-> Result<(), String> {
    let test_cases = [
        SkippedFilesTestCase {
            description: "files of another format or with a stray suffix are skipped",
            encoding: encoding(SourceFormat::Csv),
            files: &[
                ("a.csv", "id\n1\n"),
                ("b.ndjson", "{}\n"),
                ("c.csv.zst.tmp", ""),
            ],
            expected_counts: (1, 2),
        },
        SkippedFilesTestCase {
            description: "nothing is skipped when every file fits",
            encoding: encoding(SourceFormat::Ndjson),
            files: &[("a.ndjson", "{}\n"), ("b.jsonl.gz", "{}\n")],
            expected_counts: (2, 0),
        },
    ];
    for test_case in test_cases {
        let counts = block_on(async {
            let (folder, root) = folder_with(test_case.files).await?;
            let table = discover_files(&folder, &PathValues::default(), &test_case.encoding)
                .await
                .map_err(|error| error.to_string());
            let _ = std::fs::remove_dir_all(&root);
            table.map(|table| (table.files.len(), table.skipped))
        })?;
        assert_eq!(
            counts, test_case.expected_counts,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
