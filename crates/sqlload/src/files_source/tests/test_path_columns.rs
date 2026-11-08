use std::collections::BTreeMap;

use crate::column_types::models::Warehouse;
use crate::file_store::models::StoredFile;
use crate::files_source::_helpers::plan::partitions::data_file;
use crate::files_source::main::constant_columns::constant_columns;
use crate::files_source::main::path_template::path_template;
use crate::files_source::models::{PathValues, UnmatchedPaths};
use crate::files_source::tests::test_types::{
    ConstantColumnsTestCase, PathColumnsTestCase, PathTemplateErrorTestCase,
};

#[test]
fn given_a_path_template_when_listing_keys_then_captures_become_typed_values() -> Result<(), String>
{
    let test_cases = [
        PathColumnsTestCase {
            description: "plain folders: text captures and an ISO date, the file name matched by *",
            template: "{region}/{dataset}/{snapshot_date:date}/*",
            unmatched: UnmatchedPaths::Fail,
            key: "west/daily/2026-11-08/part-0.parquet",
            expected_values: Some("region=west,dataset=daily,snapshot_date=2026-11-08,source=lake"),
            expected_problem: "",
        },
        PathColumnsTestCase {
            description: "a compact date between wildcards is found by its digits, not by the first _, and stored as an ISO date",
            template: "*_{run_date:date}_*.json",
            unmatched: UnmatchedPaths::Fail,
            key: "report_a_20261108_v2.json",
            expected_values: Some("run_date=2026-11-08,source=lake"),
            expected_problem: "",
        },
        PathColumnsTestCase {
            description: "an integer capture takes digits only, so the text after it stays literal",
            template: "{name}-{part:int}.parquet",
            unmatched: UnmatchedPaths::Fail,
            key: "scores-2026-007.parquet",
            expected_values: Some("name=scores-2026,part=007,source=lake"),
            expected_problem: "",
        },
        PathColumnsTestCase {
            description: "** matches no folder",
            template: "**/{day:date}.parquet",
            unmatched: UnmatchedPaths::Fail,
            key: "2026-11-08.parquet",
            expected_values: Some("day=2026-11-08,source=lake"),
            expected_problem: "",
        },
        PathColumnsTestCase {
            description: "** matches several folders",
            template: "**/{day:date}.parquet",
            unmatched: UnmatchedPaths::Fail,
            key: "a/b/2026-11-09.parquet",
            expected_values: Some("day=2026-11-09,source=lake"),
            expected_problem: "",
        },
        PathColumnsTestCase {
            description: "a template's dots are literal",
            template: "{day:date}.parquet",
            unmatched: UnmatchedPaths::Fail,
            key: "2026-11-08Xparquet",
            expected_values: Some("source=lake"),
            expected_problem: "2026-11-08Xparquet does not match path_columns",
        },
        PathColumnsTestCase {
            description: "a template matches whole keys: no folder before it",
            template: "{day:date}.parquet",
            unmatched: UnmatchedPaths::Fail,
            key: "x/2026-11-08.parquet",
            expected_values: Some("source=lake"),
            expected_problem: "x/2026-11-08.parquet does not match path_columns",
        },
        PathColumnsTestCase {
            description: "unmatched_paths = skip still loads matching files",
            template: "{region}/*.parquet",
            unmatched: UnmatchedPaths::Skip,
            key: "west/a.parquet",
            expected_values: Some("region=west,source=lake"),
            expected_problem: "",
        },
        PathColumnsTestCase {
            description: "unmatched_paths = skip leaves a file with too few folders out without a problem",
            template: "{region}/*.parquet",
            unmatched: UnmatchedPaths::Skip,
            key: "a.parquet",
            expected_values: None,
            expected_problem: "",
        },
        PathColumnsTestCase {
            description: "* stays inside one folder, so a deeper file is skipped",
            template: "{region}/*.parquet",
            unmatched: UnmatchedPaths::Skip,
            key: "west/deep/a.parquet",
            expected_values: None,
            expected_problem: "",
        },
        PathColumnsTestCase {
            description: "a captured value that is not a valid date fails, naming the file, even with skip",
            template: "{day:date}/*",
            unmatched: UnmatchedPaths::Skip,
            key: "2026-13-45/a.parquet",
            expected_values: Some("source=lake"),
            expected_problem: "2026-13-45/a.parquet: path column day is \"2026-13-45\", which is not a date",
        },
    ];
    for test_case in test_cases {
        let values = PathValues {
            template: Some(path_template(test_case.template, &[Warehouse::DuckDb])?),
            unmatched: test_case.unmatched,
            constants: vec![("source".to_owned(), "lake".to_owned())],
            ..PathValues::default()
        };
        let file = StoredFile {
            path: object_store::path::Path::from(test_case.key),
            relative_key: test_case.key.to_owned(),
            size: 1,
            etag: None,
            modified_ms: 0,
        };
        let (data, problem) = data_file(file, &values);
        let found: Option<String> = data.map(|data| {
            data.partitions
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join(",")
        });
        let problem = problem.unwrap_or_default();
        assert_eq!(
            found.as_deref(),
            test_case.expected_values,
            "{}",
            test_case.description
        );
        assert!(
            problem.starts_with(test_case.expected_problem)
                && problem.is_empty() == test_case.expected_problem.is_empty(),
            "{}: {problem}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_malformed_path_template_when_compiling_then_the_error_says_what_is_wrong() {
    let test_cases = [
        PathTemplateErrorTestCase {
            description: "a brace never closed",
            template: "{region/*",
            expected_error: "path_columns \"{region/*\": { in \"{region\" is never closed",
        },
        PathTemplateErrorTestCase {
            description: "a trailing slash would match no file",
            template: "{region}/",
            expected_error: "cannot end in /",
        },
        PathTemplateErrorTestCase {
            description: "a template without captures adds no column",
            template: "*/*.parquet",
            expected_error: "it captures nothing",
        },
        PathTemplateErrorTestCase {
            description: "the same column captured twice",
            template: "{day:date}/{day:date}.parquet",
            expected_error: "day is captured twice",
        },
        PathTemplateErrorTestCase {
            description: "a capture name must be a plain column name",
            template: "{a b}/*",
            expected_error: "must name a column",
        },
        PathTemplateErrorTestCase {
            description: "an unknown type",
            template: "{day:dat}/*",
            expected_error: "path_columns \"{day:dat}/*\": day: ",
        },
    ];
    for test_case in test_cases {
        let error = path_template(test_case.template, &[])
            .err()
            .unwrap_or_default();
        assert!(
            error.contains(test_case.expected_error),
            "{}: {error}",
            test_case.description
        );
    }
}

#[test]
fn given_constant_columns_when_typing_them_then_their_values_or_declared_types_decide()
-> Result<(), String> {
    let test_cases = [
        ConstantColumnsTestCase {
            description: "scalars type themselves, in column name order",
            toml: "a = \"west\"\nb = 3\nc = 1.5\nd = true\n",
            expected_columns: Some("a:text=west,b:int=3,c:float=1.5,d:bool=true"),
            expected_error: "",
        },
        ConstantColumnsTestCase {
            description: "{ value, type } takes any type, the value as text",
            toml: "a = { value = \"2026-11-08\", type = \"date\" }\nb = { value = 12.5, type = \"decimal(10,2)\" }\nc = { value = \"2026-11-08T05:00:00Z\", type = \"timestamp_tz\" }\n",
            expected_columns: Some(
                "a:date=2026-11-08,b:decimal(10,2)=12.5,c:timestamp_tz=2026-11-08T05:00:00Z",
            ),
            expected_error: "",
        },
        ConstantColumnsTestCase {
            description: "a declared value that does not convert fails",
            toml: "a = { value = \"2026-02-30\", type = \"date\" }\n",
            expected_columns: None,
            expected_error: "constant column a: \"2026-02-30\" is not a date",
        },
        ConstantColumnsTestCase {
            description: "an array is not a column value",
            toml: "a = [1, 2]\n",
            expected_columns: None,
            expected_error: "constant column a: [1, 2] must be a string, number or boolean",
        },
    ];
    for test_case in test_cases {
        let constants: BTreeMap<String, toml::Value> =
            toml::from_str(test_case.toml).map_err(|error| error.to_string())?;
        let found = constant_columns(&constants, &[Warehouse::DuckDb]).map(|columns| {
            columns
                .iter()
                .map(|(name, column_type, value)| format!("{name}:{column_type}={value}"))
                .collect::<Vec<_>>()
                .join(",")
        });
        let error = found.clone().err().unwrap_or_default();
        assert_eq!(
            found.ok().as_deref(),
            test_case.expected_columns,
            "{}",
            test_case.description
        );
        assert!(
            error.starts_with(test_case.expected_error)
                && error.is_empty() == test_case.expected_error.is_empty(),
            "{}: {error}",
            test_case.description
        );
    }
    Ok(())
}
