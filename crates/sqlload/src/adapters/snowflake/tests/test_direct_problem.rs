use crate::adapters::snowflake::main::copy::direct_problem::direct_problem;
use crate::adapters::snowflake::models::{CopyValue, DirectCheck};
use crate::adapters::snowflake::tests::helpers::{column, data_file, lake, order_columns, stage};
use crate::adapters::snowflake::tests::test_types::DirectProblemTestCase;
use crate::column_types::models::ColumnType;
use crate::files_source::models::{DataFile, SourceFormat};
use crate::project::models::MaskMethod;

#[test]
fn given_a_files_table_when_choosing_the_route_then_only_exact_loads_go_direct() {
    let test_cases = [
        DirectProblemTestCase {
            description: "a column the files hold as an Arrow type the COPY does not load exactly stays with SQLLoad, named with its type",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["day=2026-11-04/part-0.parquet"],
            columns: order_columns(),
            masks: Vec::new(),
            refused: vec![("name".to_owned(), "Timestamp(ns)".to_owned())],
            shaped_by_loader: false,
            expected_problem: Some(
                "name is Timestamp(ns) in the files, which the direct COPY does not load exactly",
            ),
        },
        DirectProblemTestCase {
            description: "a refused column the table does not read does not matter",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["day=2026-11-04/part-0.parquet"],
            columns: order_columns(),
            masks: Vec::new(),
            refused: vec![("excluded".to_owned(), "UInt64".to_owned())],
            shaped_by_loader: false,
            expected_problem: None,
        },
        DirectProblemTestCase {
            description: "shaping = loader forces SQLLoad's route, whatever else fits",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["day=2026-11-04/part-0.parquet"],
            columns: order_columns(),
            masks: Vec::new(),
            refused: Vec::new(),
            shaped_by_loader: true,
            expected_problem: Some("the table sets shaping = \"loader\""),
        },
        DirectProblemTestCase {
            description: "JSON pinned over a text column loads directly, parsed",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["day=2026-11-04/part-0.parquet"],
            columns: vec![column(
                "profile",
                ColumnType::Json,
                CopyValue::File {
                    file_type: Some(ColumnType::Text),
                },
            )],
            masks: Vec::new(),
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: None,
        },
        DirectProblemTestCase {
            description: "the row number needs no check",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["day=2026-11-04/part-0.parquet"],
            columns: vec![column(
                "_sqlload_source_row",
                ColumnType::Int,
                CopyValue::SourceRow,
            )],
            masks: Vec::new(),
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: None,
        },
        DirectProblemTestCase {
            description: "Parquet under the stage url with plain columns loads directly",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["day=2026-11-04/part-0.parquet"],
            columns: order_columns(),
            masks: vec![("name".to_owned(), MaskMethod::Hash)],
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: None,
        },
        DirectProblemTestCase {
            description: "JSON stays with SQLLoad's own parsing",
            source: "raw/shop/orders/",
            format: SourceFormat::Ndjson,
            keys: &["part-0.ndjson"],
            columns: order_columns(),
            masks: Vec::new(),
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: Some(
                "ndjson files are parsed by SQLLoad, whose type inference and checks differ from Snowflake's",
            ),
        },
        DirectProblemTestCase {
            description: "files outside the stage url go through SQLLoad",
            source: "elsewhere/orders/",
            format: SourceFormat::Parquet,
            keys: &["part-0.parquet"],
            columns: order_columns(),
            masks: Vec::new(),
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: Some("the files are outside the stage url s3://lake/raw/"),
        },
        DirectProblemTestCase {
            description: "a key a stage path cannot name exactly",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["batch=10%3A00/part-0.parquet"],
            columns: order_columns(),
            masks: Vec::new(),
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: Some(
                "batch=10%3A00/part-0.parquet has characters a stage path cannot name exactly",
            ),
        },
        DirectProblemTestCase {
            description: "hmac never goes into SQL",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["part-0.parquet"],
            columns: order_columns(),
            masks: vec![("name".to_owned(), MaskMethod::Hmac)],
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: Some(
                "name is masked with hmac, which Snowflake cannot compute without its key in SQL",
            ),
        },
        DirectProblemTestCase {
            description: "hashing a column that is not text in the files would hash other text",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["part-0.parquet"],
            columns: vec![column(
                "id",
                ColumnType::Text,
                CopyValue::File {
                    file_type: Some(ColumnType::Int),
                },
            )],
            masks: vec![("id".to_owned(), MaskMethod::Hash)],
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: Some(
                "id is hashed but is not text in the files; SQLLoad hashes its text",
            ),
        },
        DirectProblemTestCase {
            description: "a pinned type the files do not hold needs SQLLoad's conversion",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["part-0.parquet"],
            columns: vec![column(
                "id",
                ColumnType::Text,
                CopyValue::File {
                    file_type: Some(ColumnType::Int),
                },
            )],
            masks: Vec::new(),
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: Some("id is int in the files but loads as text; SQLLoad converts it"),
        },
        DirectProblemTestCase {
            description: "UUID columns are not loaded directly",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["part-0.parquet"],
            columns: vec![column(
                "meta",
                ColumnType::Uuid,
                CopyValue::File {
                    file_type: Some(ColumnType::Json),
                },
            )],
            masks: Vec::new(),
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: Some("meta is uuid, which the direct COPY does not load exactly"),
        },
        DirectProblemTestCase {
            description: "a varchar partition column would be length-checked only by Snowflake",
            source: "raw/shop/orders/",
            format: SourceFormat::Parquet,
            keys: &["part-0.parquet"],
            columns: vec![column("code", ColumnType::Varchar(4), CopyValue::Partition)],
            masks: Vec::new(),
            refused: Vec::new(),
            shaped_by_loader: false,
            expected_problem: Some(
                "code is varchar(4), which the direct COPY does not load exactly",
            ),
        },
    ];
    let stage = stage();
    for test_case in test_cases {
        let files: Vec<DataFile> = test_case
            .keys
            .iter()
            .map(|key| data_file(key, "2026-11-04"))
            .collect();
        let problem = direct_problem(&DirectCheck {
            stage_url: &stage.location,
            source: &lake(test_case.source),
            format: test_case.format,
            files: &files,
            columns: &test_case.columns,
            masks: &test_case.masks,
            refused: &test_case.refused,
            shaped_by_loader: test_case.shaped_by_loader,
        });
        assert_eq!(
            problem.as_deref(),
            test_case.expected_problem,
            "{}",
            test_case.description
        );
    }
}
