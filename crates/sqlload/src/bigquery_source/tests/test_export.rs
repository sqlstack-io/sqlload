use serde_json::json;

use crate::bigquery_source::_helpers::catalog::listed_table;
use crate::bigquery_source::_helpers::export::{export_columns, export_statement};
use crate::bigquery_source::main::export_refusal::export_refusal;
use crate::bigquery_source::models::BigQueryReadRequest;
use crate::bigquery_source::tests::test_types::{ExportColumnsTestCase, ExportRefusalTestCase};
use crate::column_types::models::{ColumnType, Warehouse};
use crate::postgres_source::models::{ReadColumn, WireType};
use crate::store_paths::models::{Location, StoreKind};

#[test]
fn given_a_bigquery_table_with_an_export_folder_when_planning_it_then_only_columns_parquet_holds_as_sqlload_loads_them_export()
 {
    let test_cases = [
        ExportRefusalTestCase {
            description: "scalar columns, with times written as microseconds, export",
            fields: vec![
                json!({"name": "id", "type": "INT64", "mode": "REQUIRED"}),
                json!({"name": "amount", "type": "NUMERIC", "precision": "12", "scale": "2"}),
                json!({"name": "at", "type": "TIMESTAMP"}),
                json!({"name": "local", "type": "DATETIME"}),
                json!({"name": "clock", "type": "TIME"}),
                json!({"name": "day", "type": "DATE"}),
                json!({"name": "raw", "type": "BYTES"}),
                json!({"name": "ok", "type": "BOOL"}),
            ],
            pinned: Vec::new(),
            warehouse: Warehouse::DuckDb,
            location: "EU",
            expected_refusal: None,
        },
        ExportRefusalTestCase {
            description: "JSON, which EXPORT DATA refuses for Parquet, keeps SQLLoad's reader",
            fields: vec![
                json!({"name": "id", "type": "INT64"}),
                json!({"name": "doc", "type": "JSON"}),
            ],
            pinned: Vec::new(),
            warehouse: Warehouse::DuckDb,
            location: "EU",
            expected_refusal: Some(
                "doc is BigQuery JSON, which BigQuery doesn't export to Parquet as SQLLoad loads it",
            ),
        },
        ExportRefusalTestCase {
            description: "BIGNUMERIC, wider than DuckDB's decimals, keeps SQLLoad's reader",
            fields: vec![json!({"name": "big", "type": "BIGNUMERIC", "precision": "38", "scale": "2"})],
            pinned: Vec::new(),
            warehouse: Warehouse::DuckDb,
            location: "EU",
            expected_refusal: Some(
                "big is BigQuery BIGNUMERIC, which BigQuery doesn't export to Parquet as SQLLoad loads it",
            ),
        },
        ExportRefusalTestCase {
            description: "a repeated column, which SQLLoad reads as JSON, keeps SQLLoad's reader",
            fields: vec![json!({"name": "tags", "type": "STRING", "mode": "REPEATED"})],
            pinned: Vec::new(),
            warehouse: Warehouse::DuckDb,
            location: "EU",
            expected_refusal: Some(
                "tags is BigQuery ARRAY<STRING>, which BigQuery doesn't export to Parquet as SQLLoad loads it",
            ),
        },
        ExportRefusalTestCase {
            description: "a column pinned to another type keeps SQLLoad's reader, which converts it",
            fields: vec![json!({"name": "id", "type": "INT64"})],
            pinned: vec![("id", ColumnType::Text)],
            warehouse: Warehouse::DuckDb,
            location: "EU",
            expected_refusal: Some("id is pinned as text, which SQLLoad converts from BigQuery INT64"),
        },
        ExportRefusalTestCase {
            description: "a pin to the column's own type changes nothing",
            fields: vec![json!({"name": "id", "type": "INT64"})],
            pinned: vec![("id", ColumnType::Int)],
            warehouse: Warehouse::DuckDb,
            location: "EU",
            expected_refusal: None,
        },
        ExportRefusalTestCase {
            description: "a table in another location than the connection's query jobs keeps SQLLoad's reader",
            fields: vec![json!({"name": "id", "type": "INT64"})],
            pinned: Vec::new(),
            warehouse: Warehouse::DuckDb,
            location: "US",
            expected_refusal: Some(
                "the table is in US, not the location query jobs on its connection run in",
            ),
        },
        ExportRefusalTestCase {
            description: "a target other than DuckDB doesn't read the export",
            fields: vec![json!({"name": "id", "type": "INT64"})],
            pinned: Vec::new(),
            warehouse: Warehouse::Postgres,
            location: "EU",
            expected_refusal: Some("only DuckDB and MotherDuck read a BigQuery export, not Postgres"),
        },
    ];
    for test_case in test_cases {
        let table = listed_table(
            ("shop".to_owned(), "orders".to_owned()),
            &json!({"type": "TABLE", "location": test_case.location, "schema": {"fields": test_case.fields}}),
            "EU",
        );
        let pinned = test_case
            .pinned
            .iter()
            .map(|(name, column_type)| ((*name).to_owned(), column_type.clone()))
            .collect();
        assert_eq!(
            export_refusal(&table, &pinned, test_case.warehouse).as_deref(),
            test_case.expected_refusal,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_read_columns_when_exporting_them_then_times_are_written_as_microseconds_under_their_read_names()
 {
    let test_cases = [
        ExportColumnsTestCase {
            description: "a renamed scalar keeps its source expression under its read name",
            field: json!({"name": "Order Id", "type": "INT64"}),
            read_name: "order_id",
            expected_expression: Ok("`Order Id` AS `order_id`"),
        },
        ExportColumnsTestCase {
            description: "a TIMESTAMP is its microseconds since the epoch",
            field: json!({"name": "at", "type": "TIMESTAMP"}),
            read_name: "at",
            expected_expression: Ok("UNIX_MICROS(`at`) AS `at`"),
        },
        ExportColumnsTestCase {
            description: "a DATETIME is its microseconds since 1970-01-01 00:00, read as UTC",
            field: json!({"name": "local", "type": "DATETIME"}),
            read_name: "local",
            expected_expression: Ok("UNIX_MICROS(TIMESTAMP(`local`, 'UTC')) AS `local`"),
        },
        ExportColumnsTestCase {
            description: "a TIME is its microseconds since midnight",
            field: json!({"name": "clock", "type": "TIME"}),
            read_name: "clock",
            expected_expression: Ok(
                "TIME_DIFF(`clock`, TIME '00:00:00', MICROSECOND) AS `clock`",
            ),
        },
        ExportColumnsTestCase {
            description: "a type the export doesn't write is named",
            field: json!({"name": "doc", "type": "JSON"}),
            read_name: "doc",
            expected_expression: Err(
                "doc is BigQuery JSON, which BigQuery doesn't export to Parquet as SQLLoad loads it",
            ),
        },
    ];
    for test_case in test_cases {
        let source_name = test_case.field["name"].as_str().unwrap_or_default().to_owned();
        let mut table = listed_table(
            ("shop".to_owned(), "orders".to_owned()),
            &json!({"type": "TABLE", "location": "EU", "schema": {"fields": [test_case.field]}}),
            "EU",
        );
        for column in &mut table.columns {
            column.source_spelling = Some(source_name.clone());
            column.name = test_case.read_name.to_owned();
        }
        let read = [ReadColumn {
            name: test_case.read_name.to_owned(),
            expression: format!("`{source_name}`"),
            wire: WireType::Text,
            output: ColumnType::Text,
        }];
        let request = BigQueryReadRequest {
            table: &table,
            sql: None,
            columns: &read,
            filter: None,
            filtered: false,
        };
        let expression = export_columns(&request)
            .map(|columns| columns.into_iter().map(|column| column.expression).collect::<Vec<_>>());
        assert_eq!(
            expression,
            test_case
                .expected_expression
                .map(|expected| vec![expected.to_owned()])
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_an_export_folder_when_writing_the_statement_then_its_uri_names_the_folders_parquet_files_with_quotes_escaped()
 {
    let folder = Location {
        store: StoreKind::Gcs {
            bucket: "exports".to_owned(),
            service_account_key: None,
            endpoint: None,
            hmac: None,
        },
        prefix: "it's/sqlload_export_1/".to_owned(),
    };
    assert_eq!(
        export_statement(&folder, "SELECT 1"),
        "EXPORT DATA OPTIONS (uri = 'gs://exports/it\\'s/sqlload_export_1/*.parquet', format = 'PARQUET') AS SELECT 1"
    );
}
