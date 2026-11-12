use crate::adapters::contract::models::TableName;
use crate::adapters::duckdb::_helpers::pull_sql::{PulledFile, pull_insert};
use crate::adapters::duckdb::tests::helpers::column;
use crate::adapters::duckdb::tests::test_types::PullInsertTestCase;
use crate::column_types::models::ColumnType;

#[test]
fn given_parquet_files_when_duckdb_reads_them_itself_then_each_column_comes_from_the_right_place() {
    let test_cases = [PullInsertTestCase {
        description: "file columns are cast, a partition comes from each file's own entry in a list indexed by the row's file (NULL where its folder lacks it), rows count from 1, the load id is the batch's, and a column no file holds is NULL",
        columns: vec![
            column("id", ColumnType::Int),
            column("day", ColumnType::Date),
            column("note", ColumnType::Text),
            column("_sqlload_source_row", ColumnType::Int),
            column("_sqlload_load_id", ColumnType::Text),
        ],
        in_files: vec!["id".to_owned(), "day".to_owned()],
        files: vec![
            PulledFile {
                url: "s3://b/x/a.parquet".to_owned(),
                constants: vec![("day".to_owned(), Some("2026-01-01".to_owned()))],
            },
            PulledFile {
                url: "s3://b/x/it's.parquet".to_owned(),
                constants: vec![("day".to_owned(), None)],
            },
        ],
        expected_sql: "INSERT INTO \"RAW\".\"orders\" (\"id\", \"day\", \"note\", \"_sqlload_source_row\", \"_sqlload_load_id\") SELECT CAST(f.\"id\" AS BIGINT), CAST(['2026-01-01', NULL][CAST(f.file_index AS BIGINT) + 1] AS DATE), NULL, CAST(f.file_row_number + 1 AS BIGINT), CAST('L1' AS VARCHAR) FROM read_parquet(['s3://b/x/a.parquet', 's3://b/x/it''s.parquet'], union_by_name = true, file_row_number = true, hive_partitioning = false) AS f",
    }];
    let table = TableName {
        database: None,
        schema: "RAW".to_owned(),
        table: "orders".to_owned(),
    };
    for test_case in test_cases {
        assert_eq!(
            pull_insert(
                &table,
                (&test_case.columns, &test_case.in_files),
                &test_case.files,
                (
                    crate::load_naming::models::Naming::Sqlload,
                    "L1",
                    "2026-11-06T00:00:00Z"
                ),
            ),
            test_case.expected_sql,
            "{}",
            test_case.description
        );
    }
}
