use crate::adapters::bigquery::_helpers::dialect::pull::{PulledFile, PulledGroup, pull_query};
use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::bigquery::tests::helpers::{desired, orders};
use crate::adapters::bigquery::tests::test_types::PullQueryTestCase;
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::load_naming::models::Compat;

#[test]
fn given_parquet_files_in_gcs_when_bigquery_reads_them_itself_then_each_column_comes_from_the_right_place()
 {
    let file = |url: &str, day: Option<&str>| PulledFile {
        url: url.to_owned(),
        constants: vec![
            ("day".to_owned(), day.map(str::to_owned)),
            ("_sqlload_source_file".to_owned(), Some(url.to_owned())),
        ],
    };
    let test_cases = [PullQueryTestCase {
        description: "each group of files holding the same columns is its own external table: its columns are cast from their names in the files, a partition and the file's name come from the row's _FILE_NAME (NULL where its folder lacks it), the load id is the batch's, and a column the group's files lack is a typed NULL; types are cast to without parameters, which a cast can't name",
        columns: vec![
            desired("id", ColumnType::Int),
            desired(
                "amount",
                ColumnType::Decimal {
                    precision: 12,
                    scale: 2,
                },
            ),
            desired("code", ColumnType::Varchar(8)),
            desired("day", ColumnType::Date),
            desired("_sqlload_source_file", ColumnType::Text),
            desired("_sqlload_load_id", ColumnType::Text),
            desired("note", ColumnType::Text),
        ],
        groups: vec![
            PulledGroup {
                files: vec![
                    file("gs://b/x/a.parquet", Some("2026-01-01")),
                    file("gs://b/x/it's.parquet", None),
                ],
                in_files: vec![
                    ("id".to_owned(), "Order_ID".to_owned()),
                    ("amount".to_owned(), "amount".to_owned()),
                ],
            },
            PulledGroup {
                files: vec![file("gs://b/x/c.parquet", Some("2026-01-02"))],
                in_files: vec![("id".to_owned(), "id".to_owned())],
            },
        ],
        expected_query: "INSERT INTO `p`.`raw`.`orders` (`id`, `amount`, `code`, `day`, `_sqlload_source_file`, `_sqlload_load_id`, `note`) SELECT CAST(f.`Order_ID` AS INT64), CAST(f.`amount` AS NUMERIC), CAST(NULL AS STRING), CAST(CASE _FILE_NAME WHEN 'gs://b/x/a.parquet' THEN '2026-01-01' WHEN 'gs://b/x/it\\'s.parquet' THEN CAST(NULL AS STRING) END AS DATE), CAST(CASE _FILE_NAME WHEN 'gs://b/x/a.parquet' THEN 'gs://b/x/a.parquet' WHEN 'gs://b/x/it\\'s.parquet' THEN 'gs://b/x/it\\'s.parquet' END AS STRING), CAST('L1' AS STRING), CAST(NULL AS STRING) FROM sqlload_files_0 AS f UNION ALL SELECT CAST(f.`id` AS INT64), CAST(NULL AS NUMERIC), CAST(NULL AS STRING), CAST(CASE _FILE_NAME WHEN 'gs://b/x/c.parquet' THEN '2026-01-02' END AS DATE), CAST(CASE _FILE_NAME WHEN 'gs://b/x/c.parquet' THEN 'gs://b/x/c.parquet' END AS STRING), CAST('L1' AS STRING), CAST(NULL AS STRING) FROM sqlload_files_1 AS f",
        expected_definitions: r#"{"sqlload_files_0":{"decimalTargetTypes":["NUMERIC","BIGNUMERIC","STRING"],"sourceFormat":"PARQUET","sourceUris":["gs://b/x/a.parquet","gs://b/x/it's.parquet"]},"sqlload_files_1":{"decimalTargetTypes":["NUMERIC","BIGNUMERIC","STRING"],"sourceFormat":"PARQUET","sourceUris":["gs://b/x/c.parquet"]}}"#,
    }];
    for test_case in test_cases {
        let (query, definitions) = pull_query(
            &orders(),
            (&test_case.columns, &|column_type: &ColumnType| {
                BigQuerySql.render_staging_type(column_type)
            }),
            &test_case.groups,
            (Compat::Sqlload, "L1", "2026-11-06T00:00:00Z"),
        );
        assert_eq!(query, test_case.expected_query, "{}", test_case.description);
        assert_eq!(
            definitions.to_string(),
            test_case.expected_definitions,
            "{}",
            test_case.description
        );
    }
}
