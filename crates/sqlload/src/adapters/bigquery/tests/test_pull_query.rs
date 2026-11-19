use crate::adapters::bigquery::_helpers::dialect::pull::{
    PulledFile, PulledGroup, pull_queries, pull_query,
};
use crate::adapters::bigquery::constants::{MAX_FILES_PER_QUERY, MAX_QUERY_BYTES};
use crate::adapters::bigquery::tests::helpers::{desired, orders};
use crate::adapters::bigquery::tests::test_types::PullQueriesTestCase;
use crate::adapters::bigquery::tests::test_types::PullQueryTestCase;
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
        description: "each group of files holding the same columns is its own external table: its columns are cast from their names in the files, a partition and the file's name come from the row's _FILE_NAME (NULL where its folder lacks it, and an error for a file not in the batch, never a NULL), the load id is the batch's, and a column the group's files lack is a typed NULL; types are cast to without parameters, which a cast can't name",
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
        expected_query: "INSERT INTO `p`.`raw`.`orders` (`id`, `amount`, `code`, `day`, `_sqlload_source_file`, `_sqlload_load_id`, `note`) SELECT CAST(f.`Order_ID` AS INT64), CAST(f.`amount` AS NUMERIC), CAST(NULL AS STRING), CAST(CASE _FILE_NAME WHEN 'gs://b/x/a.parquet' THEN '2026-01-01' WHEN 'gs://b/x/it\\'s.parquet' THEN CAST(NULL AS STRING) ELSE ERROR(CONCAT('sqlload: unexpected file ', _FILE_NAME)) END AS DATE), CAST(CASE _FILE_NAME WHEN 'gs://b/x/a.parquet' THEN 'gs://b/x/a.parquet' WHEN 'gs://b/x/it\\'s.parquet' THEN 'gs://b/x/it\\'s.parquet' ELSE ERROR(CONCAT('sqlload: unexpected file ', _FILE_NAME)) END AS STRING), CAST('L1' AS STRING), CAST(NULL AS STRING) FROM sqlload_files_0 AS f UNION ALL SELECT CAST(f.`id` AS INT64), CAST(NULL AS NUMERIC), CAST(NULL AS STRING), CAST(CASE _FILE_NAME WHEN 'gs://b/x/c.parquet' THEN '2026-01-02' ELSE ERROR(CONCAT('sqlload: unexpected file ', _FILE_NAME)) END AS DATE), CAST(CASE _FILE_NAME WHEN 'gs://b/x/c.parquet' THEN 'gs://b/x/c.parquet' ELSE ERROR(CONCAT('sqlload: unexpected file ', _FILE_NAME)) END AS STRING), CAST('L1' AS STRING), CAST(NULL AS STRING) FROM sqlload_files_1 AS f",
        expected_definitions: r#"{"sqlload_files_0":{"decimalTargetTypes":["NUMERIC","BIGNUMERIC","STRING"],"sourceFormat":"PARQUET","sourceUris":["gs://b/x/a.parquet","gs://b/x/it's.parquet"]},"sqlload_files_1":{"decimalTargetTypes":["NUMERIC","BIGNUMERIC","STRING"],"sourceFormat":"PARQUET","sourceUris":["gs://b/x/c.parquet"]}}"#,
    }];
    for test_case in test_cases {
        let (query, definitions) = pull_query(
            &orders(),
            (&test_case.columns, false),
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

#[test]
fn given_a_batch_too_big_for_one_query_when_bigquery_reads_it_then_it_is_split_into_queries_under_the_limits()
 {
    let group = |files: usize, url_bytes: usize, in_files: &str| PulledGroup {
        files: (0..files)
            .map(|index| PulledFile {
                url: format!("gs://b/{index:0>url_bytes$}.parquet"),
                constants: vec![
                    ("day".to_owned(), Some(format!("{index:0>url_bytes$}"))),
                    ("_sqlload_source_file".to_owned(), Some(format!("{index}"))),
                ],
            })
            .collect(),
        in_files: vec![("id".to_owned(), in_files.to_owned())],
    };
    let test_cases = [
        PullQueriesTestCase {
            description: "a small batch is one query",
            groups: vec![group(3, 10, "id"), group(2, 10, "ID2")],
            expected_queries: 1,
        },
        PullQueriesTestCase {
            description: "long keys over two schema groups split by size, each file in exactly one query",
            groups: vec![group(1000, 500, "id"), group(1000, 500, "id2")],
            expected_queries: 4,
        },
        PullQueriesTestCase {
            description: "more files than one query takes split by count, even when short",
            groups: vec![group(1500, 1, "id")],
            expected_queries: 2,
        },
    ];
    let columns = [
        desired("id", ColumnType::Int),
        desired("day", ColumnType::Text),
        desired("_sqlload_source_file", ColumnType::Text),
    ];
    for test_case in test_cases {
        let queries = pull_queries(
            &orders(),
            (&columns, false),
            &test_case.groups,
            (Compat::Sqlload, "L1", "2026-11-06T00:00:00Z"),
        );
        let files: usize = test_case.groups.iter().map(|group| group.files.len()).sum();
        let urls: usize = queries
            .iter()
            .map(|(_, definitions)| definitions.to_string().matches("gs://b/").count())
            .sum();
        let largest = queries.iter().map(|(query, _)| query.len()).max();
        let most_files = queries
            .iter()
            .map(|(_, definitions)| definitions.to_string().matches("gs://b/").count())
            .max();
        assert_eq!(
            queries.len(),
            test_case.expected_queries,
            "{}",
            test_case.description
        );
        assert_eq!(urls, files, "{}", test_case.description);
        assert!(
            largest <= Some(MAX_QUERY_BYTES),
            "{}",
            test_case.description
        );
        assert!(
            most_files <= Some(MAX_FILES_PER_QUERY),
            "{}",
            test_case.description
        );
    }
}
