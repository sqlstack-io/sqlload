use crate::adapters::databricks::_helpers::stage::locations::location_url;
use crate::adapters::databricks::_helpers::write::s3_copy::copy_groups;
use crate::adapters::databricks::tests::helpers::s3_copy_sql;
use crate::adapters::databricks::tests::test_types::{
    CopyGroupsTestCase, LocationUrlTestCase, RenderedTestCase,
};
use arrow_schema::{DataType, Field, Schema};

#[test]
fn given_a_batch_of_parquet_files_in_s3_when_databricks_copies_them_then_each_row_gets_its_files_values_and_the_files_own_columns()
 {
    let test_cases = [
        RenderedTestCase {
            description: "straight into the table: final types, the renamed column read by its name in the files, a column no file holds as NULL, per-file values picked by the decoded path",
            rendered: s3_copy_sql(false),
            expected_statement: "COPY INTO `main`.`raw`.`orders` FROM (SELECT CAST(`ID` AS BIGINT) AS `id`, CAST(CASE url_decode(replace(_metadata.file_path, '+', '%2B')) WHEN 's3://lake/orders/day=1/a b.parquet' THEN '2026-11-17' WHEN 's3://lake/orders/o\\'brien+x.parquet' THEN NULL ELSE raise_error(concat('sqlload: COPY INTO read a file outside the batch: ', _metadata.file_path)) END AS DATE) AS `day`, CAST(CASE url_decode(replace(_metadata.file_path, '+', '%2B')) WHEN 's3://lake/orders/day=1/a b.parquet' THEN 's3://lake/orders/day=1/a b.parquet' WHEN 's3://lake/orders/o\\'brien+x.parquet' THEN 's3://lake/orders/o\\'brien+x.parquet' ELSE raise_error(concat('sqlload: COPY INTO read a file outside the batch: ', _metadata.file_path)) END AS STRING) AS `_sqlload_source_file`, CAST(_metadata.row_index + 1 AS BIGINT) AS `_sqlload_source_row`, CAST(NULL AS STRING) AS `gone`, CAST('L1' AS STRING) AS `_sqlload_load_id`, CAST('2026-11-18T00:00:00Z' AS TIMESTAMP) AS `_sqlload_loaded_at` FROM 's3://lake/orders/') FILEFORMAT = PARQUET FILES = ('day=1/a b.parquet', 'o\\'brien+x.parquet') FORMAT_OPTIONS ('mergeSchema' = 'true') COPY_OPTIONS ('force' = 'true')",
        },
        RenderedTestCase {
            description: "into the staging table: JSON stays text there, parsed only at publish",
            rendered: s3_copy_sql(true),
            expected_statement: "COPY INTO `main`.`raw`.`orders_stage` FROM (SELECT CAST(`ID` AS BIGINT) AS `id`, CAST(CASE url_decode(replace(_metadata.file_path, '+', '%2B')) WHEN 's3://lake/orders/day=1/a b.parquet' THEN '2026-11-17' WHEN 's3://lake/orders/o\\'brien+x.parquet' THEN NULL ELSE raise_error(concat('sqlload: COPY INTO read a file outside the batch: ', _metadata.file_path)) END AS DATE) AS `day`, CAST(CASE url_decode(replace(_metadata.file_path, '+', '%2B')) WHEN 's3://lake/orders/day=1/a b.parquet' THEN 's3://lake/orders/day=1/a b.parquet' WHEN 's3://lake/orders/o\\'brien+x.parquet' THEN 's3://lake/orders/o\\'brien+x.parquet' ELSE raise_error(concat('sqlload: COPY INTO read a file outside the batch: ', _metadata.file_path)) END AS STRING) AS `_sqlload_source_file`, CAST(_metadata.row_index + 1 AS BIGINT) AS `_sqlload_source_row`, CAST(`Payload` AS STRING) AS `payload`, CAST('L1' AS STRING) AS `_sqlload_load_id`, CAST('2026-11-18T00:00:00Z' AS TIMESTAMP) AS `_sqlload_loaded_at` FROM 's3://lake/orders/') FILEFORMAT = PARQUET FILES = ('day=1/a b.parquet', 'o\\'brien+x.parquet') FORMAT_OPTIONS ('mergeSchema' = 'true') COPY_OPTIONS ('force' = 'true')",
        },
    ];
    for test_case in &test_cases {
        assert_eq!(
            test_case.rendered, test_case.expected_statement,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_batchs_parquet_files_when_databricks_copies_them_then_each_copy_holds_files_whose_shared_columns_agree_and_no_more_than_files_takes()
 {
    let test_cases = [
        CopyGroupsTestCase {
            description: "files that agree, or hold different columns, share one copy",
            files: vec![
                ("a", vec![("id", DataType::Int64)]),
                ("b", vec![("id", DataType::Int64), ("name", DataType::Utf8)]),
                ("c", vec![("name", DataType::Utf8)]),
            ],
            limit: 1000,
            expected_groups: vec![vec!["a", "b", "c"]],
        },
        CopyGroupsTestCase {
            description: "an INT32 and an INT64 of one column go to separate copies, and a later file joins the first copy it agrees with",
            files: vec![
                ("a", vec![("id", DataType::Int32)]),
                ("b", vec![("id", DataType::Int64)]),
                ("c", vec![("id", DataType::Int32), ("x", DataType::Utf8)]),
                ("d", vec![("x", DataType::Int64)]),
            ],
            limit: 1000,
            expected_groups: vec![vec!["a", "c"], vec!["b", "d"]],
        },
        CopyGroupsTestCase {
            description: "one column named in different cases is one column to Databricks, so different types still split",
            files: vec![
                ("a", vec![("ID", DataType::Int32)]),
                ("b", vec![("id", DataType::Int64)]),
            ],
            limit: 1000,
            expected_groups: vec![vec!["a"], vec!["b"]],
        },
        CopyGroupsTestCase {
            description: "no copy holds more files than the limit",
            files: vec![
                ("a", vec![("id", DataType::Int64)]),
                ("b", vec![("id", DataType::Int64)]),
                ("c", vec![("id", DataType::Int64)]),
            ],
            limit: 2,
            expected_groups: vec![vec!["a", "b"], vec!["c"]],
        },
    ];
    for test_case in test_cases {
        let schemas: Vec<(&str, Schema)> = test_case
            .files
            .iter()
            .map(|(name, columns)| {
                let fields: Vec<Field> = columns
                    .iter()
                    .map(|(column, data_type)| Field::new(*column, data_type.clone(), true))
                    .collect();
                (*name, Schema::new(fields))
            })
            .collect();
        let groups = copy_groups(
            schemas
                .iter()
                .map(|(name, schema)| (*name, schema))
                .collect(),
            test_case.limit,
        );
        assert_eq!(
            groups, test_case.expected_groups,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_an_external_locations_url_when_comparing_folders_with_it_then_it_is_an_s3_folder() {
    let test_cases = [
        LocationUrlTestCase {
            description: "an s3:// URL without a trailing slash",
            url: "s3://lake/raw",
            expected_url: "s3://lake/raw/",
        },
        LocationUrlTestCase {
            description: "an s3a:// URL",
            url: "s3a://lake/raw/",
            expected_url: "s3://lake/raw/",
        },
        LocationUrlTestCase {
            description: "an s3n:// URL of a whole bucket",
            url: "s3n://lake",
            expected_url: "s3://lake/",
        },
        LocationUrlTestCase {
            description: "an Azure URL stays as it is",
            url: "abfss://box@account.dfs.core.windows.net/raw",
            expected_url: "abfss://box@account.dfs.core.windows.net/raw/",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            location_url(test_case.url),
            test_case.expected_url,
            "{}",
            test_case.description
        );
    }
}
