use crate::adapters::databricks::tests::helpers::s3_copy_sql;
use crate::adapters::databricks::tests::test_types::RenderedTestCase;

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
