use crate::adapters::contract::models::{TableName, WriteTarget};
use crate::adapters::databricks::_helpers::write::unloaded_copy::{
    UnloadedGroup, unloaded_copy_statement,
};
use crate::adapters::databricks::tests::test_types::UnloadedCopyTestCase;
use crate::column_types::models::ColumnType;
use crate::load_naming::models::Compat;
use crate::postgres_source::models::{ReadColumn, WireType};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

#[test]
fn given_files_of_a_snowflake_unload_when_databricks_copies_them_then_timestamps_come_back_from_microseconds_and_merge_rows_count_by_file()
 {
    let test_cases = [
        UnloadedCopyTestCase {
            description: "into a staging table: timestamps from microseconds whatever the session's time zone, JSON as text, the merge row by batch, file and row",
            builds: false,
            expected_statement: "COPY INTO `main`.`raw`.`orders_stage` FROM (SELECT \
         CAST(`ID` AS BIGINT) AS `ID`, \
         CAST(timestamp_micros(CAST(`at` AS BIGINT)) AS TIMESTAMP) AS `at`, \
         CAST(timestampadd(MICROSECOND, CAST(`wall` AS BIGINT), TIMESTAMP_NTZ'1970-01-01 00:00:00') AS TIMESTAMP_NTZ) AS `wall`, \
         CAST(`payload` AS STRING) AS `payload`, \
         CAST('eu' AS STRING) AS `region`, \
         CAST(NULL AS STRING) AS `missing`, \
         CAST(3 * 281474976710656 + (CASE url_decode(replace(_metadata.file_path, '+', '%2B')) WHEN 's3://lake/unload/run1/data_0_1_0.snappy.parquet' THEN 1000 WHEN 's3://lake/unload/run1/data_0_2_0.snappy.parquet' THEN 1001 ELSE raise_error(concat('sqlload: COPY INTO read a file outside the batch: ', _metadata.file_path)) END) * 4294967296 + _metadata.row_index AS BIGINT) AS `_sqlload_row`, \
         CAST('L1' AS STRING) AS `_sqlload_load_id`, \
         CAST('2026-11-18T00:00:00Z' AS TIMESTAMP) AS `_sqlload_loaded_at` \
         FROM 's3://lake/unload/run1/') FILEFORMAT = PARQUET FILES = ('data_0_1_0.snappy.parquet', 'data_0_2_0.snappy.parquet') FORMAT_OPTIONS ('mergeSchema' = 'true') COPY_OPTIONS ('force' = 'true')",
        },
        UnloadedCopyTestCase {
            description: "into a build table: final types, JSON parsed",
            builds: true,
            expected_statement: "COPY INTO `main`.`raw`.`orders_stage` FROM (SELECT \
         CAST(`ID` AS BIGINT) AS `ID`, \
         CAST(timestamp_micros(CAST(`at` AS BIGINT)) AS TIMESTAMP) AS `at`, \
         CAST(timestampadd(MICROSECOND, CAST(`wall` AS BIGINT), TIMESTAMP_NTZ'1970-01-01 00:00:00') AS TIMESTAMP_NTZ) AS `wall`, \
         parse_json(`payload`) AS `payload`, \
         CAST('eu' AS STRING) AS `region`, \
         CAST(NULL AS STRING) AS `missing`, \
         CAST(3 * 281474976710656 + (CASE url_decode(replace(_metadata.file_path, '+', '%2B')) WHEN 's3://lake/unload/run1/data_0_1_0.snappy.parquet' THEN 1000 WHEN 's3://lake/unload/run1/data_0_2_0.snappy.parquet' THEN 1001 ELSE raise_error(concat('sqlload: COPY INTO read a file outside the batch: ', _metadata.file_path)) END) * 4294967296 + _metadata.row_index AS BIGINT) AS `_sqlload_row`, \
         CAST('L1' AS STRING) AS `_sqlload_load_id`, \
         CAST('2026-11-18T00:00:00Z' AS TIMESTAMP) AS `_sqlload_loaded_at` \
         FROM 's3://lake/unload/run1/') FILEFORMAT = PARQUET FILES = ('data_0_1_0.snappy.parquet', 'data_0_2_0.snappy.parquet') FORMAT_OPTIONS ('mergeSchema' = 'true') COPY_OPTIONS ('force' = 'true')",
        },
    ];
    let column = |name: &str, column_type: ColumnType| DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    };
    let read: Vec<ReadColumn> = [
        ("ID", ColumnType::Int),
        ("at", ColumnType::TimestampTz),
        ("wall", ColumnType::TimestampNtz),
        ("payload", ColumnType::Json),
    ]
    .into_iter()
    .map(|(name, output)| ReadColumn {
        name: name.to_owned(),
        expression: format!("\"{name}\""),
        wire: WireType::Text,
        output,
    })
    .collect();
    let columns = [
        column("ID", ColumnType::Int),
        column("at", ColumnType::TimestampTz),
        column("wall", ColumnType::TimestampNtz),
        column("payload", ColumnType::Json),
        column("region", ColumnType::Text),
        column("missing", ColumnType::Text),
        column("_sqlload_row", ColumnType::Int),
        column("_sqlload_load_id", ColumnType::Text),
        column("_sqlload_loaded_at", ColumnType::TimestampTz),
    ];
    let table = TableName {
        database: Some("main".to_owned()),
        schema: "raw".to_owned(),
        table: "orders".to_owned(),
    };
    let staging = TableName {
        table: "orders_stage".to_owned(),
        ..table.clone()
    };
    let constants = [("region".to_owned(), ColumnType::Text, "eu".to_owned())];
    for test_case in &test_cases {
        let target = WriteTarget {
            table: &table,
            staging: Some(&staging),
            columns: &columns,
            write_columns: &columns,
            compat: Compat::Sqlload,
            load_id: "L1",
            batch: 3,
            direct: false,
            builds: test_case.builds,
            constants: &constants,
        };
        let keys = [
            "data_0_1_0.snappy.parquet".to_owned(),
            "data_0_2_0.snappy.parquet".to_owned(),
        ];
        let statement = unloaded_copy_statement(
            ("`main`.`raw`.`orders_stage`", &columns),
            &target,
            (
                &read,
                &UnloadedGroup {
                    folder: "s3://lake/unload/run1/",
                    keys: &keys,
                    first: 1000,
                },
            ),
            "2026-11-18T00:00:00Z",
        );
        assert_eq!(
            statement, test_case.expected_statement,
            "{}",
            test_case.description
        );
    }
}
