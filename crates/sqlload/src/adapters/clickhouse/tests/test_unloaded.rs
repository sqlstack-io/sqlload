use crate::adapters::clickhouse::_helpers::dialect::pull::PullCredentials;
use crate::adapters::clickhouse::_helpers::dialect::unloaded::{UnloadedFile, unloaded_insert};
use crate::adapters::clickhouse::tests::helpers::desired;
use crate::column_types::models::ColumnType;
use crate::load_naming::models::Compat;
use crate::postgres_source::models::{ReadColumn, WireType};

#[test]
fn given_a_snowflake_unload_when_clickhouse_reads_one_of_its_files_then_timestamps_come_back_from_microseconds_and_the_rest_as_unloaded()
 {
    let read: Vec<ReadColumn> = [
        ("id", ColumnType::Int),
        ("at", ColumnType::TimestampTz),
        ("wall", ColumnType::TimestampNtz),
        ("blob", ColumnType::Binary),
        ("day", ColumnType::Date),
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
        desired("id", ColumnType::Int),
        desired("at", ColumnType::TimestampTz),
        desired("wall", ColumnType::TimestampNtz),
        desired("blob", ColumnType::Binary),
        desired("day", ColumnType::Date),
        desired("region", ColumnType::Text),
        desired("missing", ColumnType::Text),
        desired("_sqlload_row", ColumnType::Int),
        desired("_sqlload_load_id", ColumnType::Text),
        desired("_sqlload_loaded_at", ColumnType::TimestampTz),
    ];
    let constants = [("region".to_owned(), ColumnType::Text, "eu".to_owned())];
    let credentials = PullCredentials {
        key_id: "AKIDEXAMPLE".to_owned(),
        secret: "secret".to_owned(),
        token: None,
    };
    let file = UnloadedFile {
        url: "https://unload.s3.eu-west-2.amazonaws.com/run/data_0_1_0.snappy.parquet",
        index: 2,
    };
    let insert = unloaded_insert(
        "`raw`.`orders__staging`",
        (&columns, &|_: &ColumnType| "T".to_owned()),
        (&read, &constants),
        (&file, Some(&credentials)),
        (Compat::Sqlload, 3, "L1", "2026-11-18T00:00:00Z"),
    );
    assert_eq!(
        insert,
        "INSERT INTO `raw`.`orders__staging` (`id`, `at`, `wall`, `blob`, `day`, `region`, `missing`, `_sqlload_row`, `_sqlload_load_id`, `_sqlload_loaded_at`) SELECT \
         CAST(`id` AS T), \
         CAST(fromUnixTimestamp64Micro(CAST(`at` AS Int64), 'UTC') AS T), \
         CAST(fromUnixTimestamp64Micro(CAST(`wall` AS Int64), 'UTC') AS T), \
         CAST(substring(`blob`, 1) AS T), \
         CAST(`day` AS T), \
         CAST(CAST('eu' AS String) AS T), \
         CAST(NULL AS T), \
         CAST(3 * 281474976710656 + 2 * 4294967296 + rowNumberInAllBlocks() AS T), \
         CAST(CAST('L1' AS String) AS T), \
         CAST(parseDateTime64BestEffort('2026-11-18T00:00:00Z', 6, 'UTC') AS T) \
         FROM s3('https://unload.s3.eu-west-2.amazonaws.com/run/data_0_1_0.snappy.parquet', {sqlload_key_id:String}, {sqlload_secret:String}, 'Parquet') \
         SETTINGS max_threads = 1, max_insert_threads = 1, input_format_parquet_preserve_order = 1"
    );
}
