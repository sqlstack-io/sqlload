use arrow_schema::{DataType, TimeUnit};

use crate::config::models::FileFormat;
use crate::envelope::main::encode_row::encode_row;
use crate::envelope::models::{KafkaRecord, RecordHeader, RowContext};
use crate::file_format::main::encode_file::encode_file;
use crate::file_format::tests::helpers::{
    first_date, first_int32, first_int64, first_text, first_timestamp, parquet_config, read_parquet,
};
use crate::file_format::tests::test_types::ParquetColumnTestCase;

const COLUMNS: &str = "[envelope]\ntime_zone = \"America/Los_Angeles\"\n[[envelope.columns]]\nname = \"TOPIC\"\nfield = \"topic\"\ntype = \"VARCHAR\"\n[[envelope.columns]]\nname = \"KAFKA_PARTITION\"\nfield = \"partition\"\ntype = \"NUMBER(38,0)\"\n[[envelope.columns]]\nname = \"KAFKA_OFFSET\"\nfield = \"offset\"\ntype = \"NUMBER(38,0)\"\n[[envelope.columns]]\nname = \"KAFKA_TIMESTAMP\"\nfield = \"timestamp\"\ntype = \"NUMBER(38,0)\"\n[[envelope.columns]]\nname = \"KEY\"\nfield = \"key\"\ntype = \"VARCHAR\"\n[[envelope.columns]]\nname = \"VALUE\"\nfield = \"value\"\ntype = \"VARCHAR\"\n[[envelope.columns]]\nname = \"PAYLOAD\"\nfield = \"value\"\ntype = \"VARIANT\"\n[[envelope.columns]]\nname = \"HEADERS\"\nfield = \"headers\"\ntype = \"VARIANT\"\n[[envelope.columns]]\nname = \"EXPORTED_AT\"\nfield = \"exported_at\"\ntype = \"TIMESTAMP_TZ\"\n[[envelope.columns]]\nname = \"DATE\"\nfield = \"date\"\ntype = \"DATE\"\n";

/// 2026-11-03T04:23:01.930Z, which is 20:23:01.930 on 2 November in Los Angeles.
const EXPORTED_AT_MS: i64 = 1_793_679_781_930;
const KAFKA_MS: i64 = 1_793_679_780_000;

#[test]
fn given_encoded_rows_when_writing_parquet_then_each_column_has_its_type_and_value()
-> Result<(), String> {
    let config = parquet_config(COLUMNS)?;
    let zone: chrono_tz::Tz = "America/Los_Angeles".parse().map_err(|_| "zone")?;
    let unwrap = std::collections::BTreeMap::new();
    let context = RowContext {
        exported_at_ms: EXPORTED_AT_MS,
        time_zone: zone,
        unwrap: &unwrap,
    };
    let records = [
        KafkaRecord {
            topic: "shop.orders".to_owned(),
            partition: 3,
            offset: 42,
            key: Some(b"k1".to_vec()),
            timestamp_ms: Some(KAFKA_MS),
            timestamp_type: Some(0),
            value: Some(br#"{"id":1,"price":12345678901234567890.5}"#.to_vec()),
            headers: vec![RecordHeader {
                name: "source".to_owned(),
                value: Some(b"web".to_vec()),
            }],
        },
        KafkaRecord {
            topic: "shop.orders".to_owned(),
            partition: 3,
            offset: 43,
            key: None,
            timestamp_ms: Some(KAFKA_MS),
            timestamp_type: Some(0),
            value: None,
            headers: Vec::new(),
        },
    ];
    let lines: Vec<String> = records
        .iter()
        .map(|record| encode_row(&config.envelope.columns, record, &context))
        .collect();
    let line_refs: Vec<&str> = lines.iter().map(String::as_str).collect();
    let (body, plain) = encode_file(FileFormat::Parquet, &config.envelope.columns, &line_refs)?;
    assert!(body.starts_with(b"PAR1"), "a Parquet file");
    assert_eq!(
        plain,
        lines.iter().map(|line| line.len() as u64 + 1).sum::<u64>()
    );
    let batch = read_parquet(&body)?;
    assert_eq!(batch.num_rows(), 2);
    let test_cases = [
        ParquetColumnTestCase {
            description: "partitions are 32-bit integers",
            column: "KAFKA_PARTITION",
            expected_type: DataType::Int32,
            read: first_int32,
            expected_value: "3",
        },
        ParquetColumnTestCase {
            description: "offsets are 64-bit integers",
            column: "KAFKA_OFFSET",
            expected_type: DataType::Int64,
            read: first_int64,
            expected_value: "42",
        },
        ParquetColumnTestCase {
            description: "Kafka timestamps stay epoch milliseconds",
            column: "KAFKA_TIMESTAMP",
            expected_type: DataType::Int64,
            read: first_int64,
            expected_value: "1793679780000",
        },
        ParquetColumnTestCase {
            description: "exported_at is a UTC instant whatever envelope.time_zone is",
            column: "EXPORTED_AT",
            expected_type: DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            read: first_timestamp,
            expected_value: "1793679781930",
        },
        ParquetColumnTestCase {
            description: "dates are days since the epoch",
            column: "DATE",
            expected_type: DataType::Date32,
            read: first_date,
            expected_value: "20760",
        },
        ParquetColumnTestCase {
            description: "a text value keeps its exact text",
            column: "VALUE",
            expected_type: DataType::Utf8,
            read: first_text,
            expected_value: r#"{"id":1,"price":12345678901234567890.5}"#,
        },
        ParquetColumnTestCase {
            description: "a semi-structured value is JSON text with numbers intact",
            column: "PAYLOAD",
            expected_type: DataType::Utf8,
            read: first_text,
            expected_value: r#"{"id":1,"price":12345678901234567890.5}"#,
        },
        ParquetColumnTestCase {
            description: "headers are a JSON object",
            column: "HEADERS",
            expected_type: DataType::Utf8,
            read: first_text,
            expected_value: r#"{"source":"web"}"#,
        },
        ParquetColumnTestCase {
            description: "the topic is text",
            column: "TOPIC",
            expected_type: DataType::Utf8,
            read: first_text,
            expected_value: "shop.orders",
        },
    ];
    for test_case in &test_cases {
        let array = batch
            .column_by_name(test_case.column)
            .ok_or(test_case.description)?;
        assert_eq!(
            array.data_type(),
            &test_case.expected_type,
            "{}",
            test_case.description
        );
        assert_eq!(
            (test_case.read)(array.as_ref()),
            test_case.expected_value,
            "{}",
            test_case.description
        );
    }
    let missing = ["KEY", "VALUE", "PAYLOAD"]
        .iter()
        .filter_map(|name| batch.column_by_name(name))
        .filter(|array| array.is_null(1))
        .count();
    assert_eq!(missing, 3, "a missing key and value are nulls");
    Ok(())
}
