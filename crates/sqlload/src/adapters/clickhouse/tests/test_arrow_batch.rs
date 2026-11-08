use std::sync::Arc;

use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{
    ArrayRef, Decimal128Array, DictionaryArray, Int64Array, LargeBinaryArray, ListArray,
    RecordBatch, StringViewArray, Time64MicrosecondArray, TimestampMillisecondArray,
};

use crate::adapters::clickhouse::_helpers::arrow_batch::clickhouse_batch;
use crate::adapters::clickhouse::tests::test_types::ArrowBatchTestCase;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

fn desired(name: &str, column_type: ColumnType) -> DesiredColumn {
    DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    }
}

fn batch_of(columns: Vec<(&str, ArrayRef)>) -> Result<RecordBatch, String> {
    RecordBatch::try_from_iter(columns).map_err(|error| error.to_string())
}

/// Each column of `batch` as `name: type = value|value`, NULLs as `null`, columns joined by `; `.
fn shown(batch: &RecordBatch) -> Result<String, String> {
    let options = arrow_cast::display::FormatOptions::default().with_null("null");
    let mut shown: Vec<String> = Vec::new();
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let formatter = arrow_cast::display::ArrayFormatter::try_new(array, &options)
            .map_err(|error| error.to_string())?;
        let values: Vec<String> = (0..array.len())
            .map(|row| formatter.value(row).to_string())
            .collect();
        shown.push(format!(
            "{}: {} = {}",
            field.name(),
            field.data_type(),
            values.join("|")
        ));
    }
    Ok(shown.join("; "))
}

#[test]
fn given_planned_columns_when_preparing_rows_for_clickhouse_then_each_takes_an_arrow_type_its_reader_takes()
-> Result<(), String> {
    let test_cases = [
        ArrowBatchTestCase {
            builds: false,
            description: "a time goes as its text, since the column is a String; ClickHouse would read a time as a date",
            columns: vec![desired("at", ColumnType::Time)],
            batch: batch_of(vec![(
                "at",
                Arc::new(Time64MicrosecondArray::from(vec![
                    Some(47_045_123_456),
                    None,
                ])),
            )])?,
            expected_batch: Ok("at: Utf8 = 13:04:05.123456|null"),
        },
        ArrowBatchTestCase {
            builds: false,
            description: "view, dictionary and large encodings go as plain ones, which ClickHouse's Arrow reader takes",
            columns: vec![
                desired("name", ColumnType::Text),
                desired(
                    "code",
                    ColumnType::Native("LowCardinality(String)".to_owned()),
                ),
                desired("blob", ColumnType::Binary),
            ],
            batch: batch_of(vec![
                ("name", Arc::new(StringViewArray::from(vec!["a"]))),
                (
                    "code",
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(["x"])),
                ),
                (
                    "blob",
                    Arc::new(LargeBinaryArray::from(vec![b"\x00\xff".as_slice()])),
                ),
            ])?,
            expected_batch: Ok("name: Utf8 = a; code: Utf8 = x; blob: Binary = 00ff"),
        },
        ArrowBatchTestCase {
            builds: false,
            description: "a timestamp with an offset keeps its instant in UTC microseconds",
            columns: vec![desired("at", ColumnType::TimestampTz)],
            batch: batch_of(vec![(
                "at",
                Arc::new(TimestampMillisecondArray::from(vec![3_600_000]).with_timezone("+01:00")),
            )])?,
            expected_batch: Ok("at: Timestamp(µs, \"UTC\") = 1970-01-01T01:00:00Z"),
        },
        ArrowBatchTestCase {
            builds: false,
            description: "a decimal too wide for its column fails by name rather than at the server",
            columns: vec![desired(
                "amount",
                ColumnType::Decimal {
                    precision: 4,
                    scale: 2,
                },
            )],
            batch: batch_of(vec![(
                "amount",
                Arc::new(
                    Decimal128Array::from(vec![12_345])
                        .with_precision_and_scale(10, 1)
                        .map_err(|error| error.to_string())?,
                ),
            )])?,
            expected_batch: Err("column amount cannot be sent to ClickHouse as Decimal128(4, 2)"),
        },
        ArrowBatchTestCase {
            builds: false,
            description: "a nested value is refused for a text column, which a cast would fill with display text",
            columns: vec![desired("tags", ColumnType::Text)],
            batch: batch_of(vec![(
                "tags",
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                    Some(vec![Some(1)]),
                ])),
            )])?,
            expected_batch: Err("column tags (List(Int64)) cannot load into ClickHouse"),
        },
        ArrowBatchTestCase {
            builds: true,
            description: "a field the columns leave out is not sent to a build table",
            columns: vec![desired("id", ColumnType::Int)],
            batch: batch_of(vec![
                ("id", Arc::new(Int64Array::from(vec![7]))),
                ("ignored", Arc::new(Int64Array::from(vec![8]))),
            ])?,
            expected_batch: Ok("id: Int64 = 7"),
        },
        ArrowBatchTestCase {
            builds: false,
            description: "a field the columns leave out fails a staging table's batch rather than going unloaded",
            columns: vec![desired("id", ColumnType::Int)],
            batch: batch_of(vec![
                ("id", Arc::new(Int64Array::from(vec![7]))),
                ("ignored", Arc::new(Int64Array::from(vec![8]))),
            ])?,
            expected_batch: Err("column ignored is not among the table's planned columns"),
        },
    ];
    for test_case in test_cases {
        let converted = clickhouse_batch(&test_case.batch, (&test_case.columns, test_case.builds));
        let outcome = match converted {
            Ok(batch) => Ok(shown(&batch)?),
            Err(error) => Err(error.split(": ").next().unwrap_or_default().to_owned()),
        };
        let expected = test_case
            .expected_batch
            .map(str::to_owned)
            .map_err(str::to_owned);
        assert_eq!(outcome, expected, "{}", test_case.description);
    }
    Ok(())
}
