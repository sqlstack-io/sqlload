use arrow_schema::{DataType, Field, TimeUnit};

use crate::files_source::models::StoredRead;

use crate::files_source::_helpers::plan::columns::{direct_refusal, parquet_columns, stored_read};
use crate::files_source::tests::helpers::{footer_type, only_column, overridden_footer, timestamp};
use crate::files_source::tests::test_types::{
    DirectRefusalTestCase, FooterZoneTestCase, StoredColumnTestCase, StoredRefusalTestCase,
};

#[test]
fn given_a_files_arrow_type_when_choosing_the_route_then_only_exact_ones_go_direct() {
    let test_cases = [
        DirectRefusalTestCase {
            description: "Boolean",
            data_type: DataType::Boolean,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Int8",
            data_type: DataType::Int8,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Int16",
            data_type: DataType::Int16,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Int32",
            data_type: DataType::Int32,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Int64",
            data_type: DataType::Int64,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "UInt8",
            data_type: DataType::UInt8,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "UInt16",
            data_type: DataType::UInt16,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "UInt32",
            data_type: DataType::UInt32,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Float32",
            data_type: DataType::Float32,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Float64",
            data_type: DataType::Float64,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Decimal128 at precision 38",
            data_type: DataType::Decimal128(38, 10),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Utf8",
            data_type: DataType::Utf8,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "LargeUtf8",
            data_type: DataType::LargeUtf8,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Utf8View",
            data_type: DataType::Utf8View,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Date32",
            data_type: DataType::Date32,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "seconds, a bare integer in the file",
            data_type: timestamp(TimeUnit::Second, None),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "milliseconds",
            data_type: timestamp(TimeUnit::Millisecond, None),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "microseconds",
            data_type: timestamp(TimeUnit::Microsecond, None),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "microseconds in UTC",
            data_type: timestamp(TimeUnit::Microsecond, Some("UTC")),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "microseconds at +00:00",
            data_type: timestamp(TimeUnit::Microsecond, Some("+00:00")),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "nanoseconds, as INT96 reads too",
            data_type: timestamp(TimeUnit::Nanosecond, None),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "nanoseconds in UTC",
            data_type: timestamp(TimeUnit::Nanosecond, Some("UTC")),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "a zone other than UTC",
            data_type: timestamp(TimeUnit::Microsecond, Some("Europe/London")),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "an offset other than UTC",
            data_type: timestamp(TimeUnit::Microsecond, Some("+01:00")),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Date64",
            data_type: DataType::Date64,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "UInt64",
            data_type: DataType::UInt64,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Float16",
            data_type: DataType::Float16,
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "Decimal256",
            data_type: DataType::Decimal256(40, 0),
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "Binary",
            data_type: DataType::Binary,
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "Time64",
            data_type: DataType::Time64(TimeUnit::Microsecond),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "a list",
            data_type: DataType::new_list(DataType::Int64, true),
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "a dictionary",
            data_type: DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            expected_exact: true,
        },
        DirectRefusalTestCase {
            description: "a struct",
            data_type: DataType::Struct(vec![Field::new("a", DataType::Int64, true)].into()),
            expected_exact: false,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            direct_refusal(&test_case.data_type).is_none(),
            test_case.expected_exact,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_files_footer_when_reading_its_zone_then_the_direct_filter_sees_what_the_reader_infers()
-> Result<(), String> {
    let test_cases = [
        FooterZoneTestCase {
            description: "+00:00 microseconds with no embedded schema read as UTC",
            written: timestamp(TimeUnit::Microsecond, Some("+00:00")),
            embedded_schema: false,
            expected_read: timestamp(TimeUnit::Microsecond, Some("UTC")),
            expected_exact: true,
        },
        FooterZoneTestCase {
            description: "+00:00 milliseconds with no embedded schema read as UTC",
            written: timestamp(TimeUnit::Millisecond, Some("+00:00")),
            embedded_schema: false,
            expected_read: timestamp(TimeUnit::Millisecond, Some("UTC")),
            expected_exact: true,
        },
        FooterZoneTestCase {
            description: "seconds with no embedded schema read as plain integers: this Parquet writer stores them with no logical type",
            written: timestamp(TimeUnit::Second, Some("+00:00")),
            embedded_schema: false,
            expected_read: DataType::Int64,
            expected_exact: true,
        },
        FooterZoneTestCase {
            description: "+00:00 nanoseconds with no embedded schema read exactly",
            written: timestamp(TimeUnit::Nanosecond, Some("+00:00")),
            embedded_schema: false,
            expected_read: timestamp(TimeUnit::Nanosecond, Some("UTC")),
            expected_exact: true,
        },
        FooterZoneTestCase {
            description: "+01:00 with no embedded schema reads as UTC: Parquet stores only instants",
            written: timestamp(TimeUnit::Microsecond, Some("+01:00")),
            embedded_schema: false,
            expected_read: timestamp(TimeUnit::Microsecond, Some("UTC")),
            expected_exact: true,
        },
        FooterZoneTestCase {
            description: "no zone with no embedded schema reads as wall-clock",
            written: timestamp(TimeUnit::Microsecond, None),
            embedded_schema: false,
            expected_read: timestamp(TimeUnit::Microsecond, None),
            expected_exact: true,
        },
        FooterZoneTestCase {
            description: "+00:00 only reaches the filter as written through an embedded schema",
            written: timestamp(TimeUnit::Microsecond, Some("+00:00")),
            embedded_schema: true,
            expected_read: timestamp(TimeUnit::Microsecond, Some("+00:00")),
            expected_exact: true,
        },
        FooterZoneTestCase {
            description: "seconds through an embedded schema read exactly",
            written: timestamp(TimeUnit::Second, Some("+00:00")),
            embedded_schema: true,
            expected_read: timestamp(TimeUnit::Second, Some("+00:00")),
            expected_exact: true,
        },
        FooterZoneTestCase {
            description: "+01:00 through an embedded schema reads exactly",
            written: timestamp(TimeUnit::Microsecond, Some("+01:00")),
            embedded_schema: true,
            expected_read: timestamp(TimeUnit::Microsecond, Some("+01:00")),
            expected_exact: true,
        },
    ];
    for test_case in test_cases {
        let read = footer_type(&test_case.written, test_case.embedded_schema)?;
        assert_eq!(read, test_case.expected_read, "{}", test_case.description);
        assert_eq!(
            direct_refusal(&read).is_none(),
            test_case.expected_exact,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_an_embedded_schema_over_another_parquet_type_when_reading_the_footer_then_only_what_the_file_stores_goes_direct()
-> Result<(), String> {
    let micros = || timestamp(TimeUnit::Microsecond, None);
    let millis = || timestamp(TimeUnit::Millisecond, None);
    let test_cases = [
        StoredColumnTestCase {
            description: "INT96 under embedded microseconds, which SQLLoad reads cut to that unit, is refused",
            message: "message m { optional int96 at; }",
            embedded: Some(micros()),
            expected_read: micros(),
            expected: Err("stored as INT96"),
        },
        StoredColumnTestCase {
            description: "INT96 under embedded milliseconds, which SQLLoad reads cut to that unit, is refused",
            message: "message m { optional int96 at; }",
            embedded: Some(millis()),
            expected_read: millis(),
            expected: Err("stored as INT96"),
        },
        StoredColumnTestCase {
            description: "a bare INT64 under embedded milliseconds",
            message: "message m { optional int64 at; }",
            embedded: Some(millis()),
            expected_read: millis(),
            expected: Ok(StoredRead::Epoch(3)),
        },
        StoredColumnTestCase {
            description: "a bare INT64 under embedded microseconds",
            message: "message m { optional int64 at; }",
            embedded: Some(micros()),
            expected_read: micros(),
            expected: Ok(StoredRead::Epoch(6)),
        },
        StoredColumnTestCase {
            description: "a nanosecond column under embedded microseconds",
            message: "message m { optional int64 at (TIMESTAMP(NANOS,false)); }",
            embedded: Some(micros()),
            expected_read: micros(),
            expected: Ok(StoredRead::Nanos),
        },
        StoredColumnTestCase {
            description: "a timestamp with only the older converted type",
            message: "message m { optional int64 at (TIMESTAMP_MICROS); }",
            embedded: None,
            expected_read: timestamp(TimeUnit::Microsecond, Some("UTC")),
            expected: Err("stored as TIMESTAMP_MICROS"),
        },
        StoredColumnTestCase {
            description: "a normal microsecond column goes direct",
            message: "message m { optional int64 at (TIMESTAMP(MICROS,false)); }",
            embedded: None,
            expected_read: micros(),
            expected: Ok(StoredRead::Plain),
        },
        StoredColumnTestCase {
            description: "a normal millisecond column goes direct",
            message: "message m { optional int64 at (TIMESTAMP(MILLIS,false)); }",
            embedded: None,
            expected_read: millis(),
            expected: Ok(StoredRead::Plain),
        },
        StoredColumnTestCase {
            description: "a normal microsecond column under its own embedded type goes direct",
            message: "message m { optional int64 at (TIMESTAMP(MICROS,true)); }",
            embedded: Some(timestamp(TimeUnit::Microsecond, Some("UTC"))),
            expected_read: timestamp(TimeUnit::Microsecond, Some("UTC")),
            expected: Ok(StoredRead::Plain),
        },
        StoredColumnTestCase {
            description: "a normal date goes direct",
            message: "message m { optional int32 at (DATE); }",
            embedded: None,
            expected_read: DataType::Date32,
            expected: Ok(StoredRead::Plain),
        },
        StoredColumnTestCase {
            description: "a normal decimal goes direct",
            message: "message m { optional int64 at (DECIMAL(10,2)); }",
            embedded: None,
            expected_read: DataType::Decimal128(10, 2),
            expected: Ok(StoredRead::Plain),
        },
        StoredColumnTestCase {
            description: "a bare integer is just an integer",
            message: "message m { optional int64 at; }",
            embedded: None,
            expected_read: DataType::Int64,
            expected: Ok(StoredRead::Plain),
        },
    ];
    for test_case in test_cases {
        let footer = overridden_footer(test_case.message, test_case.embedded.as_ref())?;
        assert_eq!(
            footer.schema().field(0).data_type(),
            &test_case.expected_read,
            "{}",
            test_case.description
        );
        let columns = parquet_columns(&footer);
        let found = columns[0]
            .direct_refusal
            .as_deref()
            .map_or(Ok(columns[0].stored_read), Err);
        assert_eq!(
            found.map_err(|reason| reason.ends_with(test_case.expected.err().unwrap_or(reason))),
            test_case.expected.map_err(|_| true),
            "{}: {found:?}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_files_own_column_when_checking_it_then_each_type_needs_its_logical_type()
-> Result<(), String> {
    let test_cases = [
        StoredRefusalTestCase {
            description: "Date32 over a bare INT32",
            data_type: DataType::Date32,
            message: Some("message m { optional int32 at; }"),
            expected_read: Err("Date32 stored as INT32"),
        },
        StoredRefusalTestCase {
            description: "Date32 over its logical type",
            data_type: DataType::Date32,
            message: Some("message m { optional int32 at (DATE); }"),
            expected_read: Ok(StoredRead::Plain),
        },
        StoredRefusalTestCase {
            description: "Decimal128 over a bare INT64",
            data_type: DataType::Decimal128(10, 2),
            message: Some("message m { optional int64 at; }"),
            expected_read: Err("Decimal128(10, 2) stored as INT64"),
        },
        StoredRefusalTestCase {
            description: "Decimal128 over its logical type",
            data_type: DataType::Decimal128(10, 2),
            message: Some("message m { optional int64 at (DECIMAL(10,2)); }"),
            expected_read: Ok(StoredRead::Plain),
        },
        StoredRefusalTestCase {
            description: "a timestamp the file does not hold",
            data_type: timestamp(TimeUnit::Microsecond, None),
            message: None,
            expected_read: Err("Timestamp(µs) stored as nothing"),
        },
        StoredRefusalTestCase {
            description: "other types need nothing more",
            data_type: DataType::Float64,
            message: Some("message m { optional double at; }"),
            expected_read: Ok(StoredRead::Plain),
        },
    ];
    for test_case in test_cases {
        let column = only_column(test_case.message)?;
        assert_eq!(
            stored_read(&test_case.data_type, column.as_deref())
                .as_ref()
                .copied()
                .map_err(String::as_str),
            test_case.expected_read,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
