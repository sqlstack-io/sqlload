use arrow_schema::{DataType, Field, TimeUnit};

use crate::files_source::_helpers::plan::columns::direct_refusal;
use crate::files_source::tests::helpers::timestamp;
use crate::files_source::tests::test_types::DirectRefusalTestCase;

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
            description: "seconds",
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
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "nanoseconds in UTC",
            data_type: timestamp(TimeUnit::Nanosecond, Some("UTC")),
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "a zone other than UTC",
            data_type: timestamp(TimeUnit::Microsecond, Some("Europe/London")),
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "an offset other than UTC",
            data_type: timestamp(TimeUnit::Microsecond, Some("+01:00")),
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "Date64",
            data_type: DataType::Date64,
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "UInt64",
            data_type: DataType::UInt64,
            expected_exact: false,
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
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "Time64",
            data_type: DataType::Time64(TimeUnit::Microsecond),
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "a list",
            data_type: DataType::new_list(DataType::Int64, true),
            expected_exact: false,
        },
        DirectRefusalTestCase {
            description: "a dictionary",
            data_type: DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            expected_exact: false,
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
