use arrow_array::RecordBatch;
use arrow_cast::display::array_value_to_string;

use crate::bigquery_source::_helpers::convert::{convert_batch, output_schema};
use crate::bigquery_source::tests::helpers::{floats, numeric, record};
use crate::bigquery_source::tests::test_types::ConvertBatchTestCase;
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{ReadColumn, WireType};

#[test]
fn given_read_api_arrow_when_converting_then_records_become_json_and_pins_convert_exactly() {
    let test_cases = [
        ConvertBatchTestCase {
            description: "a record loads as its JSON",
            read: record,
            output: ColumnType::Json,
            expected_value: Some(r#"{"a":1,"s":"x"}"#),
        },
        ConvertBatchTestCase {
            description: "an unpinned repeated column loads as JSON, NaN as null",
            read: floats,
            output: ColumnType::Json,
            expected_value: Some("[null,1.5]"),
        },
        ConvertBatchTestCase {
            description: "a pinned array stays a list, so NaN survives",
            read: floats,
            output: ColumnType::Array(Box::new(ColumnType::Float)),
            expected_value: Some("[NaN, 1.5]"),
        },
        ConvertBatchTestCase {
            description: "NUMERIC narrows exactly to the column's scale",
            read: numeric,
            output: ColumnType::Decimal {
                precision: 12,
                scale: 2,
            },
            expected_value: Some("1.25"),
        },
        ConvertBatchTestCase {
            description: "a value too wide for the pinned decimal fails rather than loading as NULL",
            read: numeric,
            output: ColumnType::Decimal {
                precision: 2,
                scale: 2,
            },
            expected_value: None,
        },
    ];
    for test_case in test_cases {
        let columns = [ReadColumn {
            name: "c".to_owned(),
            expression: "`c`".to_owned(),
            wire: WireType::Text,
            output: test_case.output.clone(),
        }];
        let (schema, json) = output_schema(&columns).expect("a schema");
        let batch = RecordBatch::try_from_iter([("c", (test_case.read)())]).expect("a read batch");
        let value = convert_batch(&batch, (&schema, &json))
            .ok()
            .map(|converted| array_value_to_string(converted.column(0), 0).expect("a value"));
        assert_eq!(
            value.as_deref(),
            test_case.expected_value,
            "{}",
            test_case.description
        );
    }
}
