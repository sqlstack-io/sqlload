use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};

use crate::files_source::tests::helpers::footer_column_type;
use crate::files_source::tests::test_types::FooterColumnsTestCase;

#[test]
fn given_a_files_target_footer_when_reading_its_columns_then_a_list_an_array_holds_reads_as_that_array()
-> Result<(), String> {
    let list = |item: DataType| DataType::List(Arc::new(Field::new("item", item, true)));
    let test_cases = [
        FooterColumnsTestCase {
            description: "a list of integers reads as the array(int) that wrote it, so a second load still fits",
            written: list(DataType::Int64),
            expected_type: "array(int)",
        },
        FooterColumnsTestCase {
            description: "a list of decimals keeps its precision and scale",
            written: list(DataType::Decimal128(10, 2)),
            expected_type: "array(decimal(10,2))",
        },
        FooterColumnsTestCase {
            description: "a list of lists is no array, so it stays json",
            written: list(list(DataType::Int64)),
            expected_type: "json",
        },
        FooterColumnsTestCase {
            description: "a struct stays json",
            written: DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, true)])),
            expected_type: "json",
        },
        FooterColumnsTestCase {
            description: "a scalar column is unchanged",
            written: DataType::Int64,
            expected_type: "int",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            footer_column_type(&test_case.written)?,
            test_case.expected_type,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
