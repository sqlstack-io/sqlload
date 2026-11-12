use crate::column_types::models::ColumnType;
use crate::loader::tests::helpers::constants_added;
use crate::loader::tests::test_types::ConstantsAddedTestCase;

#[test]
fn given_constant_columns_when_adding_them_to_a_batch_then_each_row_gets_its_typed_value_and_none_leave_it_equal()
 {
    let test_cases = [
        ConstantsAddedTestCase {
            description: "no constants leave the batch equal, its schema metadata included",
            constants: &[],
            expected: Ok((true, "id:Int64=1,2")),
        },
        ConstantsAddedTestCase {
            description: "each constant follows the batch's columns, typed as declared",
            constants: &[
                ("tenant_id", ColumnType::Text, "a"),
                ("shard", ColumnType::Int, "7"),
                ("as_of", ColumnType::Date, "2026-11-08"),
                ("live", ColumnType::Bool, "true"),
            ],
            expected: Ok((
                false,
                "id:Int64=1,2 tenant_id:Utf8=a,a shard:Int64=7,7 as_of:Date32=2026-11-08,2026-11-08 live:Boolean=true,true",
            )),
        },
        ConstantsAddedTestCase {
            description: "a value that does not convert to its type fails rather than load NULL",
            constants: &[("shard", ColumnType::Int, "seven")],
            expected: Err(
                "constant column shard: \"seven\" does not convert to int: Cast error: Cannot cast string 'seven' to value of Int64 type",
            ),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            constants_added(test_case.constants)
                .as_ref()
                .map(|(equal, columns)| (*equal, columns.as_str()))
                .map_err(String::as_str),
            test_case.expected,
            "{}",
            test_case.description
        );
    }
}
