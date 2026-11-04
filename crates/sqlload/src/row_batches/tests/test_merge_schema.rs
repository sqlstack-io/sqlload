use crate::row_batches::tests::helpers::merged_table;
use crate::row_batches::tests::test_types::MergeSchemaTestCase;

#[test]
fn given_batches_with_different_types_when_merging_then_types_widen_conservatively() {
    let test_cases = [
        MergeSchemaTestCase {
            description: "int and float make float; new columns are added at the end and NULL before they appear",
            batches: &["{\"a\":1}", "{\"a\":2.5,\"b\":true}"],
            expected_table: "a:Float64,b:Boolean / 1.0|NULL; 2.5|true",
        },
        MergeSchemaTestCase {
            description: "a column NULL in one batch takes the other's type; one NULL everywhere is text",
            batches: &["{\"a\":null,\"c\":null}", "{\"a\":\"x\",\"c\":null}"],
            expected_table: "a:Utf8,c:Utf8 / NULL|NULL; x|NULL",
        },
        MergeSchemaTestCase {
            description: "json and text make text; int and bool make text",
            batches: &["{\"j\":{\"k\":1},\"n\":1}", "{\"j\":\"plain\",\"n\":false}"],
            expected_table: "j:Utf8,n:Utf8 / {\"k\":1}|1; plain|false",
        },
        MergeSchemaTestCase {
            description: "json in every batch stays json",
            batches: &["{\"j\":[1]}", "{\"j\":{\"k\":null}}"],
            expected_table: "j:Utf8(json) / [1]; {\"k\":null}",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            merged_table(test_case.batches),
            Ok(test_case.expected_table.to_owned()),
            "{}",
            test_case.description
        );
    }
}
