use crate::row_batches::tests::helpers::flattened;
use crate::row_batches::tests::test_types::FlattenBatchTestCase;

#[test]
fn given_listed_objects_when_flattening_then_one_level_becomes_typed_columns_and_the_rest_stays_json()
 {
    let test_cases = [
        FlattenBatchTestCase {
            description: "the ticket's example: a listed object expands one level where it stands, nested values and unlisted maps stay JSON",
            rows: "{\"order_id\":7,\"customer\":{\"country\":\"GB\",\"address\":{\"city\":\"Leeds\"}},\"line_totals\":{\"sku-14\":12.5}}\n",
            flatten: &["customer"],
            expected: Ok(
                "order_id:Int64,customer__country:Utf8,customer__address:Utf8(json),line_totals:Utf8(json) / 7|GB|{\"city\":\"Leeds\"}|{\"sku-14\":12.5}",
            ),
        },
        FlattenBatchTestCase {
            description: "keys are typed across rows; a NULL, missing or empty object and a missing key are NULL; a key only later rows have is added",
            rows: "{\"id\":1,\"c\":{\"n\":1,\"ok\":true}}\n{\"id\":2,\"c\":null}\n{\"id\":3}\n{\"id\":4,\"c\":{}}\n{\"id\":5,\"c\":{\"n\":2.5,\"tier\":\"gold\"}}\n",
            flatten: &["c"],
            expected: Ok(
                "id:Int64,c__n:Float64,c__ok:Boolean,c__tier:Utf8 / 1|1.0|true|NULL; 2|NULL|NULL|NULL; 3|NULL|NULL|NULL; 4|NULL|NULL|NULL; 5|2.5|NULL|gold",
            ),
        },
        FlattenBatchTestCase {
            description: "a listed object that only ever held NULL adds no columns",
            rows: "{\"id\":1,\"c\":null}\n",
            flatten: &["c"],
            expected: Ok("id:Int64 / 1"),
        },
        FlattenBatchTestCase {
            description: "a listed name the data does not have changes nothing",
            rows: "{\"id\":1}\n",
            flatten: &["c"],
            expected: Ok("id:Int64 / 1"),
        },
        FlattenBatchTestCase {
            description: "a list where an object was listed fails with the column, the row and the shape",
            rows: "{\"c\":{\"a\":1}}\n{\"c\":[1,2]}\n",
            flatten: &["c"],
            expected: Err("flatten c: row 2 holds a list, not an object"),
        },
        FlattenBatchTestCase {
            description: "a scalar column where an object was listed fails",
            rows: "{\"c\":\"GB\"}\n",
            flatten: &["c"],
            expected: Err("flatten c: row 1 holds a Utf8 value, not an object"),
        },
        FlattenBatchTestCase {
            description: "a flattened name that is already a field fails",
            rows: "{\"c__a\":0,\"c\":{\"a\":1}}\n",
            flatten: &["c"],
            expected: Err("flatten c: the key a gives column c__a, which the data already has"),
        },
    ];
    for test_case in test_cases {
        let result = flattened(&test_case);
        assert_eq!(
            result
                .as_ref()
                .map(String::as_str)
                .map_err(|error| error.split(';').next().unwrap_or_default()),
            test_case.expected,
            "{}: {result:?}",
            test_case.description
        );
    }
}
