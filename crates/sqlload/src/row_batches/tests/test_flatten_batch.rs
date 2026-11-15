use crate::row_batches::models::FlattenObjects;
use crate::row_batches::tests::helpers::{flattened, paths};
use crate::row_batches::tests::test_types::FlattenBatchTestCase;

#[test]
fn given_listed_top_level_objects_when_flattening_then_one_level_becomes_typed_columns_and_the_rest_stays_json()
 {
    let test_cases = [
        FlattenBatchTestCase {
            description: "the ticket's example: a listed object expands one level where it stands, nested values and unlisted maps stay JSON",
            rows: "{\"order_id\":7,\"customer\":{\"country\":\"GB\",\"address\":{\"city\":\"Leeds\"}},\"line_totals\":{\"sku-14\":12.5}}\n",
            flatten: || paths(&["customer"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Ok(
                "order_id:Int64,customer__country:Utf8,customer__address:Utf8(json),line_totals:Utf8(json) / 7|GB|{\"city\":\"Leeds\"}|{\"sku-14\":12.5}",
            ),
        },
        FlattenBatchTestCase {
            description: "keys are typed across rows; a NULL, missing or empty object and a missing key are NULL; a key only later rows have is added",
            rows: "{\"id\":1,\"c\":{\"n\":1,\"ok\":true}}\n{\"id\":2,\"c\":null}\n{\"id\":3}\n{\"id\":4,\"c\":{}}\n{\"id\":5,\"c\":{\"n\":2.5,\"tier\":\"gold\"}}\n",
            flatten: || paths(&["c"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Ok(
                "id:Int64,c__n:Float64,c__ok:Boolean,c__tier:Utf8 / 1|1.0|true|NULL; 2|NULL|NULL|NULL; 3|NULL|NULL|NULL; 4|NULL|NULL|NULL; 5|2.5|NULL|gold",
            ),
        },
        FlattenBatchTestCase {
            description: "a listed object that only ever held NULL adds no columns",
            rows: "{\"id\":1,\"c\":null}\n",
            flatten: || paths(&["c"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Ok("id:Int64 / 1"),
        },
        FlattenBatchTestCase {
            description: "a listed name the data does not have changes nothing",
            rows: "{\"id\":1}\n",
            flatten: || paths(&["c"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Ok("id:Int64 / 1"),
        },
        FlattenBatchTestCase {
            description: "a list where an object was listed fails with the column, the row and the shape",
            rows: "{\"c\":{\"a\":1}}\n{\"c\":[1,2]}\n",
            flatten: || paths(&["c"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Err("flatten c: row 2 of a batch holds a list, not an object"),
        },
        FlattenBatchTestCase {
            description: "a scalar column where an object was listed fails",
            rows: "{\"c\":\"GB\"}\n",
            flatten: || paths(&["c"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Err("flatten c: row 1 of a batch holds a text value, not an object"),
        },
        FlattenBatchTestCase {
            description: "objects mixed with a scalar name the scalar's row",
            rows: "{\"c\":{\"a\":1}}\n{\"c\":\"GB\"}\n",
            flatten: || paths(&["c"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Err(
                "flatten c: row 2 of a batch holds a scalar (other rows hold objects), not an object",
            ),
        },
        FlattenBatchTestCase {
            description: "a number where an object was listed fails in words",
            rows: "{\"c\":5}\n",
            flatten: || paths(&["c"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Err("flatten c: row 1 of a batch holds a number, not an object"),
        },
        FlattenBatchTestCase {
            description: "a flattened name that is already a field fails",
            rows: "{\"c__a\":0,\"c\":{\"a\":1}}\n",
            flatten: || paths(&["c"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Err(
                "flatten c: the key a gives column c__a, which the data already has",
            ),
        },
        FlattenBatchTestCase {
            description: "two listed objects giving the same name fail",
            rows: "{\"a\":{\"_b\":1},\"a_\":{\"b\":2}}\n",
            flatten: || paths(&["a", "a_"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Err(
                "flatten a_: the key b gives column a___b, which flattening a also gives",
            ),
        },
    ];
    for test_case in test_cases {
        let result = flattened(&test_case);
        assert_eq!(
            result
                .as_ref()
                .map(String::as_str)
                .map_err(|error| error.split(';').next().unwrap_or_default()),
            test_case.expected_batch,
            "{}: {result:?}",
            test_case.description
        );
    }
}

#[test]
fn given_dotted_paths_or_all_when_flattening_then_objects_flatten_at_every_level_asked_and_a_json_pin_keeps_one_whole()
 {
    let test_cases = [
        FlattenBatchTestCase {
            description: "a dotted path two levels deep flattens the object inside its flattened parent",
            rows: "{\"id\":1,\"customer\":{\"name\":\"Ann\",\"address\":{\"city\":\"Oslo\",\"geo\":{\"lat\":1}}}}\n",
            flatten: || paths(&["customer", "customer.address"]),
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Ok(
                "id:Int64,customer__name:Utf8,customer__address__city:Utf8,customer__address__geo:Utf8(json) / 1|Ann|Oslo|{\"lat\":1}",
            ),
        },
        FlattenBatchTestCase {
            description: "\"all\" flattens every object at every level, and leaves lists and scalars as they are",
            rows: "{\"id\":1,\"c\":{\"a\":{\"x\":1}},\"tags\":[1],\"line_totals\":{\"sku-14\":12.5,\"sku-99\":3.0}}\n",
            flatten: || FlattenObjects::All,
            keep: &[],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Ok(
                "id:Int64,c__a__x:Int64,tags:Utf8(json),line_totals__sku-14:Float64,line_totals__sku-99:Float64 / 1|1|[1]|12.5|3.0",
            ),
        },
        FlattenBatchTestCase {
            description: "under \"all\", a json pin keeps a map whole, by its loaded name",
            rows: "{\"id\":1,\"LineTotals\":{\"sku-14\":12.5},\"c\":{\"n\":1}}\n",
            flatten: || FlattenObjects::All,
            keep: &["line_totals"],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Ok("id:Int64,LineTotals:Utf8(json),c__n:Int64 / 1|{\"sku-14\":12.5}|1"),
        },
        FlattenBatchTestCase {
            description: "a json pin on a nested object keeps it whole inside its flattened parent",
            rows: "{\"c\":{\"n\":1,\"m\":{\"k\":2}}}\n",
            flatten: || FlattenObjects::All,
            keep: &["c__m"],
            pinned: &[],
            max_depth: 1000,
            expected_batch: Ok("c__n:Int64,c__m:Utf8(json) / 1|{\"k\":2}"),
        },
        FlattenBatchTestCase {
            description: "pinning a flattened object as text fails, pointing at a json pin",
            rows: "{\"line_totals\":{\"sku-14\":12.5}}\n",
            flatten: || FlattenObjects::All,
            keep: &[],
            pinned: &[("line_totals", "text")],
            max_depth: 1000,
            expected_batch: Err(
                "columns line_totals = \"text\": flatten replaces line_totals with its keys' columns",
            ),
        },
        FlattenBatchTestCase {
            description: "objects deeper than max_depth stay JSON",
            rows: "{\"c\":{\"a\":{\"x\":1}}}\n",
            flatten: || FlattenObjects::All,
            keep: &[],
            pinned: &[],
            max_depth: 1,
            expected_batch: Ok("c__a:Utf8(json) / {\"x\":1}"),
        },
    ];
    for test_case in test_cases {
        let result = flattened(&test_case);
        assert_eq!(
            result
                .as_ref()
                .map(String::as_str)
                .map_err(|error| error.split(';').next().unwrap_or_default()),
            test_case.expected_batch,
            "{}: {result:?}",
            test_case.description
        );
    }
}
