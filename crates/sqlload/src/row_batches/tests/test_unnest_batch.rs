use crate::row_batches::tests::helpers::{every_list, items_parts_tags, tags_only, unnested};
use crate::row_batches::tests::test_types::UnnestBatchTestCase;

// Child ids are base64(shake_128("<parent id>_<child table>_<position>").digest(10)) without padding, computed with Python's hashlib: dlt 1.32.0's get_nested_row_hash.

#[test]
fn given_list_fields_when_unnesting_then_their_items_become_linked_child_rows_as_dlt_makes_them() {
    let test_cases = [
        UnnestBatchTestCase {
            description: "objects, a list inside them and scalars: each its table, linked by parent id and position, an empty or NULL list gives no rows",
            rows: "{\"id\":1,\"items\":[{\"sku\":\"a\",\"parts\":[1,2]},{\"sku\":\"b\"}],\"tags\":[\"x\",\"y\"]}\n{\"id\":2,\"items\":[],\"tags\":null}\n",
            unnest: items_parts_tags,
            max_depth: 1000,
            ids: &["P1", "P2"],
            roots: None,
            expected_batches: Ok(&[
                "id:Int64 / 1; 2",
                "orders__items = sku:Utf8,_sqlload_parent_id:Utf8,_sqlload_list_idx:Int64,_sqlload_row_id:Utf8 / a|P1|0|kSG+OAUJsLWF8Q; b|P1|1|mPLBfITNXt50Yg",
                "orders__items__parts = value:Int64,_sqlload_parent_id:Utf8,_sqlload_list_idx:Int64,_sqlload_row_id:Utf8 / 1|kSG+OAUJsLWF8Q|0|8qAwaxawytfkAw; 2|kSG+OAUJsLWF8Q|1|6IR6ucYOHMhXRQ",
                "orders__tags = value:Utf8,_sqlload_parent_id:Utf8,_sqlload_list_idx:Int64,_sqlload_row_id:Utf8 / x|P1|0|1+5nzIHNA4e8vg; y|P1|1|lyALiK/3E0Fv4w",
            ]),
        },
        UnnestBatchTestCase {
            description: "under a merge every level carries the root row's id",
            rows: "{\"id\":1,\"items\":[{\"sku\":\"a\",\"parts\":[1]}]}\n",
            unnest: items_parts_tags,
            max_depth: 1000,
            ids: &["P1"],
            roots: Some(&["P1"]),
            expected_batches: Ok(&[
                "id:Int64 / 1",
                "orders__items = sku:Utf8,_sqlload_root_id:Utf8,_sqlload_parent_id:Utf8,_sqlload_list_idx:Int64,_sqlload_row_id:Utf8 / a|P1|P1|0|kSG+OAUJsLWF8Q",
                "orders__items__parts = value:Int64,_sqlload_root_id:Utf8,_sqlload_parent_id:Utf8,_sqlload_list_idx:Int64,_sqlload_row_id:Utf8 / 1|P1|kSG+OAUJsLWF8Q|0|8qAwaxawytfkAw",
            ]),
        },
        UnnestBatchTestCase {
            description: "all, cut at max_depth 1: the list inside the items stays a JSON column, a scalar column stays in the parent",
            rows: "{\"id\":1,\"note\":\"n\",\"items\":[{\"sku\":\"a\",\"parts\":[1,2]}]}\n",
            unnest: every_list,
            max_depth: 1,
            ids: &["P1"],
            roots: None,
            expected_batches: Ok(&[
                "id:Int64,note:Utf8 / 1|n",
                "orders__items = sku:Utf8,parts:Utf8(json),_sqlload_parent_id:Utf8,_sqlload_list_idx:Int64,_sqlload_row_id:Utf8 / a|[1,2]|P1|0|kSG+OAUJsLWF8Q",
            ]),
        },
        UnnestBatchTestCase {
            description: "lists in a list: each inner list is an item with a list field, as dlt's orders__m__list",
            rows: "{\"id\":1,\"m\":[[1,2],[3]]}\n",
            unnest: every_list,
            max_depth: 1000,
            ids: &["P1"],
            roots: None,
            expected_batches: Ok(&[
                "id:Int64 / 1",
                "orders__m = _sqlload_parent_id:Utf8,_sqlload_list_idx:Int64,_sqlload_row_id:Utf8 / P1|0|v6Rwb1UkNtE0rA; P1|1|1/Eu6rJ09tjFuA",
                "orders__m__list = value:Int64,_sqlload_parent_id:Utf8,_sqlload_list_idx:Int64,_sqlload_row_id:Utf8 / 1|v6Rwb1UkNtE0rA|0|V4it2p9NB3ueLA; 2|v6Rwb1UkNtE0rA|1|MemscPod7WeYZw; 3|1/Eu6rJ09tjFuA|0|PZUUw3OF+jLPLw",
            ]),
        },
        UnnestBatchTestCase {
            description: "a listed field holding one object, as XML gives a single match, is a list of it: the child row, no parent column",
            rows: "{\"id\":1,\"tags\":[{\"k\":\"x\"}]}\n{\"id\":2,\"tags\":{\"k\":\"z\"}}\n",
            unnest: tags_only,
            max_depth: 1000,
            ids: &["P1", "P2"],
            roots: None,
            expected_batches: Ok(&[
                "id:Int64 / 1; 2",
                "orders__tags = k:Utf8,_sqlload_parent_id:Utf8,_sqlload_list_idx:Int64,_sqlload_row_id:Utf8 / x|P1|0|1+5nzIHNA4e8vg; z|P2|0|RLNQPupPJM5VLA",
            ]),
        },
        UnnestBatchTestCase {
            description: "an item field taking a linking column's name is refused, naming it",
            rows: "{\"id\":1,\"tags\":[{\"_sqlload_parent_id\":\"q\"}]}\n",
            unnest: tags_only,
            max_depth: 1000,
            ids: &["P1"],
            roots: None,
            expected_batches: Err(
                "tags: its items have a field _sqlload_parent_id, the name of the column linking orders__tags to its parent; rename it before loading",
            ),
        },
    ];
    for test_case in &test_cases {
        assert_eq!(
            unnested(test_case),
            test_case
                .expected_batches
                .map(|shown| shown.iter().map(|line| (*line).to_owned()).collect())
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
