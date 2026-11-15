use std::collections::BTreeMap;

use crate::load_naming::main::loaded_columns::loaded_columns;
use crate::load_naming::main::numbered_header::numbered_header;
use crate::load_naming::models::{
    ColumnName, NameRules, Naming, NumberedName, OnDuplicateNames, RenameColumn,
};
use crate::load_naming::tests::test_types::{LoadedColumnsTestCase, NumberedHeaderTestCase};

const NO_KEPT: &[(&str, &str, usize)] = &[];
const SNAKE: Naming = Naming::SnakeCase;
const NUMBER: OnDuplicateNames = OnDuplicateNames::Number;

#[test]
fn given_a_tables_columns_when_naming_them_then_naming_rename_and_numbering_apply_in_turn() {
    let test_cases = [
        LoadedColumnsTestCase {
            description: "snake_case is the default naming",
            columns: &["Order ID", "Qty", "createdAt"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Ok("order_id,qty,created_at"),
        },
        LoadedColumnsTestCase {
            description: "naming = \"source\" keeps the source's names",
            columns: &["Order ID", "Qty"],
            naming: Naming::Source,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Ok("Order ID,Qty"),
        },
        LoadedColumnsTestCase {
            description: "a rename map names a column by its source name or its default name",
            columns: &["Order ID", "Qty"],
            naming: SNAKE,
            max_length: None,
            renamed: &[("Order ID", "order_ref"), ("qty", "quantity")],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Ok("order_ref,quantity"),
        },
        LoadedColumnsTestCase {
            description: "a rename function tells repeated headers apart by position",
            columns: &["Qty", "Qty"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[(2, "qty_returned")],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Ok("qty,qty_returned"),
        },
        LoadedColumnsTestCase {
            description: "a rename that makes a clash fails, even when numbering would resolve it",
            columns: &["Order ID", "Qty"],
            naming: SNAKE,
            max_length: None,
            renamed: &[("Qty", "order_id")],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Err(
                "rename gives column \"Qty\" the name order_id, which column \"Order ID\" loads as too; give each a name of its own",
            ),
        },
        LoadedColumnsTestCase {
            description: "a rename that makes a clash fails under on_duplicate_names = \"fail\" too",
            columns: &["Order ID", "Qty"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[(2, "Order_ID")],
            on_duplicate_names: OnDuplicateNames::Fail,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Err(
                "rename gives column \"Qty\" the name Order_ID, which column \"Order ID\" loads as too; give each a name of its own",
            ),
        },
        LoadedColumnsTestCase {
            description: "a repeated header is numbered, skipping a name the table already has",
            columns: &["qty", "qty_1", "qty"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Ok("qty,qty_1,qty_2"),
        },
        LoadedColumnsTestCase {
            description: "a name taken by a later column is skipped too",
            columns: &["qty", "qty", "qty_1"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Ok("qty,qty_2,qty_1"),
        },
        LoadedColumnsTestCase {
            description: "different source names meeting after snake_case are numbered in source-name order, not by position",
            columns: &["OrderID", "Order ID"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Ok("order_id_1,order_id"),
        },
        LoadedColumnsTestCase {
            description: "names that differ only by case meet under naming = \"source\", as warehouses fold case",
            columns: &["Qty", "qty"],
            naming: Naming::Source,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Ok("Qty,qty_1"),
        },
        LoadedColumnsTestCase {
            description: "a numbered name is cut so the number fits the destination's limit",
            columns: &["abcdefgh", "abcdefgh"],
            naming: SNAKE,
            max_length: Some(8),
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Ok("abcdefgh,abcdef_1"),
        },
        LoadedColumnsTestCase {
            description: "on_duplicate_names = \"fail\" names the columns and the fixes",
            columns: &["OrderID", "Order ID"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: OnDuplicateNames::Fail,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Err(
                "columns \"OrderID\" and \"Order ID\" both load as order_id; give them names of their own with rename, or set on_duplicate_names = \"number\"",
            ),
        },
        LoadedColumnsTestCase {
            description: "a repeated header under on_duplicate_names = \"fail\" is named by its place",
            columns: &["Qty", "Qty"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: OnDuplicateNames::Fail,
            kept: NO_KEPT,
            existing: &[],
            expected_names: Err(
                "columns \"Qty\" (column 1) and \"Qty\" (column 2) both load as qty; give them names of their own with rename, or set on_duplicate_names = \"number\"",
            ),
        },
        LoadedColumnsTestCase {
            description: "numbers given before stay with their source names, and a new clash gets the next free number though it sorts first",
            columns: &["OrderID", "Order ID", "ORDER_ID"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: &[("order_id", "Order ID", 0), ("order_id_1", "OrderID", 0)],
            existing: &[],
            expected_names: Ok("order_id_1,order_id,order_id_2"),
        },
        LoadedColumnsTestCase {
            description: "a number given before stays with its source name when the clash is gone",
            columns: &["OrderID"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: &[("order_id", "Order ID", 0), ("order_id_1", "OrderID", 0)],
            existing: &[],
            expected_names: Ok("order_id_1"),
        },
        LoadedColumnsTestCase {
            description: "a name whose source column has gone is never given to another",
            columns: &["OrderID", "ORDER_ID"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: &[("order_id", "Order ID", 0), ("order_id_1", "OrderID", 0)],
            existing: &[],
            expected_names: Err(
                "column order_id holds \"Order ID\" from earlier loads, which the source no longer has, and \"ORDER_ID\" would now load into it; give \"ORDER_ID\" a name of its own with rename, or rename it to order_id if it is the same column under a new name",
            ),
        },
        LoadedColumnsTestCase {
            description: "renaming the new column onto the gone one's name keeps every number",
            columns: &["OrderID", "ORDER_ID"],
            naming: SNAKE,
            max_length: None,
            renamed: &[("ORDER_ID", "order_id")],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: &[("order_id", "Order ID", 0), ("order_id_1", "OrderID", 0)],
            existing: &[],
            expected_names: Ok("order_id_1,order_id"),
        },
        LoadedColumnsTestCase {
            description: "a clash on a column the table has, which no earlier load recorded, can't be numbered without guessing which column it holds",
            columns: &["OrderID", "Order ID"],
            naming: SNAKE,
            max_length: None,
            renamed: &[],
            returned: &[],
            on_duplicate_names: NUMBER,
            kept: NO_KEPT,
            existing: &["order_id"],
            expected_names: Err(
                "columns \"Order ID\" and \"OrderID\" now both load as order_id, which the table already has, and no earlier load recorded which of them it holds; give all but one a name of its own with rename",
            ),
        },
    ];
    for test_case in test_cases {
        let columns: Vec<ColumnName> = test_case
            .columns
            .iter()
            .enumerate()
            .map(|(index, name)| ColumnName {
                position: index + 1,
                source: (*name).to_owned(),
                read_as: (*name).to_owned(),
            })
            .collect();
        let rules = NameRules {
            naming: test_case.naming,
            max_length: test_case.max_length,
            renamed: test_case
                .renamed
                .iter()
                .map(|(from, to)| ((*from).to_owned(), (*to).to_owned()))
                .collect(),
            on_duplicate_names: test_case.on_duplicate_names,
            kept: test_case
                .kept
                .iter()
                .map(|(column, source, repeat)| NumberedName {
                    column: (*column).to_owned(),
                    source: (*source).to_owned(),
                    repeat: *repeat,
                })
                .collect(),
            function: None,
            existing: test_case
                .existing
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
        };
        let returned: BTreeMap<usize, String> = test_case
            .returned
            .iter()
            .map(|(position, name)| (*position, (*name).to_owned()))
            .collect();
        let rename = |_: &[RenameColumn]| Ok(returned.clone());
        let names = loaded_columns(&columns, &rules, &rename)
            .map(|names| {
                names
                    .columns
                    .iter()
                    .map(|column| column.loaded.clone())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .map_err(|problems| problems.join("; "));
        assert_eq!(
            names.as_deref(),
            test_case.expected_names.map_err(str::to_owned).as_deref(),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_header_row_when_reading_it_then_a_repeated_name_is_numbered_past_every_name_it_has() {
    let test_cases = [
        NumberedHeaderTestCase {
            description: "names the header has once stay as they are",
            header: &["id", "Qty"],
            expected_read: "id,Qty",
        },
        NumberedHeaderTestCase {
            description: "a repeat, exact or in another case, is numbered past the names the header has",
            header: &["Qty", "Qty_1", "Qty", "qty"],
            expected_read: "Qty,Qty_1,Qty_2<Qty,qty_3<qty",
        },
    ];
    for test_case in test_cases {
        let header: Vec<String> = test_case
            .header
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let read: Vec<String> = numbered_header(&header)
            .into_iter()
            .map(|(name, repeat)| {
                let repeated = repeat
                    .map(|repeat| format!("<{repeat}"))
                    .unwrap_or_default();
                format!("{name}{repeated}")
            })
            .collect();
        assert_eq!(
            read.join(","),
            test_case.expected_read,
            "{}",
            test_case.description
        );
    }
}
