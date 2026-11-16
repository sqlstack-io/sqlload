use crate::files_helpers::{names_case_outcome, text_bytes, workbook};
use crate::test_types::{ColumnNamesTestCase, NamesRun};

/// A CSV source appending each run's files.
const CSV: &str = "format = \"csv\"\nwrite_strategy = \"append\"\n";

/// A run that plans and loads.
const LOADS: NamesRun = NamesRun {
    file: "a.csv",
    contents: "",
    encode: text_bytes,
    expected_ok: true,
    expected_plan: "",
};

/// No query after the runs.
const NO_CHECK: (&str, &str) = ("SELECT ''", "");

/// The first load of the numbering memory cases: `Order ID` sorts first, so it keeps `order_id` and `OrderID` is numbered.
const ORDER_IDS: NamesRun = NamesRun {
    contents: "OrderID,Order ID\n1,2\n",
    expected_plan: "├── names   OrderID -> order_id_1 (numbered)",
    ..LOADS
};

#[test]
fn given_files_whose_names_need_naming_when_loading_then_columns_take_snake_case_their_renames_and_numbers_that_stay()
-> Result<(), String> {
    let test_cases = [
        ColumnNamesTestCase {
            description: "snake_case is the default, for the table's name too; a repeated header is numbered past a name the header has",
            settings: CSV,
            runs: &[NamesRun {
                contents: "Order ID,Qty,Qty,qty_1\n1,5,2,9\n",
                expected_plan: "├── names   Qty (column 3) -> qty_2 (numbered)",
                ..LOADS
            }],
            expected_columns: ("shop_orders", "order_id,qty,qty_2,qty_1"),
            check: (
                "SELECT qty || '/' || qty_2 || '/' || qty_1 FROM raw.shop_orders",
                "5/2/9",
            ),
        },
        ColumnNamesTestCase {
            description: "an Excel sheet's repeated header is numbered",
            settings: "format = \"xlsx\"\nwrite_strategy = \"append\"\n",
            runs: &[NamesRun {
                file: "a.xlsx",
                contents: "Order ID|Qty|Qty\n1|5|2\n",
                encode: workbook,
                expected_plan: "├── names   Qty (column 3) -> qty_1 (numbered)",
                ..LOADS
            }],
            expected_columns: ("shop_orders", "order_id,qty,qty_1"),
            check: ("SELECT qty || '/' || qty_1 FROM raw.shop_orders", "5/2"),
        },
        ColumnNamesTestCase {
            description: "naming = \"source\" on the table keeps the source's names",
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\n[sources.ShopOrders.tables.ShopOrders]\nnaming = \"source\"\n",
            runs: &[NamesRun {
                contents: "Order ID,Qty\n1,5\n",
                expected_plan: "├── columns 2",
                ..LOADS
            }],
            expected_columns: ("shoporders", "Order ID,Qty"),
            check: NO_CHECK,
        },
        ColumnNamesTestCase {
            description: "a rename map names a column by its source name",
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\nrename = { \"Order ID\" = \"order_ref\" }\n",
            runs: &[NamesRun {
                contents: "Order ID,Qty\n1,5\n",
                expected_plan: "├── names   Order ID -> order_ref (rename)",
                ..LOADS
            }],
            expected_columns: ("shop_orders", "order_ref,qty"),
            check: NO_CHECK,
        },
        ColumnNamesTestCase {
            description: "a rename that makes a clash fails plan, whatever on_duplicate_names says",
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\nrename = { Qty = \"order_id\" }\n",
            runs: &[NamesRun {
                contents: "Order ID,Qty\n1,5\n",
                expected_ok: false,
                expected_plan: "rename gives column \"Qty\" the name order_id, which column \"Order ID\" loads as too; give each a name of its own",
                ..LOADS
            }],
            expected_columns: ("shop_orders", ""),
            check: NO_CHECK,
        },
        ColumnNamesTestCase {
            description: "on_duplicate_names = \"fail\" refuses names colliding after naming, naming both",
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\non_duplicate_names = \"fail\"\n",
            runs: &[NamesRun {
                expected_ok: false,
                expected_plan: "columns \"OrderID\" and \"Order ID\" both load as order_id; give them names of their own with rename, or set on_duplicate_names = \"number\"",
                ..ORDER_IDS
            }],
            expected_columns: ("shop_orders", ""),
            check: NO_CHECK,
        },
        ColumnNamesTestCase {
            description: "numbers given once stay with their source names: a later file's ORDER_ID, which sorts first, gets the next free number",
            settings: CSV,
            runs: &[
                ORDER_IDS,
                NamesRun {
                    file: "b.csv",
                    contents: "OrderID,Order ID,ORDER_ID\n3,4,5\n",
                    expected_plan: "├── names   OrderID -> order_id_1 (numbered)\n    │           ORDER_ID -> order_id_2 (numbered)",
                    ..LOADS
                },
            ],
            expected_columns: ("shop_orders", "order_id_1,order_id,order_id_2"),
            check: (
                "SELECT string_agg(order_id || ':' || order_id_1 || ':' || coalesce(CAST(order_id_2 AS VARCHAR), '-'), ',' ORDER BY order_id) FROM raw.shop_orders",
                "2:1:-,4:3:5",
            ),
        },
        ColumnNamesTestCase {
            description: "a numbered column whose source name has gone, which another source name would take, fails plan",
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\nafter_load = \"delete\"\n",
            runs: &[
                ORDER_IDS,
                NamesRun {
                    file: "b.csv",
                    contents: "OrderID,ORDER_ID\n3,5\n",
                    expected_ok: false,
                    expected_plan: "column order_id holds \"Order ID\" from earlier loads, which the source no longer has, and \"ORDER_ID\" would now load into it; give \"ORDER_ID\" a name of its own with rename, or rename it to order_id if it is the same column under a new name",
                    ..LOADS
                },
            ],
            expected_columns: ("shop_orders", "order_id_1,order_id"),
            check: (
                "SELECT string_agg(order_id || ':' || order_id_1, ',') FROM raw.shop_orders",
                "2:1",
            ),
        },
    ];
    for test_case in &test_cases {
        let outcome = names_case_outcome(test_case)?;
        let expected_runs: Vec<(bool, bool, bool)> = test_case
            .runs
            .iter()
            .map(|run| (run.expected_ok, run.expected_ok, true))
            .collect();
        assert_eq!(
            (
                outcome.runs,
                outcome.columns.as_str(),
                outcome.checked.as_str()
            ),
            (
                expected_runs,
                test_case.expected_columns.1,
                test_case.check.1
            ),
            "{}: {}",
            test_case.description,
            outcome.output
        );
    }
    Ok(())
}
