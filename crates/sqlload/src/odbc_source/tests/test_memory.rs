use crate::odbc_source::_helpers::columns::fetch_size;
use crate::odbc_source::_helpers::memory::{budget_note, fetch_budget, memory_line};
use crate::odbc_source::main::memory_warning::memory_warning;
use crate::odbc_source::models::{BudgetReason, FetchShape, FetchSize};
use crate::odbc_source::tests::test_types::{
    BudgetNoteTestCase, FetchBudgetTestCase, FetchSizeTestCase,
    MemoryLineTestCase, MemoryWarningTestCase,
};

const MIB: usize = 1024 * 1024;

const GIB: u64 = 1024 * 1024 * 1024;

#[test]
fn given_a_tables_share_of_the_memory_budget_when_choosing_a_fetch_budget_then_a_quarter_of_it_caps_512_mib_and_never_raises_it()
 {
    let test_cases = [
        FetchBudgetTestCase {
            description: "a 1 GiB share: a quarter of it, 256 MiB",
            set: None,
            share: Some(GIB),
            expected_bytes: 256 * MIB,
            expected_reason: BudgetReason::Capped,
        },
        FetchBudgetTestCase {
            description: "an 8 GiB share: its quarter is more than 512 MiB, which stays",
            set: None,
            share: Some(8 * GIB),
            expected_bytes: 512 * MIB,
            expected_reason: BudgetReason::Default,
        },
        FetchBudgetTestCase {
            description: "exactly 512 MiB is the default, not a cap",
            set: None,
            share: Some(2 * GIB),
            expected_bytes: 512 * MIB,
            expected_reason: BudgetReason::Default,
        },
        FetchBudgetTestCase {
            description: "no budget keeps 512 MiB",
            set: None,
            share: None,
            expected_bytes: 512 * MIB,
            expected_reason: BudgetReason::Default,
        },
        FetchBudgetTestCase {
            description: "max_batch_memory wins over the share, even above 512 MiB",
            set: Some(2_000_000_000),
            share: Some(GIB),
            expected_bytes: 2_000_000_000,
            expected_reason: BudgetReason::Set,
        },
    ];
    for test_case in test_cases {
        let budget = fetch_budget(test_case.set, test_case.share);
        assert_eq!(
            (budget.bytes, budget.reason),
            (test_case.expected_bytes, test_case.expected_reason),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_read_shape_when_sizing_one_fetch_then_bound_rows_fit_the_budget_and_a_read_value_by_value_counts_its_values()
 {
    let test_cases = [
        FetchSizeTestCase {
            description: "a narrow table keeps its 65,536 rows per fetch",
            shape: FetchShape {
                bound_bytes_per_row: 120,
                long_columns: Vec::new(),
                columns: 4,
            },
            budget: 512 * MIB,
            expected_size: FetchSize {
                rows: 65_536,
                bytes: 65_536 * 120,
            },
        },
        FetchSizeTestCase {
            description: "a table of wide bound columns gets as many rows as the budget holds",
            shape: FetchShape {
                bound_bytes_per_row: 100_000,
                long_columns: Vec::new(),
                columns: 30,
            },
            budget: 256 * MIB,
            expected_size: FetchSize {
                rows: 2_684,
                bytes: 268_400_000,
            },
        },
        FetchSizeTestCase {
            description: "a row larger than the budget still fetches one row",
            shape: FetchShape {
                bound_bytes_per_row: 2 * MIB,
                long_columns: Vec::new(),
                columns: 2,
            },
            budget: MIB,
            expected_size: FetchSize {
                rows: 1,
                bytes: 2 * MIB,
            },
        },
        FetchSizeTestCase {
            description: "a table with a long column reads up to 65,536 rows a fetch, ended by its values reaching the budget",
            shape: FetchShape {
                bound_bytes_per_row: 0,
                long_columns: vec!["body".to_owned()],
                columns: 3,
            },
            budget: 64 * MIB,
            expected_size: FetchSize {
                rows: 65_536,
                bytes: 64 * MIB,
            },
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            fetch_size(&test_case.shape, test_case.budget, 65_536),
            test_case.expected_size,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_tables_share_when_planning_an_odbc_table_then_the_buffer_line_says_what_set_it() {
    let test_cases = [
        BudgetNoteTestCase {
            description: "a 1 GiB share caps it at 256 MiB, and says so",
            set: None,
            share: Some(GIB),
            expected_note: "256 MiB fetch buffer per table (capped: a quarter of each table's 1 GiB share of the memory budget)",
        },
        BudgetNoteTestCase {
            description: "a large share keeps the default",
            set: None,
            share: Some(8 * GIB),
            expected_note: "512 MiB fetch buffer per table (default; each table's share of the memory budget is 8 GiB)",
        },
        BudgetNoteTestCase {
            description: "the source's setting, in binary units to a tenth",
            set: Some(2_000_000_000),
            share: Some(GIB),
            expected_note: "1.9 GiB fetch buffer per table (max_batch_memory)",
        },
        BudgetNoteTestCase {
            description: "a run without a budget",
            set: None,
            share: None,
            expected_note: "512 MiB fetch buffer per table (default; the run has no memory budget)",
        },
    ];
    for test_case in test_cases {
        let budget = fetch_budget(test_case.set, test_case.share);
        assert_eq!(
            budget_note(&budget),
            test_case.expected_note,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_read_shape_when_planning_an_odbc_table_then_the_memory_line_shows_one_fetch() {
    let test_cases = [
        MemoryLineTestCase {
            description: "a narrow table: one fetch's buffers and rows",
            shape: FetchShape {
                bound_bytes_per_row: 64,
                long_columns: Vec::new(),
                columns: 4,
            },
            budget: 512 * MIB,
            expected_line: "~4 MiB per fetch, 65,536 rows",
        },
        MemoryLineTestCase {
            description: "265 long columns: the budget ends each fetch, and three are named",
            shape: FetchShape {
                bound_bytes_per_row: 0,
                long_columns: (1..=265).map(|number| format!("c{number:03}")).collect(),
                columns: 266,
            },
            budget: 256 * MIB,
            expected_line: "up to 256 MiB or 65,536 rows per fetch; 265 long columns (c001, c002, c003, …), so all 266 columns are read value by value",
        },
        MemoryLineTestCase {
            description: "one long column, named",
            shape: FetchShape {
                bound_bytes_per_row: 0,
                long_columns: vec!["body".to_owned()],
                columns: 3,
            },
            budget: 512 * MIB,
            expected_line: "up to 512 MiB or 65,536 rows per fetch; 1 long column (body), so all 3 columns are read value by value",
        },
    ];
    for test_case in test_cases {
        let size = fetch_size(&test_case.shape, test_case.budget, 65_536);
        assert_eq!(
            memory_line(&test_case.shape, &size),
            test_case.expected_line,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_planned_fetches_when_one_holds_more_than_its_part_of_the_tables_share_then_plan_warns_naming_the_fix()
 {
    let test_cases = [
        MemoryWarningTestCase {
            description: "a 600 MiB fetch set by max_batch_memory against a 1 GiB share, whose part is 256 MiB",
            fetches: &[(600 * MIB, "erp"), (100 * MIB, "erp"), (MIB, "crm")],
            share: Some(GIB),
            expected_warning: Some(
                "an ODBC fetch may hold 600 MiB, more than the 256 MiB a fetch gets of each table's 1 GiB share of the memory budget; lower max_batch_memory on source erp, or raise memory_limit",
            ),
        },
        MemoryWarningTestCase {
            description: "exactly the part is within it",
            fetches: &[(256 * MIB, "erp")],
            share: Some(GIB),
            expected_warning: None,
        },
        MemoryWarningTestCase {
            description: "sources named in order, once each, with the largest fetch",
            fetches: &[(300 * MIB, "erp"), (900 * MIB, "crm"), (400 * MIB, "erp")],
            share: Some(GIB),
            expected_warning: Some(
                "an ODBC fetch may hold 900 MiB, more than the 256 MiB a fetch gets of each table's 1 GiB share of the memory budget; lower max_batch_memory on sources crm, erp, or raise memory_limit",
            ),
        },
        MemoryWarningTestCase {
            description: "no warning without a budget",
            fetches: &[(64 * GIB as usize, "erp")],
            share: None,
            expected_warning: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            memory_warning(test_case.fetches, test_case.share).as_deref(),
            test_case.expected_warning,
            "{}",
            test_case.description
        );
    }
}
