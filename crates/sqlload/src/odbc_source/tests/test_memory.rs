use crate::odbc_source::_helpers::columns::fetch_size;
use crate::odbc_source::_helpers::memory::{budget_note, cgroup_limit, fetch_budget, memory_line};
use crate::odbc_source::main::memory_warning::memory_warning;
use crate::odbc_source::models::{BudgetReason, FetchShape, FetchSize, MemoryLimit};
use crate::odbc_source::tests::test_types::{
    BudgetNoteTestCase, CgroupLimitTestCase, FetchBudgetTestCase, FetchSizeTestCase,
    MemoryLineTestCase, MemoryWarningTestCase,
};

const MIB: usize = 1024 * 1024;

const GIB: u64 = 1024 * 1024 * 1024;

#[test]
fn given_a_memory_limit_and_concurrency_when_choosing_a_fetch_budget_then_half_the_limit_is_shared_and_512_mib_never_raised()
 {
    let test_cases = [
        FetchBudgetTestCase {
            description: "a 2 GiB container with concurrency 4: half of it shared by 4 tables, 256 MiB each",
            set: None,
            limit: Some(MemoryLimit {
                bytes: 2 * GIB,
                container: true,
            }),
            concurrency: 4,
            expected_bytes: 256 * MIB,
            expected_reason: BudgetReason::Capped,
        },
        FetchBudgetTestCase {
            description: "a 64 GiB machine: its share is more than 512 MiB, which stays",
            set: None,
            limit: Some(MemoryLimit {
                bytes: 64 * GIB,
                container: false,
            }),
            concurrency: 4,
            expected_bytes: 512 * MIB,
            expected_reason: BudgetReason::Default,
        },
        FetchBudgetTestCase {
            description: "exactly 512 MiB each is the default, not a cap",
            set: None,
            limit: Some(MemoryLimit {
                bytes: 4 * GIB,
                container: false,
            }),
            concurrency: 4,
            expected_bytes: 512 * MIB,
            expected_reason: BudgetReason::Default,
        },
        FetchBudgetTestCase {
            description: "an unknown limit keeps 512 MiB",
            set: None,
            limit: None,
            concurrency: 16,
            expected_bytes: 512 * MIB,
            expected_reason: BudgetReason::Default,
        },
        FetchBudgetTestCase {
            description: "max_batch_memory wins over the limit, even above 512 MiB",
            set: Some(2_000_000_000),
            limit: Some(MemoryLimit {
                bytes: 2 * GIB,
                container: true,
            }),
            concurrency: 4,
            expected_bytes: 2_000_000_000,
            expected_reason: BudgetReason::Set,
        },
        FetchBudgetTestCase {
            description: "concurrency 0 counts as 1",
            set: None,
            limit: Some(MemoryLimit {
                bytes: GIB,
                container: true,
            }),
            concurrency: 0,
            expected_bytes: 512 * MIB,
            expected_reason: BudgetReason::Default,
        },
    ];
    for test_case in test_cases {
        let budget = fetch_budget(test_case.set, test_case.limit, test_case.concurrency);
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
fn given_cgroup_v2_folders_when_reading_the_memory_limit_then_the_lowest_memory_max_from_the_process_group_up_is_taken()
-> Result<(), String> {
    let test_cases = [
        CgroupLimitTestCase {
            description: "a container's own group sets memory.max",
            groups: "0::/\n",
            limits: &[("", "2147483648\n")],
            expected_limit: Some(2_147_483_648),
        },
        CgroupLimitTestCase {
            description: "the process's group is unlimited, a parent slice is not",
            groups: "0::/app.slice/run.scope\n",
            limits: &[
                ("app.slice/run.scope", "max\n"),
                ("app.slice", "6442450944\n"),
            ],
            expected_limit: Some(6_442_450_944),
        },
        CgroupLimitTestCase {
            description: "the lowest of several limits",
            groups: "0::/a/b\n",
            limits: &[("a/b", "8589934592\n"), ("a", "1073741824\n")],
            expected_limit: Some(1_073_741_824),
        },
        CgroupLimitTestCase {
            description: "no limit anywhere",
            groups: "0::/a\n",
            limits: &[("a", "max\n")],
            expected_limit: None,
        },
        CgroupLimitTestCase {
            description: "cgroup v1 only: no v2 line",
            groups: "4:memory:/docker/abc\n",
            limits: &[],
            expected_limit: None,
        },
    ];
    for test_case in test_cases {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        for (folder, limit) in test_case.limits {
            let folder = root.path().join(folder);
            std::fs::create_dir_all(&folder).map_err(|error| error.to_string())?;
            std::fs::write(folder.join("memory.max"), limit).map_err(|error| error.to_string())?;
        }
        assert_eq!(
            cgroup_limit(test_case.groups, root.path()),
            Ok(test_case.expected_limit),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_memory_limit_when_planning_an_odbc_table_then_the_budget_line_says_what_set_it() {
    let test_cases = [
        BudgetNoteTestCase {
            description: "a 2 GiB container with concurrency 4 caps it at 256 MiB, and says so",
            set: None,
            limit: Some(MemoryLimit {
                bytes: 2 * GIB,
                container: true,
            }),
            unreadable: None,
            concurrency: 4,
            expected_note: "256 MiB fetch buffer per table (capped: 2 GiB container limit, concurrency 4)",
        },
        BudgetNoteTestCase {
            description: "a large machine keeps the default",
            set: None,
            limit: Some(MemoryLimit {
                bytes: 64 * GIB,
                container: false,
            }),
            unreadable: None,
            concurrency: 4,
            expected_note: "512 MiB fetch buffer per table (default; 64 GiB of memory, concurrency 4)",
        },
        BudgetNoteTestCase {
            description: "the source's setting, in binary units to a tenth",
            set: Some(2_000_000_000),
            limit: Some(MemoryLimit {
                bytes: 2 * GIB,
                container: true,
            }),
            unreadable: None,
            concurrency: 4,
            expected_note: "1.9 GiB fetch buffer per table (max_batch_memory)",
        },
        BudgetNoteTestCase {
            description: "a machine whose memory isn't known (Windows)",
            set: None,
            limit: None,
            unreadable: None,
            concurrency: 4,
            expected_note: "512 MiB fetch buffer per table (default; this machine's memory is unknown)",
        },
        BudgetNoteTestCase {
            description: "a limit file that can't be read says why",
            set: None,
            limit: None,
            unreadable: Some("/sys/fs/cgroup/memory.max: \"lots\": invalid digit found in string"),
            concurrency: 4,
            expected_note: "512 MiB fetch buffer per table (default; this machine's memory can't be read: /sys/fs/cgroup/memory.max: \"lots\": invalid digit found in string)",
        },
    ];
    for test_case in test_cases {
        let budget = fetch_budget(test_case.set, test_case.limit, test_case.concurrency);
        assert_eq!(
            budget_note(&budget, test_case.unreadable),
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
fn given_the_tables_loading_at_once_when_their_fetches_pass_half_the_memory_limit_then_plan_warns_naming_the_fix()
 {
    let test_cases = [
        MemoryWarningTestCase {
            description: "two 600 MiB fetches at once in a 2 GiB container pass its half; exactly half, as a capped budget gives, would not",
            fetches: &[(600 * MIB, "erp"), (600 * MIB, "erp"), (MIB, "crm")],
            limit: Some(2 * GIB),
            concurrency: 2,
            expected_warning: Some(
                "the ODBC fetch buffers of 2 tables loading at once may hold 1.2 GiB together, more than half of this machine's 2 GiB memory limit; this counts only those buffers, not the memory the batches, the destination and the commits take besides; lower max_batch_memory on source erp, or lower concurrency (now 2)",
            ),
        },
        MemoryWarningTestCase {
            description: "only the largest `concurrency` count: three tables, two at once, within half",
            fetches: &[(400 * MIB, "erp"), (400 * MIB, "crm"), (400 * MIB, "erp")],
            limit: Some(2 * GIB),
            concurrency: 2,
            expected_warning: None,
        },
        MemoryWarningTestCase {
            description: "sources named in order, once each",
            fetches: &[(3 * GIB as usize, "erp"), (MIB, "crm")],
            limit: Some(4 * GIB),
            concurrency: 4,
            expected_warning: Some(
                "the ODBC fetch buffers of 2 tables loading at once may hold 3 GiB together, more than half of this machine's 4 GiB memory limit; this counts only those buffers, not the memory the batches, the destination and the commits take besides; lower max_batch_memory on sources crm, erp, or lower concurrency (now 4)",
            ),
        },
        MemoryWarningTestCase {
            description: "no warning without a known limit",
            fetches: &[(64 * GIB as usize, "erp")],
            limit: None,
            concurrency: 4,
            expected_warning: None,
        },
    ];
    for test_case in test_cases {
        let fetches: Vec<(usize, &str, Option<u64>)> = test_case
            .fetches
            .iter()
            .map(|(bytes, source)| (*bytes, *source, test_case.limit))
            .collect();
        assert_eq!(
            memory_warning(&fetches, test_case.concurrency).as_deref(),
            test_case.expected_warning,
            "{}",
            test_case.description
        );
    }
}
