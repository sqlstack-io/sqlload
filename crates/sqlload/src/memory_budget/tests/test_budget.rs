use crate::memory_budget::_helpers::budget_line::budget_line;
use crate::memory_budget::_helpers::plan_budget::plan_budget;
use crate::memory_budget::models::{BudgetOrigin, BudgetRequest, LimitOrigin, MemoryLimit};
use crate::memory_budget::tests::test_types::PlanBudgetTestCase;

const MIB: u64 = 1024 * 1024;

const GIB: u64 = 1024 * MIB;

#[test]
fn given_a_limit_or_a_setting_when_planning_the_memory_budget_then_duckdb_and_the_tables_share_it_and_plan_says_where_it_came_from()
 {
    let test_cases = [
        PlanBudgetTestCase {
            description: "a 2 GiB container: 80% of it; DuckDB's 30% would give a memory_limit under 256MB, so it gets 256MB",
            set: None,
            limit: Some((2 * GIB, LimitOrigin::Container)),
            overhead: 100 * MIB,
            duckdb: 1,
            concurrency: 4,
            cores: 8,
            expected_duckdb: Some((512_000_000, 256_000_000, 2, 64_000_000)),
            expected_tables: (275_282_329, 4),
            expected_line: Ok(
                "1.6 GiB budget (80% of the 2 GiB container limit); 100 MiB held before loading; DuckDB 488 MiB (memory_limit 244 MiB, 2 threads); 263 MiB per table, 4 at once",
            ),
        },
        PlanBudgetTestCase {
            description: "a 16 GiB machine without DuckDB: half of it, all for the tables",
            set: None,
            limit: Some((16 * GIB, LimitOrigin::Machine)),
            overhead: 200 * MIB,
            duckdb: 0,
            concurrency: 4,
            cores: 8,
            expected_duckdb: None,
            expected_tables: (2_095_054_848, 4),
            expected_line: Ok(
                "8 GiB budget (50% of 16 GiB of memory); 200 MiB held before loading; 2 GiB per table, 4 at once",
            ),
        },
        PlanBudgetTestCase {
            description: "a Windows job object's limit counts as a container's",
            set: None,
            limit: Some((4 * GIB, LimitOrigin::JobObject)),
            overhead: 100 * MIB,
            duckdb: 0,
            concurrency: 4,
            cores: 8,
            expected_duckdb: None,
            expected_tables: (832_779_059, 4),
            expected_line: Ok(
                "3.2 GiB budget (80% of the 4 GiB job object limit); 100 MiB held before loading; 794 MiB per table, 4 at once",
            ),
        },
        PlanBudgetTestCase {
            description: "--memory-limit wins over the machine; 124 MiB left fits one table, not 8",
            set: Some((GIB, BudgetOrigin::Flag)),
            limit: Some((64 * GIB, LimitOrigin::Machine)),
            overhead: 900 * MIB,
            duckdb: 0,
            concurrency: 8,
            cores: 8,
            expected_duckdb: None,
            expected_tables: (130_023_424, 1),
            expected_line: Ok(
                "1 GiB budget (--memory-limit); 900 MiB held before loading; 124 MiB per table, 1 at once (concurrency 8 doesn't fit)",
            ),
        },
        PlanBudgetTestCase {
            description: "two DuckDB targets split the 30%; threads follow the memory_limit, capped at the cores",
            set: Some((8 * GIB, BudgetOrigin::Setting)),
            limit: None,
            overhead: 50 * MIB,
            duckdb: 2,
            concurrency: 2,
            cores: 3,
            expected_duckdb: Some((1_280_625_868, 640_312_934, 3, 160_078_233)),
            expected_tables: (2_988_127_028, 2),
            expected_line: Ok(
                "8 GiB budget (memory_limit); 50 MiB held before loading; DuckDB 1.2 GiB each of 2 (memory_limit 611 MiB, 3 threads); 2.8 GiB per table, 2 at once",
            ),
        },
        PlanBudgetTestCase {
            description: "the overhead alone passes the budget",
            set: Some((512 * MIB, BudgetOrigin::Setting)),
            limit: None,
            overhead: 600 * MIB,
            duckdb: 0,
            concurrency: 4,
            cores: 8,
            expected_duckdb: None,
            expected_tables: (0, 0),
            expected_line: Err(
                "the 512 MiB memory budget is no more than the 600 MiB SQLLoad holds before loading anything; raise memory_limit to at least 664MiB",
            ),
        },
        PlanBudgetTestCase {
            description: "DuckDB's 256MB leaves the tables less than one table's 64 MiB",
            set: Some((600 * MIB, BudgetOrigin::Setting)),
            limit: None,
            overhead: 100 * MIB,
            duckdb: 1,
            concurrency: 4,
            cores: 8,
            expected_duckdb: None,
            expected_tables: (0, 0),
            expected_line: Err(
                "the 600 MiB memory budget leaves the tables 11.7 MiB after DuckDB's 488 MiB, under the 64 MiB one table loads in; raise memory_limit to at least 653MiB",
            ),
        },
        PlanBudgetTestCase {
            description: "no limit and no setting: no budget, and plan names the setting",
            set: None,
            limit: None,
            overhead: 100 * MIB,
            duckdb: 1,
            concurrency: 4,
            cores: 8,
            expected_duckdb: None,
            expected_tables: (0, 0),
            expected_line: Ok("no budget: this OS doesn't give its memory; set memory_limit"),
        },
    ];
    for test_case in test_cases {
        let request = BudgetRequest {
            set: test_case.set,
            limit: test_case.limit.map(|(bytes, origin)| MemoryLimit { bytes, origin }),
            overhead: test_case.overhead,
            duckdb: test_case.duckdb,
            concurrency: test_case.concurrency,
            cores: test_case.cores,
        };
        let planned = plan_budget(&request);
        let line = planned.as_ref().map(|budget| {
            budget_line(budget.as_ref(), (test_case.concurrency, None))
        });
        assert_eq!(
            line.as_ref()
                .map(String::as_str)
                .map_err(|error| error.as_str()),
            test_case.expected_line,
            "{}",
            test_case.description
        );
        let budget = planned.ok().flatten();
        let duckdb = budget.and_then(|budget| budget.duckdb).map(|share| {
            (share.bytes, share.memory_limit, share.threads, share.commit_bytes)
        });
        assert_eq!(duckdb, test_case.expected_duckdb, "{}", test_case.description);
        let tables = budget.map_or((0, 0), |budget| (budget.table_share, budget.tables_at_once));
        assert_eq!(tables, test_case.expected_tables, "{}", test_case.description);
    }
}
