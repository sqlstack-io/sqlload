use crate::column_types::models::Warehouse;
use crate::project::models::{Deletes, SourceKind, WriteStrategy};
use crate::row_deletes::main::deletes_problems::deletes_problems;
use crate::row_deletes::tests::test_types::DeletesProblemsTestCase;

#[test]
fn given_a_tables_deletes_when_validating_it_then_only_merges_of_listed_keys_into_sql_warehouses_pass()
 {
    let test_cases = [
        DeletesProblemsTestCase {
            description: "a merge of a database table into DuckDB takes either kind",
            deletes: Some(Deletes::Hard),
            strategy: WriteStrategy::Merge,
            kind: SourceKind::Odbc,
            warehouse: Warehouse::DuckDb,
            unnests: false,
            expected_problems: &[],
        },
        DeletesProblemsTestCase {
            description: "a captured table's deletes are its changes' to apply",
            deletes: Some(Deletes::Hard),
            strategy: WriteStrategy::Append,
            kind: SourceKind::PostgresCdc,
            warehouse: Warehouse::ClickHouse,
            unnests: true,
            expected_problems: &[],
        },
        DeletesProblemsTestCase {
            description: "an append has no key, a files source lists no keys, ClickHouse mutates later, and child rows would stay",
            deletes: Some(Deletes::Soft),
            strategy: WriteStrategy::Append,
            kind: SourceKind::Files,
            warehouse: Warehouse::ClickHouse,
            unnests: true,
            expected_problems: &[
                "deletes marks the rows whose unique_key the source no longer holds, which needs write_strategy = \"merge\"; append keeps no key to match. Fix: remove deletes, or merge on a unique_key",
                "deletes reads every key of the source table to find the ones it no longer holds, which only database and warehouse sources list; a files source can't. Fix: remove deletes",
                "deletes marks rows of the table in place, which SQLLoad doesn't do in ClickHouse yet. Fix: remove deletes",
                "deletes would leave the child tables' rows of each row it removes or marks. Fix: drop unnest, or remove deletes",
            ],
        },
        DeletesProblemsTestCase {
            description: "files targets change no rows in place",
            deletes: Some(Deletes::Soft),
            strategy: WriteStrategy::Merge,
            kind: SourceKind::Postgres,
            warehouse: Warehouse::Parquet,
            unnests: false,
            expected_problems: &[
                "deletes marks rows of the table in place, which SQLLoad doesn't do in Parquet files yet. Fix: remove deletes",
            ],
        },
    ];
    for test_case in test_cases {
        let problems = deletes_problems(
            test_case.deletes,
            (test_case.strategy, test_case.kind),
            test_case.warehouse,
            test_case.unnests,
        );
        assert_eq!(
            problems, test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
}
