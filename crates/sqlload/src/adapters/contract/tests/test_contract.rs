use crate::adapters::contract::constants::SWAP_SUFFIX;
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::tests::helpers::{accounts, columns, writes};
use crate::adapters::contract::tests::test_types::ContractTestCase;
use crate::adapters::dispatch::main::dialect::_every_warehouse::every_warehouse;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;

#[test]
fn given_any_sql_adapter_when_a_load_commits_then_rows_are_written_once_and_the_table_changes_atomically()
 {
    let test_cases = [
        ContractTestCase {
            description: "append copies the staged rows in",
            mode: WriteMode::Append,
            built: false,
            expected_copies: 1,
        },
        ContractTestCase {
            description: "merge copies the staged rows in",
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
            },
            built: false,
            expected_copies: 1,
        },
        ContractTestCase {
            description: "delete_insert replaces its window from the staged rows",
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-04T09:00:00Z".to_owned()),
                to: Some("2026-11-05T09:00:00Z".to_owned()),
                values: Vec::new(),
            },
            built: false,
            expected_copies: 1,
        },
        ContractTestCase {
            description: "replace from rows staged as text converts them once into a copy it swaps in",
            mode: WriteMode::Replace,
            built: false,
            expected_copies: 1,
        },
        ContractTestCase {
            description: "replace from a build table swaps it in without copying a row",
            mode: WriteMode::Replace,
            built: true,
            expected_copies: 0,
        },
        ContractTestCase {
            description: "--reload from a build table swaps it in without copying a row",
            mode: WriteMode::Recreate,
            built: true,
            expected_copies: 0,
        },
    ];
    let staging = TableName {
        table: "app__accounts__sqlload_staging_load1".to_owned(),
        ..accounts()
    };
    for warehouse in every_warehouse()
        .into_iter()
        .filter(|warehouse| sql_adapter(*warehouse).runs_sql())
    {
        let adapter = sql_adapter(warehouse);
        let target = adapter.render_qualified_name(&accounts());
        let name = adapter.render_identifier(&accounts().table);
        let swap = TableName {
            table: format!("{}{SWAP_SUFFIX}", staging.table),
            ..staging.clone()
        };
        let sources = [
            format!("FROM {}", adapter.render_qualified_name(&staging)),
            format!("FROM {}", adapter.render_qualified_name(&swap)),
        ];
        for test_case in &test_cases {
            let statements = adapter.render_commit(
                &WriteStatementsRequest {
                    table: &accounts(),
                    columns: &columns(),
                    mode: &test_case.mode,
                    hashed: &[],
                    built: test_case.built.then_some(&[]),
                },
                &staging,
                (None, true),
            );
            let copies = statements
                .iter()
                .filter(|statement| !statement.starts_with("DELETE"))
                .filter(|statement| sources.iter().any(|source| statement.contains(source)))
                .count();
            assert_eq!(
                copies, test_case.expected_copies,
                "{warehouse:?}: {}: {statements:#?}",
                test_case.description
            );
            let written: Vec<usize> = statements
                .iter()
                .enumerate()
                .filter(|(_, statement)| writes(statement, (&target, &name)))
                .map(|(index, _)| index)
                .collect();
            let (Some(first), Some(last)) = (written.first(), written.last()) else {
                panic!(
                    "{warehouse:?}: {}: the table is never written: {statements:#?}",
                    test_case.description
                );
            };
            let atomic = first == last
                || (statements[..*first]
                    .iter()
                    .any(|statement| statement == "BEGIN")
                    && statements[*last..]
                        .iter()
                        .any(|statement| statement == "COMMIT")
                    && !statements[*first..*last]
                        .iter()
                        .any(|statement| statement == "COMMIT"));
            assert!(
                atomic,
                "{warehouse:?}: {}: the table's writes are not one transaction: {statements:#?}",
                test_case.description
            );
        }
    }
}
