use crate::project::models::{CheckName, TableChecks, WriteStrategy};
use crate::publish_checks::main::check_problems::check_problems;
use crate::publish_checks::tests::test_types::CheckProblemsTestCase;

#[test]
fn given_a_tables_checks_when_validating_them_then_only_runnable_checks_pass() {
    let test_cases = [
        CheckProblemsTestCase {
            description: "every check on replace, with warn naming a set check, is fine",
            checks: TableChecks {
                unique: Some(vec!["id".to_owned()]),
                not_null: vec!["id".to_owned()],
                min_rows: Some(1),
                max_row_drop: Some(20),
                warn: vec![CheckName::MaxRowDrop],
            },
            strategy: WriteStrategy::Replace,
            files_target: false,
            expected_problems: &[],
        },
        CheckProblemsTestCase {
            description: "unique and not_null run on every commit, so merge takes them",
            checks: TableChecks {
                unique: Some(vec!["id".to_owned()]),
                not_null: vec!["id".to_owned()],
                ..TableChecks::default()
            },
            strategy: WriteStrategy::Merge,
            files_target: false,
            expected_problems: &[],
        },
        CheckProblemsTestCase {
            description: "whole-table checks are refused on a strategy that publishes batch by batch, naming each",
            checks: TableChecks {
                min_rows: Some(1),
                max_row_drop: Some(0),
                ..TableChecks::default()
            },
            strategy: WriteStrategy::DeleteInsert,
            files_target: false,
            expected_problems: &[
                "only write_strategy = \"replace\" publishes the whole table in one commit, so only it takes checks.min_rows and checks.max_row_drop; other strategies publish batch by batch, so check those batches with unique and not_null",
            ],
        },
        CheckProblemsTestCase {
            description: "an empty unique list and a warn entry for a check not set are refused",
            checks: TableChecks {
                unique: Some(Vec::new()),
                warn: vec![CheckName::NotNull],
                ..TableChecks::default()
            },
            strategy: WriteStrategy::Append,
            files_target: false,
            expected_problems: &[
                "checks.unique needs at least one column",
                "checks.warn lists not_null, which the table does not check",
            ],
        },
        CheckProblemsTestCase {
            description: "a checks block that sets no check is refused rather than silently changing the route",
            checks: TableChecks::default(),
            strategy: WriteStrategy::Replace,
            files_target: false,
            expected_problems: &["checks sets no check; add one, or remove checks"],
        },
        CheckProblemsTestCase {
            description: "a files target has no staged rows to check",
            checks: TableChecks {
                not_null: vec!["id".to_owned()],
                ..TableChecks::default()
            },
            strategy: WriteStrategy::Replace,
            files_target: true,
            expected_problems: &[
                "checks run on the staged rows of a database target before they are published; a files target has none, so remove checks",
            ],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            check_problems(
                &test_case.checks,
                (test_case.strategy, test_case.files_target)
            ),
            test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
}
