use crate::project::models::TableChecks;
use crate::publish_checks::main::check_column_problems::check_column_problems;
use crate::publish_checks::tests::test_types::CheckColumnProblemsTestCase;

#[test]
fn given_checks_naming_columns_when_matching_the_loaded_columns_then_missing_ones_are_named() {
    let test_cases = [
        CheckColumnProblemsTestCase {
            description: "columns are matched ignoring case, as the table's columns are",
            checks: TableChecks {
                unique: Some(vec!["Id".to_owned()]),
                not_null: vec!["EMAIL".to_owned()],
                ..TableChecks::default()
            },
            loaded: &["id", "email"],
            expected_problems: &[],
        },
        CheckColumnProblemsTestCase {
            description: "a column the table does not load is named under its check",
            checks: TableChecks {
                unique: Some(vec!["id".to_owned(), "region".to_owned()]),
                not_null: vec!["email".to_owned()],
                ..TableChecks::default()
            },
            loaded: &["id"],
            expected_problems: &[
                "checks.unique names region, which the table does not load",
                "checks.not_null names email, which the table does not load",
            ],
        },
    ];
    for test_case in test_cases {
        let loaded: Vec<String> = test_case
            .loaded
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        assert_eq!(
            check_column_problems(&test_case.checks, &loaded),
            test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
}
