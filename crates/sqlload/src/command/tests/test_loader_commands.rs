use crate::column_types::models::Warehouse;
use crate::command::_helpers::loader_commands::unloadable_problem;
use crate::command::tests::helpers::{debug_without_host, run_without_host};
use crate::command::tests::test_types::{
    LoaderCommandTestCase, PlanFailureTestCase, UnloadableTestCase,
};

const TARGET: &str = "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n";
const NO_LOAD_METHOD: &str = "[connections.wh]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"D\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n";

#[test]
fn given_the_standalone_executable_when_a_project_has_python_paths_then_it_says_to_install_the_python_package()
 {
    let test_cases = [
        LoaderCommandTestCase {
            description: "python_paths need the Python package",
            project: "python_paths = [\"sources\"]\n[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n",
            expected_error: "install it (for example `uv tool install sqlload`)",
        },
        LoaderCommandTestCase {
            description: "a project without python_paths runs",
            project: TARGET,
            expected_error: "",
        },
    ];
    for test_case in test_cases {
        let error = debug_without_host(test_case.project);
        assert!(
            error.contains(test_case.expected_error)
                && error.is_empty() == test_case.expected_error.is_empty(),
            "{}: {error}",
            test_case.description
        );
    }
}

#[test]
fn given_a_plan_that_fails_when_planning_then_it_prints_the_errors_or_one_json_error_and_exits_1() {
    let test_cases = [
        PlanFailureTestCase {
            description: "text: an Errors section with the reason and the fix, and no error: prefix",
            project: NO_LOAD_METHOD,
            argv: &["plan"],
            expected_output: "Errors (1)\n└── connection wh\n    ├── reason  load_method is required: how should data get into Snowflake?\n    └── fix     add one of these to [connections.wh]:\n                load_method = { type = \"external_stage\", url = \"s3://...\", storage_integration = \"...\" }   (fastest)\n                load_method = { type = \"internal_user_stage\" }   (no setup)\n\n✗ Plan failed with errors",
            expected_exit: "exit 1",
        },
        PlanFailureTestCase {
            description: "--json: sqlbuild's error shape, the reason in message and the fix in help",
            project: NO_LOAD_METHOD,
            argv: &["plan", "--json"],
            expected_output: "{\"error\":{\"code\":\"config\",\"help\":\"add one of these to [connections.wh]:\\nload_method = { type = \\\"external_stage\\\"",
            expected_exit: "exit 1",
        },
        PlanFailureTestCase {
            description: "--json: a failure after the project parses is a JSON error too, never bare text",
            project: "[connections.wh]\nkind = \"duckdb\"\ndatabase = \"/no/such/folder/wh.duckdb\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n",
            argv: &["plan", "--json"],
            expected_output: "{\"error\":{\"code\":\"plan\",\"help\":null,\"message\":\"",
            expected_exit: "exit 1",
        },
        PlanFailureTestCase {
            description: "--json on apply is refused before the project is read",
            project: NO_LOAD_METHOD,
            argv: &["apply", "--json"],
            expected_output: "--json applies to plan only",
            expected_exit: "exit 1",
        },
        PlanFailureTestCase {
            description: "a plan with nothing selected is complete and exits 0",
            project: TARGET,
            argv: &["plan"],
            expected_output: "Plan ready  0 selected",
            expected_exit: "exit 0",
        },
    ];
    for test_case in test_cases {
        let output = run_without_host(test_case.project, test_case.argv);
        assert!(
            output.starts_with(test_case.expected_output)
                && output.ends_with(test_case.expected_exit),
            "{}: {output}",
            test_case.description
        );
    }
}

#[test]
fn given_a_load_plan_when_a_table_targets_a_warehouse_taking_no_rows_yet_then_load_refuses_and_names_it()
 {
    let test_cases = [
        UnloadableTestCase {
            description: "a BigQuery table is refused, naming the warehouse and its target",
            tables: &[(Warehouse::BigQuery, "dev")],
            expected_problem: Some(
                "loading rows into BigQuery (target dev) is not available yet, so nothing was applied or loaded. Fix: leave those tables out with --exclude; plan, apply and contract diff work against it",
            ),
        },
        UnloadableTestCase {
            description: "warehouses that load rows are not refused",
            tables: &[
                (Warehouse::Snowflake, "dev"),
                (Warehouse::Databricks, "lake"),
                (Warehouse::Parquet, "files"),
            ],
            expected_problem: None,
        },
        UnloadableTestCase {
            description: "a mixed plan is refused whole, each BigQuery target named once",
            tables: &[
                (Warehouse::Snowflake, "dev"),
                (Warehouse::BigQuery, "bq"),
                (Warehouse::BigQuery, "bq"),
                (Warehouse::BigQuery, "analytics"),
            ],
            expected_problem: Some(
                "loading rows into BigQuery (target analytics), BigQuery (target bq) is not available yet, so nothing was applied or loaded. Fix: leave those tables out with --exclude; plan, apply and contract diff work against it",
            ),
        },
        UnloadableTestCase {
            description: "a plan with no unblocked table is not refused",
            tables: &[],
            expected_problem: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            unloadable_problem(
                test_case
                    .tables
                    .iter()
                    .map(|(warehouse, target)| (*warehouse, *target))
            ),
            test_case.expected_problem.map(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
