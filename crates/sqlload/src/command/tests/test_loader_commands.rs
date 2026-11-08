use crate::command::tests::helpers::{debug_without_host, run_without_host};
use crate::command::tests::test_types::{LoaderCommandTestCase, PlanFailureTestCase};

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
fn given_a_files_source_into_a_files_target_when_planning_then_a_target_folder_overlapping_the_source_folder_is_blocked()
-> Result<(), String> {
    let root = tempfile::tempdir().map_err(|error| error.to_string())?;
    let day = root.path().join("data/orders/day=2026-11-01");
    std::fs::create_dir_all(&day).map_err(|error| error.to_string())?;
    std::fs::write(day.join("a.csv"), "id\n1\n").map_err(|error| error.to_string())?;
    let test_cases = [
        ("data/{table}/", "which overlaps the folder it reads from"),
        ("out/{table}/", ""),
    ];
    for (target_path, expected_problem) in test_cases {
        let project = format!(
            "[connections.lake]\nkind = \"filesystem\"\nroot = \"{}\"\n[targets.lake]\nconnection = \"lake\"\npath = \"{target_path}\"\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"lake\"\npath = \"data/orders/\"\nformat = \"csv\"\npartition_columns = {{ day = \"date\" }}\n",
            root.path().display()
        );
        let output = run_without_host(&project, &["plan"]);
        assert!(
            output.contains(expected_problem)
                && output.ends_with(match expected_problem.is_empty() {
                    true => "exit 0",
                    false => "exit 1",
                }),
            "{target_path}: {output}"
        );
    }
    Ok(())
}
