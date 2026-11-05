use crate::command::tests::helpers::{debug_without_host, run_without_host};
use crate::command::tests::test_types::{LoaderCommandTestCase, PlanFailureTestCase};

const TARGET: &str = "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n";

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
fn given_a_snowflake_connection_without_load_method_when_planning_then_the_plan_fails_naming_the_fix()
 {
    let project = "[connections.wh]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"D\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n";
    let test_cases = [
        PlanFailureTestCase {
            description: "text: an Errors section with the reason and the fix, and no error: prefix",
            argv: &["plan"],
            expected_output: "Errors (1)\n└── connection wh\n    ├── reason  load_method is required: how should data get into Snowflake?\n    └── fix     add one of these to [connections.wh]:\n                load_method = { type = \"external_stage\", url = \"s3://...\", storage_integration = \"...\" }   (fastest)\n                load_method = { type = \"internal_user_stage\" }   (no setup)\n\n✗ Plan failed with errors",
        },
        PlanFailureTestCase {
            description: "--json: sqlbuild's error shape",
            argv: &["plan", "--json"],
            expected_output: "{\"error\":{\"code\":\"config\",\"help\":null,\"message\":\"invalid project:\\n  - connection wh: load_method is required",
        },
    ];
    for test_case in test_cases {
        let output = run_without_host(project, test_case.argv);
        assert!(
            output.starts_with(test_case.expected_output),
            "{}: {output}",
            test_case.description
        );
    }
}
