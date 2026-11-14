use crate::command::tests::helpers::{debug_without_host, run_without_host};
use crate::command::tests::test_types::{
    ExitCodeTestCase, FolderOverlapTestCase, LoaderCommandTestCase, PlanFailureTestCase,
};

const TARGET: &str = "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n";
const NO_LOAD_METHOD: &str = "[connections.wh]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"D\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n";
const REFUSING_TARGET: &str = "[connections.pg]\nkind = \"postgres\"\nhost = \"127.0.0.1\"\nport = 1\nuser = \"loader\"\npassword = \"p\"\ndatabase = \"app\"\nsslmode = \"disable\"\n[targets.dev]\nconnection = \"pg\"\nloader_schema = \"raw\"\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"lake\"\npath = \"orders/\"\nformat = \"csv\"\nwrite_strategy = \"append\"\n[connections.lake]\nkind = \"filesystem\"\nroot = '{root}'\n";
const UNREACHABLE_SOURCE: &str = "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n[connections.app]\nkind = \"postgres\"\nhost = \"127.0.0.1\"\nport = 1\nuser = \"u\"\npassword = \"p\"\ndatabase = \"app\"\nsslmode = \"disable\"\n[sources.app]\nkind = \"postgres\"\nconnection = \"app\"\ndestination = \"dev\"\ninclude = [\"public.*\"]\n";
/// `UNREACHABLE_SOURCE` with its password read from `${ENV:PATH}`, which is set on every OS (`HOME` is not, on Windows).
const UNREACHABLE_FROM_ENV: &str = "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n[connections.app]\nkind = \"postgres\"\nhost = \"127.0.0.1\"\nport = 1\nuser = \"u\"\npassword = \"${ENV:PATH}\"\ndatabase = \"app\"\nsslmode = \"disable\"\n[sources.app]\nkind = \"postgres\"\nconnection = \"app\"\ndestination = \"dev\"\ninclude = [\"public.*\"]\n";

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
            description: "--json on apply: a project that fails to plan is one JSON error, as for plan",
            project: NO_LOAD_METHOD,
            argv: &["apply", "--json"],
            expected_output: "{\"error\":{\"code\":\"config\",\"help\":\"add one of these to [connections.wh]:",
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
fn given_a_files_source_into_a_files_target_when_planning_then_an_overlapping_folder_or_single_file_is_blocked()
-> Result<(), String> {
    let root = tempfile::tempdir().map_err(|error| error.to_string())?;
    let day = root.path().join("data/orders/day=2026-11-01");
    std::fs::create_dir_all(&day).map_err(|error| error.to_string())?;
    std::fs::write(day.join("a.csv"), "id\n1\n").map_err(|error| error.to_string())?;
    let test_cases = [
        FolderOverlapTestCase {
            description: "a target folder that is the source folder is blocked",
            target_path: "data/{table}/",
            single_file: false,
            expected_problem: "which overlaps the folder it reads from",
            expected_exit: "exit 1",
        },
        FolderOverlapTestCase {
            description: "a target folder beside the source folder plans",
            target_path: "out/{table}/",
            single_file: false,
            expected_problem: "",
            expected_exit: "exit 0",
        },
        FolderOverlapTestCase {
            description: "single_file has no load id to record the source files against",
            target_path: "out/{table}/",
            single_file: true,
            expected_problem: "single_file can't record which source files",
            expected_exit: "exit 1",
        },
    ];
    for test_case in test_cases {
        let project = format!(
            "[connections.lake]\nkind = \"filesystem\"\nroot = '{}'\n[targets.lake]\nconnection = \"lake\"\npath = \"{}\"\nsingle_file = {}\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"lake\"\npath = \"data/orders/\"\nformat = \"csv\"\nwrite_strategy = \"replace\"\npartition_columns = {{ day = \"date\" }}\n",
            root.path().display(),
            test_case.target_path,
            test_case.single_file
        );
        let output = run_without_host(&project, &["plan"]);
        let words = output
            .split_whitespace()
            .filter(|word| *word != "│")
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            words.contains(test_case.expected_problem) && output.ends_with(test_case.expected_exit),
            "{}: {output}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_failure_a_rerun_may_pass_when_loading_then_it_exits_75_and_says_so_else_1()
-> Result<(), String> {
    let root = tempfile::tempdir().map_err(|error| error.to_string())?;
    std::fs::create_dir_all(root.path().join("orders")).map_err(|error| error.to_string())?;
    std::fs::write(root.path().join("orders/a.csv"), "id\n1\n")
        .map_err(|error| error.to_string())?;
    let test_cases = [
        ExitCodeTestCase {
            description: "--json: a target that refuses the connection is retryable",
            project: REFUSING_TARGET,
            argv: &["load", "--json"],
            expected_output: "\"retryable\":true",
            expected_exit: "exit 75",
        },
        ExitCodeTestCase {
            description: "text: the same failure exits 75 too",
            project: REFUSING_TARGET,
            argv: &["load"],
            expected_output: "cannot connect to Postgres at",
            expected_exit: "exit 75",
        },
        ExitCodeTestCase {
            description: "--json: a source that refuses the connection is a retryable plan error",
            project: UNREACHABLE_SOURCE,
            argv: &["load", "--json"],
            expected_output: "\"retryable\": true",
            expected_exit: "exit 75",
        },
        ExitCodeTestCase {
            description: "text: plan exits 75 for the same source",
            project: UNREACHABLE_SOURCE,
            argv: &["plan"],
            expected_output: "cannot connect to Postgres at 127.0.0.1:1",
            expected_exit: "exit 75",
        },
        ExitCodeTestCase {
            description: "text: the fix names the variable the connection's settings read",
            project: UNREACHABLE_FROM_ENV,
            argv: &["plan"],
            expected_output: "fix     check PATH (connection app); sqlload debug tests every connection",
            expected_exit: "exit 75",
        },
        ExitCodeTestCase {
            description: "--json: an unreachable source beside a plan error that is not retryable exits 1",
            project: "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n[connections.app]\nkind = \"postgres\"\nhost = \"127.0.0.1\"\nport = 1\nuser = \"u\"\npassword = \"p\"\ndatabase = \"app\"\nsslmode = \"disable\"\n[sources.app]\nkind = \"postgres\"\nconnection = \"app\"\ndestination = \"dev\"\ninclude = [\"public.*\"]\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"lake\"\npath = \"missing/\"\nformat = \"csv\"\nwrite_strategy = \"append\"\ndestination = \"dev\"\n[connections.lake]\nkind = \"filesystem\"\nroot = '{root}'\n",
            argv: &["load", "--json"],
            expected_output: "\"retryable\": false",
            expected_exit: "exit 1",
        },
        ExitCodeTestCase {
            description: "--json: a configuration that does not parse is not retryable",
            project: NO_LOAD_METHOD,
            argv: &["load", "--json"],
            expected_output: "\"retryable\":false",
            expected_exit: "exit 1",
        },
        ExitCodeTestCase {
            description: "--json: a target that cannot open is not retryable",
            project: "[connections.wh]\nkind = \"duckdb\"\ndatabase = \"/no/such/folder/wh.duckdb\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n",
            argv: &["load", "--json"],
            expected_output: "\"retryable\":false",
            expected_exit: "exit 1",
        },
    ];
    for test_case in test_cases {
        let project = test_case
            .project
            .replace("{root}", &root.path().display().to_string());
        let output = run_without_host(&project, test_case.argv);
        assert!(
            output.contains(test_case.expected_output) && output.ends_with(test_case.expected_exit),
            "{}: {output}",
            test_case.description
        );
    }
    Ok(())
}
