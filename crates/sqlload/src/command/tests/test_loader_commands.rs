use crate::command::tests::helpers::debug_without_host;
use crate::command::tests::test_types::LoaderCommandTestCase;

const TARGET: &str = "[connections.wh]\nkind = \"duckdb\"\npath = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n";

#[test]
fn given_the_standalone_executable_when_a_project_has_python_paths_then_it_says_to_install_the_python_package()
 {
    let test_cases = [
        LoaderCommandTestCase {
            description: "python_paths need the Python package",
            project: "python_paths = [\"sources\"]\n[connections.wh]\nkind = \"duckdb\"\npath = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n",
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
