use crate::project::main::is_project::is_project;
use crate::project::tests::helpers::PROJECT;
use crate::project::tests::test_types::IsProjectTestCase;

#[test]
fn given_a_configuration_file_when_detecting_its_format_then_only_loader_projects_match() {
    let test_cases = [
        IsProjectTestCase {
            description: "a file with sources is a project",
            text: PROJECT,
            expected_is_project: true,
        },
        IsProjectTestCase {
            description: "an exporter configuration is not",
            text: "[source]\nkind = \"kafka\"\n",
            expected_is_project: false,
        },
        IsProjectTestCase {
            description: "invalid TOML is not",
            text: "[sources",
            expected_is_project: false,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            is_project(test_case.text),
            test_case.expected_is_project,
            "{}",
            test_case.description
        );
    }
}
