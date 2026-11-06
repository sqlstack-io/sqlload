use crate::copy::main::copy_project::copy_project;
use crate::copy::models::CopyRequest;

pub(crate) const PROJECT: &str = "[connections.app]\nkind = \"postgres\"\nhost = \"db\"\nuser = \"loader\"\npassword = \"${ENV:APP_PASSWORD}\"\ndatabase = \"shop\"\n";

/// The project's lines that carry a decision, then each variable as `NAME=value`, then each warning; or the error.
pub(crate) fn outcome(source: &str, destination: &str, project: &str) -> Vec<String> {
    let request = CopyRequest {
        source: source.to_owned(),
        destination: destination.to_owned(),
        ..CopyRequest::default()
    };
    copy_project(&request, project).map_or_else(
        |error| vec![error],
        |copy| {
            let lines = copy.text.lines().filter(|line| {
                [
                    "password",
                    "path",
                    "include",
                    "name",
                    "root",
                    "bucket",
                    "loader_schema",
                    "table_name",
                ]
                .iter()
                .any(|key| line.starts_with(&format!("{key} =")))
            });
            let variables = copy
                .variables
                .iter()
                .map(|(name, value)| format!("{name}={value}"));
            lines
                .map(str::to_owned)
                .chain(variables)
                .chain(copy.warnings)
                .collect()
        },
    )
}
