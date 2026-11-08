use crate::copy::main::copy_project::copy_project;
use crate::copy::models::CopyRequest;

pub(crate) const PROJECT: &str = "[connections.app]\nkind = \"postgres\"\nhost = \"db\"\nuser = \"loader\"\npassword = \"${ENV:APP_PASSWORD}\"\ndatabase = \"shop\"\n";

/// The password every test URI uses; no output may contain it.
const SECRET: &str = "hunter2";

/// Settings keys whose lines the tests compare.
const KEYS: &[&str] = &[
    "password",
    "host",
    "port",
    "user",
    "database",
    "include",
    "name",
    "root",
    "bucket",
    "path",
    "table_name",
    "loader_schema",
    "destination",
    "description",
    "sslmode",
    "sslrootcert",
];

/// The project's lines for `KEYS`, the connections `--print-toml` prints, each variable as `NAME=value` and each warning; or the error. A line `leaked` follows when any of it but the variables holds the test password.
pub(crate) fn outcome(source: &str, destination: &str, project: &str) -> Vec<String> {
    let request = CopyRequest {
        source: source.to_owned(),
        destination: destination.to_owned(),
        ..CopyRequest::default()
    };
    let (lines, shown): (Vec<String>, String) = copy_project(&request, project).map_or_else(
        |error| (vec![error.clone()], error),
        |copy| {
            let settings = copy
                .text
                .lines()
                .filter(|line| KEYS.iter().any(|key| line.starts_with(&format!("{key} ="))));
            let printed = copy
                .printed
                .lines()
                .filter(|line| line.starts_with("[connections."))
                .map(|line| format!("printed {line}"));
            let variables = copy
                .variables
                .iter()
                .map(|(name, value)| format!("{name}={value}"));
            let shown = [
                copy.text.clone(),
                copy.printed.clone(),
                copy.warnings.join("\n"),
            ]
            .join("\n");
            let lines = settings
                .map(str::to_owned)
                .chain(printed)
                .chain(variables)
                .chain(copy.warnings)
                .collect();
            (lines, shown)
        },
    );
    let leaked = shown.contains(SECRET).then(|| "leaked".to_owned());
    lines.into_iter().chain(leaked).collect()
}
