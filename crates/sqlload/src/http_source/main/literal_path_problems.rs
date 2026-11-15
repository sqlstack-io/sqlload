//! Check the `sl.http` paths a Python table writes as literals with load's own checker, before any request.

use crate::http_source::_helpers::job::key_path_problem;
use crate::http_source::models::LiteralPaths;
use crate::row_batches::main::projection::projection;
use crate::row_batches::models::FieldSpec;

/// Why load would refuse each of `paths`, in load's words: `fields["sku"]: field path "$.lines[*].sku" uses the wildcard [*], ...`, or `key_path ...`.
pub fn literal_path_problems(paths: &LiteralPaths) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    for (name, path) in &paths.fields {
        let field = FieldSpec {
            name: name.clone(),
            path: path.clone(),
            required: false,
            parse: None,
            on_error: Default::default(),
            from: Default::default(),
        };
        problems.extend(projection(Some(&[field]), None).err());
    }
    problems.extend(
        paths
            .key_paths
            .iter()
            .filter_map(|path| key_path_problem(path)),
    );
    problems
}
