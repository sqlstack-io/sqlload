use globset::Glob;

use crate::adapters::files::constants::{CURSOR_PLACEHOLDER_START, TABLE_PLACEHOLDER};
use crate::files_source::models::SourceFormat;
use crate::load_naming::models::Compat;
use crate::loader::constants::UNNEST_ALL;
use crate::project::constants::DEFAULT_MAX_DEPTH;
use crate::project::main::table_flatten::table_flatten;
use crate::project::models::{PathsSetting, SourceKind, TableOverride, WriteStrategy};
use crate::row_batches::models::Unnest;
use crate::table_options::main::layered::layered;
use crate::table_options::main::table_compat::table_compat;
use crate::table_options::main::table_naming::table_naming;
use crate::table_options::models::{Nesting, OptionContext};

/// The table's naming and compat (each its own, its source's, then its target's), whether its rows get an id (always under `compat = "dlt"`), its `unnest` (under `compat = "dlt"`, `"all"` for JSON and NDJSON files on a warehouse), `max_depth` and `flatten` (see [`table_flatten`]), and the problems with them: `unnest` names list fields by dotted paths whose ancestors it also names, no deeper than `max_depth`, that no column setting names, on a table whose writes can carry child tables.
pub(crate) fn nesting(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
    strategy: WriteStrategy,
) -> (Nesting, Vec<String>) {
    let settings = &overrides.settings;
    let defaults = &context.source.defaults;
    let naming = table_naming((settings, defaults), context.target.naming);
    let compat = table_compat((settings, defaults), context.target.compat);
    let row_id = layered(&settings.row_id, &defaults.row_id).map(|(value, _)| value);
    let max_depth = overrides.max_depth.unwrap_or(DEFAULT_MAX_DEPTH);
    let mut problems: Vec<String> = Vec::new();
    if compat == Compat::Dlt && row_id == Some(false) {
        problems.push(
            "row_id = false: compat = \"dlt\" gives every row its _dlt_id; drop row_id".to_owned(),
        );
    }
    let json_files = context.source.kind == SourceKind::Files
        && matches!(
            context.source.format,
            SourceFormat::Json | SourceFormat::Ndjson
        );
    let unnest = match &overrides.unnest {
        None if compat == Compat::Dlt && json_files && context.target.path.is_none() => Unnest::All,
        None => Unnest::None,
        Some(PathsSetting::Paths(paths)) if paths.is_empty() => Unnest::None,
        Some(PathsSetting::Word(word)) if word == UNNEST_ALL => Unnest::All,
        Some(PathsSetting::Word(word)) => {
            problems.push(format!(
                "unnest = {word:?}: list the dotted paths of the list fields to unnest, or write \"{UNNEST_ALL}\""
            ));
            Unnest::None
        }
        Some(PathsSetting::Paths(paths)) => {
            problems.extend(
                paths
                    .iter()
                    .filter_map(|path| path_problem(path, paths, max_depth)),
            );
            problems.extend(column_setting_problems(context, overrides, paths));
            Unnest::Paths(paths.clone())
        }
    };
    let transform = settings.transform.is_some() || defaults.transform.is_some();
    let (flatten, found) = table_flatten(
        (context.source, Some(overrides)),
        (compat, naming, transform),
    );
    problems.extend(found);
    match unnest {
        Unnest::None if overrides.max_depth.is_some() && flatten.is_none() => {
            problems.push(
                "max_depth limits the child tables unnest makes and the objects flatten flattens, and this table sets neither"
                    .to_owned(),
            );
        }
        Unnest::None => {}
        Unnest::All | Unnest::Paths(_) => {
            problems.extend(refused_writes(context, overrides, strategy));
            if row_id == Some(false) {
                problems.push("row_id = false: unnest gives every row an id, which its child rows name as their parent's; drop row_id".to_owned());
            }
        }
    }
    (
        Nesting {
            naming,
            compat,
            row_id: row_id.unwrap_or(compat == Compat::Dlt || unnest != Unnest::None),
            unnest,
            max_depth,
            flatten,
        },
        problems,
    )
}

/// What is wrong with the `unnest` path `path` among `paths`: an empty part, a parent path not also listed, or more parts than `max_depth`.
fn path_problem(path: &str, paths: &[String], max_depth: u32) -> Option<String> {
    let parts: Vec<&str> = path.split('.').collect();
    let parent = path.rsplit_once('.').map(|(parent, _)| parent);
    let listed = parent.is_none_or(|parent| paths.iter().any(|listed| listed == parent));
    match (parts.iter().any(|part| part.trim().is_empty()), listed) {
        (true, _) => Some(format!(
            "unnest {path:?}: a path names a field at each level, joined by \".\""
        )),
        (false, false) => Some(format!(
            "unnest {path:?}: its parent list {:?} must be unnested too; add it",
            parent.unwrap_or_default()
        )),
        (false, true) if parts.len() > max_depth as usize => Some(format!(
            "unnest {path:?}: it is {} levels deep, past max_depth = {max_depth}; raise max_depth or drop it",
            parts.len()
        )),
        (false, true) => None,
    }
}

/// The top-level `unnest` fields a `mask`, `columns` or `exclude_columns` entry also names: unnesting takes the field out of the table, so that setting would silently stop applying.
fn column_setting_problems(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
    paths: &[String],
) -> Vec<String> {
    let exclude: Vec<&String> = context
        .source
        .exclude_columns
        .iter()
        .chain(&overrides.exclude_columns)
        .collect();
    paths
        .iter()
        .filter(|path| !path.contains('.'))
        .filter_map(|field| {
            let excluded = matching_pattern(field, &exclude);
            let setting = match (excluded, overrides.mask.contains_key(field), overrides.columns.contains_key(field)) {
                (Some(pattern), _, _) => format!("exclude_columns {pattern}"),
                (None, true, _) => "mask".to_owned(),
                (None, false, true) => "columns".to_owned(),
                (None, false, false) => return None,
            };
            Some(format!(
                "unnest {field:?}: {setting} also names {field}, which unnesting moves to child tables; drop one of them"
            ))
        })
        .collect()
}

/// The first of `patterns` matching `field`.
fn matching_pattern<'a>(field: &str, patterns: &[&'a String]) -> Option<&'a String> {
    patterns
        .iter()
        .find(|pattern| Glob::new(pattern).is_ok_and(|glob| glob.compile_matcher().is_match(field)))
        .copied()
}

/// Why this table's writes cannot carry child tables yet: captured changes on a files target; on a files target, `"all"` (whose earlier child tables a load cannot find), a merge or `delete_insert` (which would rewrite a child table's files), and a path that would not give each child table a folder of its own (without `{table}`, or partitioned by the cursor, which child rows do not carry).
fn refused_writes(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
    strategy: WriteStrategy,
) -> Vec<String> {
    let template = context.target.path.as_ref().map(|target_path| {
        overrides
            .destination_path
            .as_ref()
            .or(context.source.destination_path.as_ref())
            .unwrap_or(target_path)
    });
    let files = |refused: bool, what: String| (template.is_some() && refused).then_some(what);
    let all = matches!(&overrides.unnest, Some(PathsSetting::Word(word)) if word == UNNEST_ALL);
    let rewrites = matches!(strategy, WriteStrategy::Merge | WriteStrategy::DeleteInsert);
    let files_target = template.is_some();
    let template = template.map(String::as_str).unwrap_or_default();
    [
        (context.source.kind == SourceKind::PostgresCdc && files_target)
            .then(|| "kind = \"postgres_cdc\" on a files target".to_owned()),
        files(all, "unnest = \"all\" on a files target (list the paths)".to_owned()),
        files(
            rewrites,
            format!("write_strategy = \"{strategy}\" on a files target"),
        ),
        files(
            !template.contains(TABLE_PLACEHOLDER),
            format!("a files path without {TABLE_PLACEHOLDER} ({template:?}), as each child table needs its own folder"),
        ),
        files(
            template.contains(CURSOR_PLACEHOLDER_START),
            format!("a files path partitioned by the cursor ({template:?})"),
        ),
    ]
    .into_iter()
    .flatten()
    .map(|what| format!("unnest does not yet write child tables with {what}; drop unnest, and its lists stay JSON columns"))
    .collect()
}
