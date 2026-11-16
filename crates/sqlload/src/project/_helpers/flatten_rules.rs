//! The parts of a table's flatten rule: which objects, which stay whole, and what is wrong with its paths.

use globset::Glob;

use crate::files_source::models::SourceFormat;
use crate::load_naming::models::Compat;
use crate::project::constants::FLATTEN_ALL;
use crate::project::models::{PathsSetting, SourceConfig, SourceKind, TableOverride};
use crate::row_batches::constants::FLATTEN_SEPARATOR;
use crate::row_batches::models::{Flatten, FlattenObjects};

/// Which objects the table's `flatten` names, else its compat's default, and the problems with it: a word other than `"all"`, or a source whose rows hold no objects.
pub(crate) fn flattened_objects(
    (source, overrides): (&SourceConfig, Option<&TableOverride>),
    (compat, transform): (Compat, bool),
) -> (FlattenObjects, Vec<String>) {
    let json_files = source.kind == SourceKind::Files
        && matches!(source.format, SourceFormat::Json | SourceFormat::Ndjson);
    let setting = overrides.and_then(|table| table.flatten.as_ref());
    let mut problems: Vec<String> = Vec::new();
    if setting.is_some() && !(source.kind == SourceKind::Python || json_files || transform) {
        problems.push("flatten applies to Python tables, JSON and NDJSON files, and tables with a transform; nested values from this source stay JSON".to_owned());
    }
    let objects = match setting {
        None if compat == Compat::Dlt && json_files => FlattenObjects::All,
        None => FlattenObjects::None,
        Some(PathsSetting::Word(word)) if word == FLATTEN_ALL => FlattenObjects::All,
        Some(PathsSetting::Word(word)) => {
            problems.push(format!(
                "flatten = {word:?}: list the dotted paths of the objects to flatten, or write \"{FLATTEN_ALL}\""
            ));
            FlattenObjects::None
        }
        Some(PathsSetting::Paths(paths)) if paths.is_empty() => FlattenObjects::None,
        Some(PathsSetting::Paths(paths)) => FlattenObjects::Paths(paths.clone()),
    };
    (objects, problems)
}

/// The objects the table keeps whole (its `json` pins, masks, and `exclude_columns` and `allow_columns` names that are no pattern), and its pins of other types, by name with `.` read as `__`.
pub(crate) fn kept_and_pinned(
    source: &SourceConfig,
    overrides: Option<&TableOverride>,
) -> (Vec<String>, Vec<(String, String)>) {
    let (mut keep, mut pinned): (Vec<String>, Vec<(String, String)>) = (Vec::new(), Vec::new());
    for (name, column_type) in overrides.iter().flat_map(|table| &table.columns) {
        let name = name.replace('.', FLATTEN_SEPARATOR);
        match column_type.trim().eq_ignore_ascii_case("json") {
            true => keep.push(name),
            false => pinned.push((name, column_type.clone())),
        }
    }
    keep.extend(
        overrides
            .iter()
            .flat_map(|table| table.mask.keys().cloned()),
    );
    let listed = [&source.exclude_columns, &source.allow_columns]
        .into_iter()
        .chain(
            overrides
                .into_iter()
                .flat_map(|table| [&table.exclude_columns, &table.allow_columns]),
        )
        .flatten()
        .filter(|entry| !entry.contains(['*', '?', '[', '{']));
    keep.extend(listed.cloned());
    (keep, pinned)
}

/// What is wrong with the `flatten` path `path` among `paths`: an empty part, a parent path listed in neither `flatten` nor the table's `unnest`, or more parts than `max_depth`.
pub(crate) fn path_problem(
    path: &str,
    (paths, overrides): (&[String], &TableOverride),
    max_depth: u32,
) -> Option<String> {
    let parts: Vec<&str> = path.split('.').collect();
    let parent = path.rsplit_once('.').map(|(parent, _)| parent);
    let unnested = |parent: &str| match &overrides.unnest {
        Some(PathsSetting::Paths(unnest)) => unnest.iter().any(|listed| listed == parent),
        Some(PathsSetting::Word(word)) => word == FLATTEN_ALL,
        None => false,
    };
    let listed =
        parent.is_none_or(|parent| paths.iter().any(|listed| listed == parent) || unnested(parent));
    match (parts.iter().any(|part| part.trim().is_empty()), listed) {
        (true, _) => Some(format!(
            "flatten {path:?}: a path names an object at each level, joined by \".\""
        )),
        (false, false) => Some(format!(
            "flatten {path:?}: its parent {:?} must be flattened or unnested too; add it",
            parent.unwrap_or_default()
        )),
        (false, true) if parts.len() > max_depth as usize => Some(format!(
            "flatten {path:?}: it is {} levels deep, past max_depth = {max_depth}; raise max_depth or drop it",
            parts.len()
        )),
        (false, true) => None,
    }
}

/// What makes the listed object at `path` contradict another setting: a pin or mask that keeps it whole, or an `exclude_columns` pattern matching it but not the `<object>__<key>` columns that replace it, so the exclusion would stop applying.
pub(crate) fn setting_problem(
    path: &str,
    (source, overrides): (&SourceConfig, &TableOverride),
    flatten: &Flatten,
) -> Option<String> {
    let parts: Vec<&str> = path.split('.').collect();
    let name = parts.join(FLATTEN_SEPARATOR);
    if let Err(problem) = flatten.flattens(&parts) {
        return Some(problem);
    }
    let excluded = [&source.exclude_columns, &overrides.exclude_columns]
        .into_iter()
        .flatten()
        .find(|pattern| {
            Glob::new(pattern).is_ok_and(|glob| {
                let matcher = glob.compile_matcher();
                matcher.is_match(&name) && !matcher.is_match(format!("{name}{FLATTEN_SEPARATOR}"))
            })
        });
    match (excluded, flatten.keeps(&parts)) {
        (Some(pattern), _) => Some(format!(
            "flatten {path}: exclude_columns {pattern} also names {name}, which flattening replaces with {name}__<key> columns; name those columns instead, or leave {path} unflattened"
        )),
        (None, true) => Some(format!(
            "flatten {path}: a json pin, a mask or an allow_columns entry names {name}, which keeps it whole; leave {path} out of flatten, or drop that setting"
        )),
        (None, false) => None,
    }
}
