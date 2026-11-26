use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::cdc::constants::{CDC_FILES_PROBLEM, CDC_TARGETS_PROBLEM};
use crate::files_source::models::FilesOrder;
use crate::loader::models::SettingOrigin;
use crate::project::models::{
    Deletes, MaskMethod, NullKeys, OnDuplicateKeys, OnOverlappingFiles, SourceKind, TableChecks,
    TableOverride, WriteStrategy,
};
use crate::publish_checks::main::check_problems::check_problems;
use crate::row_batches::models::Unnest;
use crate::row_deletes::main::deletes_problems::deletes_problems;
use crate::table_options::_helpers::nesting::nesting;
use crate::table_options::_helpers::rules::{
    after_load_problems, cursor_problems, dedupe_settings, overlapping_files_problem,
    replace_key_problems, strategy_problems,
};
use crate::table_options::main::layered::layered;
use crate::table_options::models::{Nesting, OptionContext, TableStrategy};

/// The strategy the table loads with (a captured table's is `append`) and the problems with it, its keys and its cursor: a `postgres_cdc` table on a target that cannot take its changes, or the strategy's own rules with `unique_key`, `replace_key` and the cursor.
pub(crate) fn strategy_checks(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
    (chosen, unique_key): (&TableStrategy, &[String]),
) -> (WriteStrategy, Vec<String>) {
    let cursor = chosen.cursor.as_ref();
    let adapter = sql_adapter(context.target.warehouse);
    let (strategy, mut problems) = match &chosen.cdc {
        Some(cdc) if !adapter.captures_changes(cdc.apply) => {
            let problem = match adapter.runs_sql() {
                true => CDC_TARGETS_PROBLEM,
                false => CDC_FILES_PROBLEM,
            };
            (WriteStrategy::Append, vec![problem.to_owned()])
        }
        Some(_) => (WriteStrategy::Append, Vec::new()),
        None => (
            chosen.strategy,
            strategy_problems(chosen.strategy, (unique_key, &chosen.replace_key), cursor),
        ),
    };
    problems.extend(replace_key_problems(context, cursor, chosen.keyed));
    let settings = (&overrides.settings, &context.source.defaults);
    problems.extend(cursor_problems(context, settings, strategy, cursor));
    (strategy, problems)
}

/// The table's `on_duplicate_keys` and `newest_by` (its own, else its source's on a merge, the one it sets winning over the other the source sets), and the problems with them.
pub(crate) fn dedupe_options(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
    (strategy, merges, unique_key): (WriteStrategy, bool, &[String]),
) -> (
    Option<(OnDuplicateKeys, SettingOrigin)>,
    Vec<String>,
    Vec<String>,
) {
    let (settings, defaults) = (&overrides.settings, &context.source.defaults);
    let table_sorts = settings.newest_by.is_some();
    let on_duplicate_keys = layered(&settings.on_duplicate_keys, &defaults.on_duplicate_keys)
        .filter(|(_, origin)| *origin == SettingOrigin::Table || (merges && !table_sorts));
    let table_fails = on_duplicate_keys == Some((OnDuplicateKeys::Fail, SettingOrigin::Table));
    let newest_by = layered(&settings.newest_by, &defaults.newest_by)
        .filter(|(_, origin)| *origin == SettingOrigin::Table || (merges && !table_fails))
        .map(|(value, _)| value);
    let (newest_by, problems) = dedupe_settings(
        strategy,
        unique_key,
        (
            on_duplicate_keys.map(|(value, _)| value),
            newest_by.as_deref(),
        ),
        overrides,
    );
    (on_duplicate_keys, newest_by, problems)
}

/// The table's `on_overlapping_files` (its source's only when the table takes the source's strategy, or replaces by key), and the problems with the settings of a files source: `order`, `on_overlapping_files` and `after_load`.
pub(crate) fn files_options(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
    (chosen, strategy): (&TableStrategy, WriteStrategy),
) -> (Option<OnOverlappingFiles>, Vec<String>) {
    let (settings, defaults) = (&overrides.settings, &context.source.defaults);
    let keyed = chosen.keyed;
    let mut problems: Vec<String> = Vec::new();
    let ordered = strategy == WriteStrategy::Merge
        || (strategy == WriteStrategy::DeleteInsert && !chosen.replace_key.is_empty());
    if context.source.order == FilesOrder::Path && !ordered {
        problems.push(format!("order = \"path\" orders the files a merge or a delete_insert by replace_key applies in turn; {strategy} loads them without an order, so remove it"));
    }
    let own_strategy = chosen.strategy_origin == SettingOrigin::Table && !keyed;
    let overlapping = layered(
        &settings.on_overlapping_files,
        &defaults.on_overlapping_files,
    )
    .filter(|(_, origin)| !own_strategy || *origin == SettingOrigin::Table)
    .map(|(value, _)| value);
    let files = context.source.kind == SourceKind::Files;
    problems.extend(overlapping.and_then(|_| overlapping_files_problem(files, strategy, keyed)));
    problems.extend(after_load_problems(context, (strategy, keyed)));
    (overlapping, problems)
}

/// The table's `null_keys` (its own, else its source's on a merge or a `delete_insert` by key), and the problems with it and with `mask`: `null_keys` on a table matching no key, a masked cursor or key column, and an `hmac` mask without the source's `mask_key`.
pub(crate) fn key_options(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
    (chosen, strategy, unique_key): (&TableStrategy, WriteStrategy, &[String]),
) -> (Option<NullKeys>, Vec<String>) {
    let (settings, defaults) = (&overrides.settings, &context.source.defaults);
    let (keyed, merges) = (chosen.keyed, chosen.merges);
    let mut problems: Vec<String> = Vec::new();
    let null_keys = layered(&settings.null_keys, &defaults.null_keys)
        .filter(|(_, origin)| *origin == SettingOrigin::Table || merges || keyed)
        .map(|(value, _)| value);
    if null_keys.is_some() && strategy != WriteStrategy::Merge && !keyed {
        problems.push(format!("null_keys is for write_strategy = \"merge\", which matches rows on unique_key, and \"delete_insert\" with a replace_key; {strategy} keeps every row"));
    }
    let uses_key = strategy == WriteStrategy::Merge;
    problems.extend(overrides.mask.keys().filter_map(|masked| {
        let is_cursor = (chosen.cursor.as_ref()).is_some_and(|cursor| cursor.names(masked));
        let is_key =
            (uses_key && unique_key.contains(masked)) || chosen.replace_key.contains(masked);
        (is_cursor || is_key).then(|| {
            format!("mask {masked}: a masked column cannot be the cursor_column or part of the unique_key or replace_key")
        })
    }));
    let hmac = (overrides.mask.values()).any(|method| *method == MaskMethod::Hmac);
    if context.source.mask_key.is_none() && hmac {
        problems.push("mask uses \"hmac\" but the source sets no mask_key (for example mask_key = \"${ENV:MASK_KEY}\")".to_owned());
    }
    (null_keys, problems)
}

/// The table's `deletes` (its own, else its source's on a merge or a captured table) and `checks`, and the checks its strategy and target cannot run.
pub(crate) fn deletes_and_checks(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
    (chosen, strategy): (&TableStrategy, WriteStrategy),
) -> (Option<Deletes>, Option<TableChecks>, Vec<String>) {
    let (settings, defaults) = (&overrides.settings, &context.source.defaults);
    let captured = chosen.cdc.is_some();
    let deletes = layered(&settings.deletes, &defaults.deletes)
        .filter(|(_, origin)| *origin == SettingOrigin::Table || chosen.merges || captured)
        .map(|(value, _)| value);
    let checks = layered(&settings.checks, &defaults.checks).map(|(value, _)| value);
    let files_target = context.target.path.is_some();
    let problems: Vec<String> = checks
        .iter()
        .flat_map(|checks| check_problems(checks, (strategy, files_target, deletes.is_some())))
        .collect();
    (deletes, checks, problems)
}

/// The table's nesting, and the problems with it and with what it carries: nesting settings, `deletes` the table cannot apply, and child tables a `delete_insert` by key cannot carry.
pub(crate) fn nesting_options(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
    (chosen, strategy, deletes): (&TableStrategy, WriteStrategy, Option<Deletes>),
) -> (Nesting, Vec<String>) {
    let (nesting, mut problems) = nesting(context, overrides, strategy);
    let unnests = !matches!(nesting.unnest, Unnest::None);
    let kind = (strategy, context.source.kind);
    problems.extend(deletes_problems(
        deletes,
        kind,
        context.target.warehouse,
        unnests,
    ));
    if chosen.keyed && unnests {
        problems.push("replace_key does not carry child tables yet, whose rows hang off the replaced rows by their ids. Fix: drop unnest, or use write_strategy = \"merge\" on a unique_key".to_owned());
    }
    (nesting, problems)
}
