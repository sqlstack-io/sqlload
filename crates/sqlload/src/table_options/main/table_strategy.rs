//! A table's write strategy and the cursor and keys it reads and replaces by.

use crate::loader::models::{CursorSpec, SettingOrigin};
use crate::project::models::{TableOverride, WriteStrategy};
use crate::table_options::_helpers::settings::{cdc_spec, cursor};
use crate::table_options::main::layered::layered;
use crate::table_options::models::{OptionContext, TableStrategy};

/// The write strategy `overrides` (the table's settings, over its source's in `context`) loads with, with its cursor and `replace_key`; or the table's cursor when neither it nor its source sets one, which the caller reports.
pub fn table_strategy(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
) -> Result<TableStrategy, Option<Box<CursorSpec>>> {
    let settings = &overrides.settings;
    let defaults = &context.source.defaults;
    let cursor = cursor(settings, defaults);
    let deletes = layered(&settings.deletes, &defaults.deletes);
    let cdc = cdc_spec(context, deletes.map(|(value, _)| value));
    let (strategy, strategy_origin) = match (
        layered(&settings.write_strategy, &defaults.write_strategy),
        &cdc,
    ) {
        (Some(set), _) => set,
        (None, Some(_)) => (WriteStrategy::Append, SettingOrigin::Source),
        (None, None) => return Err(cursor.map(Box::new)),
    };
    let merges = strategy == WriteStrategy::Merge && cdc.is_none();
    let replace_key: Vec<String> = layered(&settings.replace_key, &defaults.replace_key)
        .filter(|(_, origin)| {
            *origin == SettingOrigin::Table
                || strategy == WriteStrategy::DeleteInsert
                || strategy_origin != SettingOrigin::Table
        })
        .map(|(key, _)| key)
        .unwrap_or_default();
    let keyed = strategy == WriteStrategy::DeleteInsert && !replace_key.is_empty();
    Ok(TableStrategy {
        cursor,
        cdc,
        strategy,
        strategy_origin,
        merges,
        replace_key,
        keyed,
    })
}
