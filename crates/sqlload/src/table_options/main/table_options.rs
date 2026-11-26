//! Check a table's options together and read what they come to.

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::project::models::TableOverride;
use crate::table_options::_helpers::options::{
    dedupe_options, deletes_and_checks, files_options, key_options, nesting_options,
    strategy_checks,
};
use crate::table_options::_helpers::settings::{table_layout, table_parses, table_timezone};
use crate::table_options::models::{OptionContext, TableOptions, TableStrategy};

/// What `overrides` (the table's settings, over its source's in `context`) come to with `chosen`'s strategy and `unique_key` (the key a merge matches on), and every problem with how they combine: the strategy with its keys and cursor, the cursor settings, dedupe, a files source's settings, `null_keys` and masks, `checks`, nesting, `deletes`, the target's layout, `parse` and `timezone`. None of them needs the source table, so they are checked at parse as well as when the table is planned.
pub fn table_options(
    context: &OptionContext<'_>,
    overrides: &TableOverride,
    chosen: &TableStrategy,
    unique_key: &[String],
) -> (TableOptions, Vec<String>) {
    let (strategy, mut problems) = strategy_checks(context, overrides, (chosen, unique_key));
    let dedupe = (strategy, chosen.merges, unique_key);
    let (on_duplicate_keys, newest_by, found) = dedupe_options(context, overrides, dedupe);
    problems.extend(found);
    let (overlapping, found) = files_options(context, overrides, (chosen, strategy));
    problems.extend(found);
    let (null_keys, found) = key_options(context, overrides, (chosen, strategy, unique_key));
    problems.extend(found);
    let (deletes, checks, found) = deletes_and_checks(context, overrides, (chosen, strategy));
    problems.extend(found);
    let (nesting, found) = nesting_options(context, overrides, (chosen, strategy, deletes));
    problems.extend(found);
    let layout = table_layout(&overrides.settings, &context.source.defaults);
    problems.extend(sql_adapter(context.target.warehouse).layout_problems(&layout));
    let parses = table_parses(overrides).unwrap_or_else(|found| {
        problems.extend(found);
        Default::default()
    });
    let timezone =
        table_timezone((&overrides.settings, &context.source.defaults)).unwrap_or_else(|problem| {
            problems.push(problem);
            None
        });
    let options = TableOptions {
        strategy,
        on_duplicate_keys,
        newest_by,
        overlapping,
        null_keys,
        deletes,
        checks,
        nesting,
        layout,
        parses,
        timezone,
    };
    (options, problems)
}
