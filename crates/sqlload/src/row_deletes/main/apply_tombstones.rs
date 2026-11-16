//! Mark a merge table's rows by its loaded tombstones.

use crate::adapters::contract::models::{TombstoneWrite, WriteMode};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::live_columns::live_columns;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::loader::errors::LoaderError;
use crate::loader::models::TablePlan;
use crate::number_text::main::thousands::thousands;
use crate::project::models::Deletes;

/// After `planned`'s merge, mark its rows whose key a loaded tombstone (`deletes_from`) holds, deleted at or after the row's cursor (a coalesced cursor's first non-NULL column): every tombstone kept so far, so a row loaded after its tombstone is still marked, and marking again changes nothing, a soft mark being the tombstones' own time. Returns what it did for the table's line, `(tombstones: 3 rows soft-deleted)`; `None` without a tombstone table loaded yet.
pub async fn apply_tombstones(
    planned: &TablePlan,
    destination: &Destination,
) -> Result<Option<String>, LoaderError> {
    let Some(deletes) = planned.spec.deletes.as_ref() else {
        return Ok(None);
    };
    let from = deletes.from.as_ref().and_then(|from| from.loaded.as_ref());
    let (
        Some((tombstones, deleted_at)),
        Some(cursor),
        WriteMode::Merge {
            unique_key,
            not_null_keys,
            ..
        },
    ) = (from, planned.spec.cursor.as_ref(), &planned.mode)
    else {
        return Ok(None);
    };
    if live_columns(destination, tombstones).await?.is_none() {
        return Ok(None);
    }
    let write = TombstoneWrite {
        tombstones: tombstones.clone(),
        deleted_at: deleted_at.clone(),
        cursor: cursor.columns().map(str::to_owned).collect(),
        unique_key: unique_key.clone(),
        not_null_keys: not_null_keys.clone(),
        soft: deletes.apply == Deletes::Soft,
    };
    let [count, mark] =
        sql_adapter(planned.spec.warehouse).render_tombstones(&planned.spec.destination, &write);
    let counted = text_values(destination, count).await?;
    let marked: u64 = match counted.first().map(|value| value.trim().parse::<u64>()) {
        Some(Ok(marked)) => marked,
        _ => {
            return Err(LoaderError::Invalid(format!(
                "counting the rows the tombstones of {} mark came back as {counted:?}, not a number",
                tombstones.table
            )));
        }
    };
    execute(destination, vec![mark]).await?;
    let rows = match marked {
        1 => "1 row".to_owned(),
        _ => format!("{} rows", thousands(marked)),
    };
    let action = match write.soft {
        true => "soft-deleted",
        false => "deleted",
    };
    Ok(Some(format!("(tombstones: {rows} {action})")))
}
