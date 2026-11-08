//! Choose where a load that rewrites the whole table writes it first.

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::_helpers::layout::swap::{local_folder, staged_stem};
use crate::store_paths::models::Location;

/// The hidden folder beside table folder `live` that a `replace` or `--reload` of a local directory writes the table into, to swap it in at commit; `None` for other write modes, other stores and a table at the directory's root.
pub fn staged_folder(live: &Location, mode: &WriteMode, load_id: &str) -> Option<Location> {
    let rewrites = matches!(mode, WriteMode::Replace | WriteMode::Recreate);
    let (_, parent, name) = local_folder(live).filter(|_| rewrites)?;
    Some(Location {
        store: live.store.clone(),
        prefix: format!("{parent}{}{load_id}/", staged_stem(name)),
    })
}
