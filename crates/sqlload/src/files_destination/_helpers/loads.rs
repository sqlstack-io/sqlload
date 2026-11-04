use crate::files_destination::constants::PART_PREFIX;

/// The load id in a written file's key (`.../part-<load id>-<n>.parquet`), if the file name has that shape.
pub(crate) fn load_id_of(key: &str) -> Option<&str> {
    let name = key.rsplit_once('/').map_or(key, |(_, name)| name);
    let (load_id, _) = name.strip_prefix(PART_PREFIX)?.rsplit_once('-')?;
    (!load_id.is_empty()).then_some(load_id)
}
