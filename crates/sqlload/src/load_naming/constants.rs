use crate::load_naming::models::MetadataRole;

/// The prefix of every column SQLLoad adds under its own name, whatever the naming.
pub const SQLLOAD_PREFIX: &str = "_sqlload_";

/// The load id column under `naming = "sqlload"`.
pub const SQLLOAD_LOAD_ID: &str = "_sqlload_load_id";

/// The load time column under every naming: dlt has none, so it keeps SQLLoad's.
pub const SQLLOAD_LOADED_AT: &str = "_sqlload_loaded_at";

/// Each column a load adds, as `(role, name under naming = "sqlload", name under naming = "dlt")`.
pub const METADATA_NAMES: [(MetadataRole, &str, &str); 3] = [
    (MetadataRole::LoadId, SQLLOAD_LOAD_ID, "_dlt_load_id"),
    (MetadataRole::LoadedAt, SQLLOAD_LOADED_AT, SQLLOAD_LOADED_AT),
    (MetadataRole::RowId, "_sqlload_row_id", "_dlt_id"),
];
