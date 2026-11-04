/// The load id column every loaded row carries.
pub const LOAD_ID_COLUMN: &str = "_sqlload_load_id";

/// The load time column every loaded row carries.
pub const LOADED_AT_COLUMN: &str = "_sqlload_loaded_at";

/// The command-line flag that allows table rebuilds for one run.
pub const ALLOW_REBUILD_FLAG: &str = "--allow-type-change-rebuild";
