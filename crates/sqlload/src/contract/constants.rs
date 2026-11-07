/// Columns SQLLoad adds itself start with this; they are never findings.
pub const SQLLOAD_COLUMN_PREFIX: &str = "_sqlload_";

/// A live column no declaration names, on a `contract = "enforced"` table.
pub const MISSING_DECLARATION: &str = "missing_declaration";

/// A declared column the live table lacks.
pub const MISSING_PHYSICAL_COLUMN: &str = "missing_physical_column";

/// A declared column whose live type differs.
pub const TYPE_MISMATCH: &str = "type_mismatch";

/// A table that does not exist yet.
pub const RELATION_MISSING: &str = "relation_missing";

/// A table whose columns could not be read.
pub const INSPECTION_UNAVAILABLE: &str = "inspection_unavailable";

/// With `--all-files`: files holding a column under another type than the newest load.
pub const FILES_TYPE_MISMATCH: &str = "files_type_mismatch";

/// With `--all-files`: files lacking a column the newest load has.
pub const FILES_MISSING_COLUMN: &str = "files_missing_column";

/// With `--all-files`: a column some files hold that the newest load lacks.
pub const FILES_EXTRA_COLUMN: &str = "files_extra_column";

/// File keys a files finding names before it only counts the rest.
pub const NAMED_FILES: usize = 3;

/// With `--source`: a column the source would load that the live table lacks.
pub const SOURCE_NEW_COLUMN: &str = "source_new_column";

/// With `--source`: a live column the source would no longer load.
pub const SOURCE_MISSING_COLUMN: &str = "source_missing_column";

/// With `--source`: a column the source would load as another type than the live table holds.
pub const SOURCE_TYPE_MISMATCH: &str = "source_type_mismatch";
