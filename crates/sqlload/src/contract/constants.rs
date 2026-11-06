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
