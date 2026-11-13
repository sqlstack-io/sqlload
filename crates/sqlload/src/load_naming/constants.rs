use crate::load_naming::models::MetadataRole;

/// The prefix of every column SQLLoad adds under its own name, whatever the naming.
pub const SQLLOAD_PREFIX: &str = "_sqlload_";

/// The load id column under `naming = "sqlload"`.
pub const SQLLOAD_LOAD_ID: &str = "_sqlload_load_id";

/// The load time column under every naming: dlt has none, so it keeps SQLLoad's.
pub const SQLLOAD_LOADED_AT: &str = "_sqlload_loaded_at";

/// Each column a load adds, as `(role, name under naming = "sqlload", name under naming = "dlt")`.
pub const METADATA_NAMES: [(MetadataRole, &str, &str); 6] = [
    (MetadataRole::LoadId, SQLLOAD_LOAD_ID, "_dlt_load_id"),
    (MetadataRole::LoadedAt, SQLLOAD_LOADED_AT, SQLLOAD_LOADED_AT),
    (MetadataRole::RowId, "_sqlload_row_id", "_dlt_id"),
    (
        MetadataRole::ParentId,
        "_sqlload_parent_id",
        "_dlt_parent_id",
    ),
    (
        MetadataRole::ListIndex,
        "_sqlload_list_idx",
        "_dlt_list_idx",
    ),
    (MetadataRole::RootId, "_sqlload_root_id", "_dlt_root_id"),
];

/// What joins a parent table's name and a list field's into its child table's (`orders__items`), under every naming, as dlt's.
pub const CHILD_TABLE_SEPARATOR: &str = "__";

/// The column a list of scalars puts each item in, under every naming, as dlt's.
pub const VALUE_COLUMN: &str = "value";

/// The name dlt gives a key that is empty or only whitespace.
pub const DLT_EMPTY_NAME: &str = "_empty";

/// The characters dlt's snake_case swaps before anything else, each for the one beside it: `+` and `*` to `x`, `-` to `_`, `@` to `a`, `|` to `l`.
pub const DLT_SWAPPED: [(char, char); 5] =
    [('+', 'x'), ('-', '_'), ('*', 'x'), ('@', 'a'), ('|', 'l')];

/// The bytes of SHAKE128 in the tag dlt puts in a name it shortens, as its collision probability of 0.001 gives: 6 characters of base64.
pub const DLT_TAG_BYTES: usize = 4;
