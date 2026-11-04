/// The path segment meaning "this folder", dropped when joining.
pub const CURRENT_FOLDER: &str = ".";

/// The path segment meaning "the parent folder", rejected so paths never leave their base.
pub const PARENT_FOLDER: &str = "..";
