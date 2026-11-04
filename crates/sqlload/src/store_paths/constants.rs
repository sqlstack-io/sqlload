/// The path segment meaning "this folder", dropped when joining.
pub const CURRENT_FOLDER: &str = ".";

/// The path segment meaning "the parent folder", rejected so paths never leave their base.
pub const PARENT_FOLDER: &str = "..";

/// Characters Hive and Spark write as `%XX` in partition folder names, besides control characters.
pub const HIVE_ESCAPED: &[char] = &[
    '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '{', '[', ']', '^',
];
