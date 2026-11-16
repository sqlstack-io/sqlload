/// How the folder a load holds its batches in is named, before its random part.
pub const FOLDER_PREFIX: &str = "sqlload-send-";

/// The suffix of a held file: Arrow IPC, which keeps every Arrow type as it was.
pub const FILE_SUFFIX: &str = ".arrow";
