use crate::store_paths::models::StoreKind;

pub(crate) struct FolderTestCase {
    pub(crate) description: &'static str,
    pub(crate) parts: &'static [&'static str],
    pub(crate) expected_folder: Result<&'static str, &'static str>,
}

pub(crate) struct RenderTemplateTestCase {
    pub(crate) description: &'static str,
    pub(crate) template: &'static str,
    pub(crate) values: &'static [(&'static str, &'static str)],
    pub(crate) expected_text: Result<&'static str, &'static str>,
}

pub(crate) struct HiveValuesTestCase {
    pub(crate) description: &'static str,
    pub(crate) key: &'static str,
    pub(crate) expected_values: &'static [(&'static str, &'static str)],
}

pub(crate) struct ResolveLocationTestCase {
    pub(crate) description: &'static str,
    pub(crate) connection: Option<StoreKind>,
    pub(crate) path: &'static str,
    /// The location as it prints, or the error text.
    pub(crate) expected_location: Result<&'static str, &'static str>,
}

pub(crate) struct PartitionValueTestCase {
    pub(crate) description: &'static str,
    pub(crate) value: &'static str,
    pub(crate) expected_escaped: &'static str,
}

/// How a local path reads on Windows, and on Unix, where only `/` starts an absolute path.
pub(crate) struct LocalRootTestCase {
    pub(crate) description: &'static str,
    pub(crate) path: &'static str,
    /// The root and the folders below it, as `root|folders`; `None` for a relative path.
    pub(crate) expected_windows: Option<&'static str>,
    pub(crate) expected_unix: Option<&'static str>,
}

/// The local path a `file://` URL names, given what follows `file://`, on Windows and on Unix.
pub(crate) struct FileUrlPathTestCase {
    pub(crate) description: &'static str,
    pub(crate) rest: &'static str,
    pub(crate) expected_windows: &'static str,
    pub(crate) expected_unix: &'static str,
}

/// A folder prefix below a connection's folders, on Windows and on Unix.
pub(crate) struct StripFoldersTestCase {
    pub(crate) description: &'static str,
    pub(crate) prefix: &'static str,
    pub(crate) folders: &'static str,
    pub(crate) expected_windows: Option<&'static str>,
    pub(crate) expected_unix: Option<&'static str>,
}

/// A filesystem root, as object_store's local store opens it on Windows and on Unix.
pub(crate) struct ShareByAddressTestCase {
    pub(crate) description: &'static str,
    pub(crate) root: &'static str,
    pub(crate) expected_windows: &'static str,
}
