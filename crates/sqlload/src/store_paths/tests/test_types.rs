use crate::store_paths::models::StoreKind;

pub(crate) struct FolderTestCase {
    pub(crate) description: &'static str,
    pub(crate) parts: &'static [&'static str],
    pub(crate) expected_folder: Result<&'static str, &'static str>,
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
