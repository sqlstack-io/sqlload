use crate::column_types::models::ColumnType;

pub(crate) struct SelectFilesTestCase {
    pub(crate) description: &'static str,
    /// Loaded files and their versions.
    pub(crate) loaded: &'static [(&'static str, &'static str)],
    pub(crate) cursor_type: Option<ColumnType>,
    pub(crate) bounds: (Option<&'static str>, Option<&'static str>),
    /// The keys chosen, or the error text.
    pub(crate) expected_keys: Result<&'static [&'static str], &'static str>,
}
