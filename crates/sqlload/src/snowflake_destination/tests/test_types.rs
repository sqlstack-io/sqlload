pub(crate) struct CopyStagedTestCase {
    pub(crate) description: &'static str,
    pub(crate) files: usize,
    /// How many statements, and the start of the first.
    pub(crate) expected_copies: (usize, &'static str),
}

pub(crate) struct StaleFoldersTestCase {
    pub(crate) description: &'static str,
    /// Each staged file's name and `last_modified`, as `LIST` shows them.
    pub(crate) listed: &'static [(&'static str, &'static str)],
    pub(crate) expected_folders: &'static [&'static str],
}
