pub(crate) struct CreatePlaygroundTestCase {
    pub(crate) description: &'static str,
    /// A folder below the temporary folder made before the run: `shop` itself, or another.
    pub(crate) existing_folder: &'static str,
    /// Files written into that folder before the run.
    pub(crate) existing_files: &'static [&'static str],
    pub(crate) expected_lines: Result<&'static [&'static str], &'static str>,
    /// Each file below `shop` with its text afterwards; an empty text where there is no file.
    pub(crate) expected_files: &'static [(&'static str, &'static str)],
}

pub(crate) struct ApplyChangeTestCase {
    pub(crate) description: &'static str,
    /// The project's sqlload.toml, the playground's own or another.
    pub(crate) config: &'static str,
    /// How many times `--change` runs; the lines are the last run's.
    pub(crate) runs: usize,
    pub(crate) expected_lines: Result<&'static [&'static str], &'static str>,
    /// The text of the change file afterwards; empty when it was not added.
    pub(crate) expected_change: &'static str,
}
