pub(crate) struct TomlOptionsTestCase {
    pub(crate) description: &'static str,
    /// The `[targets.dev]` section: a DuckDB target, or a files target.
    pub(crate) target: &'static str,
    /// The `[sources.s]` section's lines before `settings`: its kind and connection, and a table entry's header.
    pub(crate) source: &'static str,
    pub(crate) settings: &'static str,
    /// Text parse's error must hold (where the table is set, then the problem); empty when the project parses.
    pub(crate) expected_problem: &'static str,
}

pub(crate) struct PythonOptionsTestCase {
    pub(crate) description: &'static str,
    /// Python table `t`'s settings, as TOML.
    pub(crate) settings: &'static str,
    pub(crate) expected_problem: &'static str,
}
