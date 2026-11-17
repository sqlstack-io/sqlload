pub(crate) struct BuildPlanTestCase {
    pub(crate) description: &'static str,
    pub(crate) staging: &'static str,
    pub(crate) key_pair: bool,
    pub(crate) existing_columns: &'static [(&'static str, &'static str)],
    pub(crate) expected_objects: &'static [&'static str],
    pub(crate) expected_problems: usize,
}

pub(crate) struct PipeFormatTestCase {
    pub(crate) description: &'static str,
    pub(crate) format: &'static str,
    /// The existing pipe's definition, or None when there is no pipe.
    pub(crate) existing_definition: Option<&'static str>,
    pub(crate) expected_objects: &'static [&'static str],
    /// Text the pipe's create statement must contain.
    pub(crate) expected_sql: &'static str,
}

pub(crate) struct DatabricksPlanTestCase {
    pub(crate) description: &'static str,
    pub(crate) staging: &'static str,
    /// Columns of an existing `shop__orders` table, as information_schema lists them.
    pub(crate) existing_columns: &'static [(&'static str, &'static str)],
    pub(crate) expected_objects: &'static [&'static str],
    /// The create statements of the objects to create.
    pub(crate) expected_sql: &'static [&'static str],
}
