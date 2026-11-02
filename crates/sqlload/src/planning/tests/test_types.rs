pub(crate) struct BuildPlanTestCase {
    pub(crate) description: &'static str,
    pub(crate) staging: &'static str,
    pub(crate) key_pair: bool,
    pub(crate) existing_columns: &'static [(&'static str, &'static str)],
    pub(crate) expected_objects: &'static [&'static str],
    pub(crate) expected_problems: usize,
}
