use crate::schema_plan::models::{DesiredColumn, LiveColumn, SchemaRules};

pub(crate) struct PlanSchemaTestCase {
    pub(crate) description: &'static str,
    pub(crate) desired: Vec<DesiredColumn>,
    pub(crate) live: Option<Vec<LiveColumn>>,
    pub(crate) rules: SchemaRules,
    /// `create`, `unchanged`, or one line per change.
    pub(crate) expected_action: &'static [&'static str],
    pub(crate) expected_written: &'static [&'static str],
    /// Text every blocking reason must include, joined; empty when not blocked.
    pub(crate) expected_blocked: &'static str,
    pub(crate) expected_fix_flag: Option<&'static str>,
    pub(crate) expected_warnings: usize,
}
