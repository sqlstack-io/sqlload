use crate::schema_plan::models::{ColumnChange, LiveColumn};

pub(crate) struct ChangeColumnsTestCase {
    pub(crate) description: &'static str,
    pub(crate) live: Vec<LiveColumn>,
    pub(crate) changes: Vec<ColumnChange>,
    pub(crate) expected_statements: &'static [&'static str],
}
