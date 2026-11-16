use crate::column_types::models::Warehouse;
use crate::project::models::{Deletes, SourceKind, WriteStrategy};

pub(crate) struct DeletesProblemsTestCase {
    pub(crate) description: &'static str,
    pub(crate) deletes: Option<Deletes>,
    pub(crate) strategy: WriteStrategy,
    pub(crate) kind: SourceKind,
    pub(crate) warehouse: Warehouse,
    pub(crate) motherduck: bool,
    pub(crate) unnests: bool,
    pub(crate) expected_problems: &'static [&'static str],
}
