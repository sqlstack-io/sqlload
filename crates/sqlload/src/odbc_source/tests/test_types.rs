use crate::postgres_source::models::SourceTable;

pub(crate) struct SelectQueryTestCase {
    pub(crate) description: &'static str,
    pub(crate) table: SourceTable,
    pub(crate) sql: Option<&'static str>,
    pub(crate) filter: Option<&'static str>,
    pub(crate) expected_query: &'static str,
}
