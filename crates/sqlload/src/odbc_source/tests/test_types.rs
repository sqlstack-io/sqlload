use crate::postgres_source::models::SourceTable;

pub(crate) struct SelectQueryTestCase {
    pub(crate) description: &'static str,
    pub(crate) table: SourceTable,
    pub(crate) sql: Option<&'static str>,
    pub(crate) filter: Option<&'static str>,
    pub(crate) expected_query: &'static str,
}

pub(crate) struct ConvertedDecimalTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    /// The value loaded as decimal(28,6), in millionths; `None` when the read must fail.
    pub(crate) expected_value: Option<i128>,
}

pub(crate) struct OdbcReaderTestCase {
    pub(crate) description: &'static str,
    pub(crate) connection_string: Option<&'static str>,
    pub(crate) dsn: Option<&'static str>,
    pub(crate) user: Option<&'static str>,
    pub(crate) password: Option<&'static str>,
    pub(crate) expected_connection_string: &'static str,
}
