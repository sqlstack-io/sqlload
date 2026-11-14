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

pub(crate) struct TimestampTextTestCase {
    pub(crate) description: &'static str,
    pub(crate) time: &'static str,
    pub(crate) digits: u16,
    pub(crate) expected_text: &'static str,
}

pub(crate) struct ConnectErrorTestCase {
    pub(crate) description: &'static str,
    /// The file written in the test's folder, by its path there, and what it holds.
    pub(crate) file: (&'static str, Vec<u8>),
    /// The path the connection string registers as the driver library, after the folder and a `/`.
    pub(crate) library: &'static str,
    /// What the error adds under unixODBC's, `{path}` standing for the library's path and `{folder}` for the folder's.
    pub(crate) expected_reason: &'static str,
}
