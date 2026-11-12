use crate::adapters::contract::models::BoundParameter;
use crate::auth::models::Secret;
use crate::postgres_source::models::{ReadColumn, SourceTable};

/// An ODBC source: the connection string its driver manager connects with, and the buffer caps for long values.
#[derive(Debug, Clone)]
pub struct OdbcReader {
    pub(crate) connection_string: Secret,
    pub(crate) limits: ValueLimits,
}

/// The largest text and binary value a read holds, in bytes; `None` lets the driver say, which for unbounded columns (`TEXT`, `VARCHAR(MAX)`) can be far more than any value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ValueLimits {
    pub max_text_size: Option<usize>,
    pub max_binary_size: Option<usize>,
}

/// What one read selects: a table or a query, its columns, and the rows' filter.
#[derive(Debug, Clone, Copy)]
pub struct OdbcReadRequest<'a> {
    pub table: &'a SourceTable,
    /// The query whose rows are the table; `None` reads the table itself.
    pub sql: Option<&'a str>,
    pub columns: &'a [ReadColumn],
    /// A condition on the source's columns, already rendered.
    pub filter: Option<&'a str>,
    /// The values bound to `filter`'s `?` placeholders, in order, each as its type.
    pub parameters: &'a [BoundParameter],
    pub batch_rows: usize,
}
