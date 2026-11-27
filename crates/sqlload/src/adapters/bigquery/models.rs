/// BigQuery GoogleSQL's dialect.
#[derive(Debug, Clone, Copy, Default)]
pub struct BigQuerySql;

/// An authenticated BigQuery project, sharing one HTTP pool.
#[derive(Debug, Clone)]
pub struct BigQueryDestination {
    pub api: crate::bigquery_api::models::BigQueryClient,
    /// The project tables load into: the target's, the connection's, or the key's.
    pub project: String,
    /// Held while a load's tables change the schema, one at a time.
    pub ddl: std::sync::Arc<tokio::sync::Mutex<()>>,
}

/// One pending write stream a batch's requests go to.
#[derive(Debug)]
pub struct OpenStream {
    pub name: String,
    /// Rows sent so far: the next request's offset.
    pub offset: i64,
    /// Where requests go; `None` once the stream is closed to appends.
    pub requests: Option<tokio::sync::mpsc::Sender<crate::bigquery_api::models::AppendRowsRequest>>,
    /// Reads every answer, failing on the first error or refused row; the requests it saw answered.
    pub answers: Option<tokio::task::JoinHandle<Result<usize, crate::http::errors::ApiError>>>,
}

/// A table's BigQuery write session: each batch streams into its staging or build table through pending streams, committed together once the batch is sent.
#[derive(Debug)]
pub struct BigQueryWriter {
    pub destination: BigQueryDestination,
    /// The table the open streams write into, as `projects/p/datasets/d/tables/t`.
    pub parent: String,
    pub streams: Vec<OpenStream>,
    /// Requests sent in the batch, spread over its streams in turn.
    pub requests_sent: usize,
}

/// How a BigQuery table is partitioned, as its `partition_by` names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Partitioning {
    /// By time: a DATE column's own value (`function` `None`), a TIMESTAMP's or DATETIME's day (`DATE`), or a truncation to `unit` (`DATE_TRUNC`, `TIMESTAMP_TRUNC`, `DATETIME_TRUNC`).
    Time {
        function: Option<&'static str>,
        column: String,
        unit: Option<&'static str>,
    },
    /// By an integer column's ranges, `RANGE_BUCKET(column, GENERATE_ARRAY(start, end, interval))`.
    Range {
        column: String,
        start: i64,
        end: i64,
        interval: i64,
    },
}
