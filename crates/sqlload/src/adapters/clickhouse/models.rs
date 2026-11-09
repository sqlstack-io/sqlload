/// ClickHouse's SQL dialect.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClickHouseSql;

/// A ClickHouse server reached over HTTP, sharing one connection pool.
#[derive(Debug, Clone)]
pub struct ClickHouseDestination {
    pub http: reqwest::Client,
    /// The server's root, such as `http://localhost:8123/`.
    pub url: String,
    pub user: String,
    pub password: Option<crate::auth::models::Secret>,
    /// Held while a load's tables change the schema, one at a time.
    pub ddl: std::sync::Arc<tokio::sync::Mutex<()>>,
}

/// A table's ClickHouse write session: Arrow rows gathered into inserts of a few dozen megabytes, each sent while the next fills.
#[derive(Debug)]
pub struct ClickHouseWriter {
    pub destination: ClickHouseDestination,
    /// The rows waiting for the next insert, and the statement they go in with.
    pub pending: PendingInsert,
    /// The insert being sent.
    pub sending: Option<tokio::task::JoinHandle<Result<(), crate::http::errors::ApiError>>>,
}

/// Rows gathered for one insert request.
#[derive(Debug, Default)]
pub struct PendingInsert {
    /// `INSERT INTO ... (...) FORMAT ArrowStream` for the rows' columns.
    pub statement: String,
    pub batches: Vec<arrow_array::RecordBatch>,
    pub bytes: usize,
}

/// A step of a write that the statement runner carries out itself, as it needs what the server answers or a fallback (sent among the statements as [`PUBLISH_PREFIX`](crate::adapters::clickhouse::constants::PUBLISH_PREFIX) and JSON).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PublishStep {
    /// Swap `build` in for `target` with `EXCHANGE TABLES`, or by renames through `aside` where the database cannot exchange tables; `build` then holds the old table.
    Exchange {
        target: String,
        build: String,
        aside: String,
    },
    /// Replace `target`'s partition `partition` (its ID) with `copy`'s, or drop it when `copy` holds none of its rows: ClickHouse 26.6 and later refuse to replace a partition from a table with no parts in it.
    ReplacePartition {
        target: String,
        copy: String,
        partition: String,
    },
    /// Delete `table`'s rows matching `condition` with a lightweight `DELETE`, or with `ALTER TABLE ... DELETE` where the table refuses one (a table with projections does unless it is set to rebuild them).
    DeleteRows { table: String, condition: String },
    /// Publish the rows of `delta` (a table in `target`'s shape) into `target`, partition by partition. With `displaced`, a condition on `target`'s rows, the rows it holds are replaced: each partition the delta or those rows touch is hard-linked into `copy`, the displaced rows are deleted there and the delta's attached, then it is swapped in. Without it the delta's partitions are added. `aside` is where a rename swap of `copy` moves the old table.
    Partitions {
        target: String,
        delta: String,
        copy: String,
        aside: String,
        displaced: Option<String>,
    },
}
