use crate::http::errors::ApiError;

/// Where a ClickHouse write's statements and publish steps run: SQLLoad's own connection over HTTP, or a caller's session; every publish step is carried out through this alone, so both run them alike.
pub(crate) trait StepRunner: Sync {
    /// Why a statement failed; a step's own failures (a malformed step, a table with no engine) are protocol errors.
    type Error: From<ApiError> + std::fmt::Display + Send;

    /// Run one statement.
    async fn run(&self, statement: &str) -> Result<(), Self::Error>;

    /// The rows `query` returns, each value as text (`None` for NULL).
    async fn rows(&self, query: &str) -> Result<Vec<Vec<Option<String>>>, Self::Error>;

    /// The ClickHouse error code `error` carries, which picks a step's fallback.
    fn code(error: &Self::Error) -> Option<String>;

    /// Held while a load's tables change the schema, one at a time.
    fn ddl(&self) -> &tokio::sync::Mutex<()>;
}
