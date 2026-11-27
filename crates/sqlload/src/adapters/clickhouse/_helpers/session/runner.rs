use crate::adapters::clickhouse::_helpers::session::http::send;
use crate::adapters::clickhouse::_helpers::session::rows::text_rows;
use crate::adapters::clickhouse::constants::EXCEPTION_CODE_PREFIX;
use crate::adapters::clickhouse::models::{ClickHouseDestination, SessionSteps};
use crate::adapters::clickhouse::types::StepRunner;
use crate::adapters::session::main::_execute_statement::execute_statement;
use crate::adapters::session::main::_query_rows::query_rows;
use crate::http::errors::ApiError;
use crate::loader::errors::LoaderError;

impl StepRunner for ClickHouseDestination {
    type Error = ApiError;

    async fn run(&self, statement: &str) -> Result<(), ApiError> {
        send(self, None, statement.as_bytes().to_vec())
            .await
            .map(|_| ())
    }

    async fn rows(&self, query: &str) -> Result<Vec<Vec<Option<String>>>, ApiError> {
        let text = send(
            self,
            None,
            format!("{query} FORMAT JSONCompactStrings").into_bytes(),
        )
        .await?;
        text_rows(&text)
    }

    fn code(error: &ApiError) -> Option<String> {
        error.code().map(str::to_owned)
    }

    fn ddl(&self) -> &tokio::sync::Mutex<()> {
        &self.ddl
    }
}

impl StepRunner for SessionSteps<'_> {
    type Error = LoaderError;

    async fn run(&self, statement: &str) -> Result<(), LoaderError> {
        execute_statement(self.destination, statement.to_owned()).await
    }

    async fn rows(&self, query: &str) -> Result<Vec<Vec<Option<String>>>, LoaderError> {
        query_rows(self.destination, query.to_owned()).await
    }

    fn code(error: &LoaderError) -> Option<String> {
        exception_code(&error.to_string())
    }

    fn ddl(&self) -> &tokio::sync::Mutex<()> {
        &self.destination.ddl
    }
}

/// The error code in a ClickHouse exception's text, `Code: 48. DB::Exception: ...`, as every client reports it.
pub(crate) fn exception_code(message: &str) -> Option<String> {
    let (_, rest) = message.split_once(EXCEPTION_CODE_PREFIX)?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    (!digits.is_empty()).then_some(digits)
}
