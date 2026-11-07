//! How the Statement Execution API hands back a statement's answer: at once or after polls, in one chunk or several.

use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::fake_databricks::_helpers::volume::Outcome;
use crate::fake_databricks::models::FakeState;

const STATEMENTS: &str = "/api/2.0/sql/statements";

impl FakeState {
    /// Give `answer` an id, split its rows into chunks when chunking is on, and hold it back for the configured polls.
    pub(crate) fn deliver(&mut self, mut answer: Value) -> Value {
        self.statement_ids += 1;
        let id = format!("stmt-{}", self.statement_ids);
        answer["statement_id"] = json!(id);
        if let Some(size) = self.chunk_rows.filter(|size| *size > 0) {
            let rows = answer["result"]["data_array"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            let mut chunks: Vec<Value> = rows
                .chunks(size)
                .map(|rows| json!({ "data_array": rows }))
                .collect();
            let count = chunks.len();
            for (index, chunk) in chunks.iter_mut().enumerate().take(count.saturating_sub(1)) {
                chunk["next_chunk_internal_link"] =
                    json!(format!("{STATEMENTS}/{id}/result/chunks/{}", index + 1));
            }
            let mut chunks = chunks.into_iter();
            answer["result"] = chunks.next().unwrap_or_else(|| json!({ "data_array": [] }));
            for (index, chunk) in chunks.enumerate() {
                self.chunks.insert((id.clone(), index + 1), chunk);
            }
        }
        if self.pending_polls == 0 {
            return answer;
        }
        self.running
            .insert(id.clone(), (self.pending_polls, answer));
        json!({ "statement_id": id, "status": { "state": "PENDING" } })
    }

    /// Answer a poll of statement `id`: a scripted failure, still `PENDING`, or its answer once its polls are used up.
    pub(crate) fn poll(&mut self, id: &str) -> Outcome {
        if let Some(status) = self.poll_failures.pop_front() {
            return Err((
                StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                json!({ "error_code": "TEMPORARILY_UNAVAILABLE", "message": "poll failed" }),
            ));
        }
        let Some((left, answer)) = self.running.get_mut(id) else {
            return Err((
                StatusCode::NOT_FOUND,
                json!({ "error_code": "NOT_FOUND", "message": id }),
            ));
        };
        *left = left.saturating_sub(1);
        match *left {
            0 => Ok(answer.clone()),
            _ => Ok(json!({ "statement_id": id, "status": { "state": "RUNNING" } })),
        }
    }

    /// One later chunk of a statement's result.
    pub(crate) fn chunk(&self, id: &str, index: usize) -> Outcome {
        match self.chunks.get(&(id.to_owned(), index)) {
            Some(chunk) => Ok(chunk.clone()),
            None => Err((
                StatusCode::NOT_FOUND,
                json!({ "error_code": "NOT_FOUND", "message": id }),
            )),
        }
    }
}

/// `/api/2.0/sql/statements/<id>[/result/chunks/<n>|/cancel]` → the id and the rest.
pub(crate) fn statement_path(path: &str) -> Option<(&str, &str)> {
    let rest = path.strip_prefix(STATEMENTS)?.strip_prefix('/')?;
    Some(rest.split_once('/').unwrap_or((rest, "")))
}
