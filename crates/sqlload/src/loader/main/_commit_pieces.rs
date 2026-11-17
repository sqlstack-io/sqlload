//! A table's commits cut into pieces under DuckDB's memory share: no commit holds more of the values DuckDB keeps in memory until a checkpoint than its share commits at once.

use std::sync::atomic::Ordering;

use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};

use crate::adapters::contract::constants::{COMMIT_STATEMENT, ROLLBACK_STATEMENT};
use crate::adapters::contract::models::{TableName, WriteMode};
use crate::adapters::contract::types::TableWriter;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::execute_ddl::execute_ddl;
use crate::adapters::dispatch::main::session::single_text::single_text;
use crate::load_naming::main::metadata_name::metadata_name;
use crate::load_naming::models::MetadataRole;
use crate::loader::_helpers::publish::{staged_columns, write_target};
use crate::loader::errors::LoaderError;
use crate::loader::models::{CommitRange, Publisher};
use crate::memory_budget::constants::{
    DUCKDB_COMMIT_PERCENT, DUCKDB_ROW_GROUP_PERCENT, DUCKDB_ROW_GROUP_ROWS, HOLDING_DIVISOR, LARGE_VALUE_BYTES,
    STAGED_COPIES,
};
use crate::memory_budget::main::large_values::large_values;
use crate::memory_budget::main::needed_budget::needed_budget;
use crate::memory_budget::main::table_memory::table_memory;
use crate::memory_budget::models::DuckDbShare;
use crate::number_text::main::memory_text::memory_text;
use crate::number_text::main::thousands::thousands;
use crate::run_log::main::unfinished_marker::unfinished_marker;
use crate::run_log::models::UnfinishedMarker;

/// Where a table's commit may be cut into pieces.
enum Cuts {
    /// Between any two rows.
    Anywhere,
    /// Only where these columns' value changes: a `delete_insert` replaces each value's rows together.
    Between(Vec<String>),
    /// Nowhere, and why.
    Never(&'static str),
}

/// Stop the process at `point` when a crash test asks for it there (`SQLLOAD_TEST_CRASH_AT`); compiled into test builds only.
fn crash_point(point: &str) {
    #[cfg(feature = "container-tests")]
    if std::env::var("SQLLOAD_TEST_CRASH_AT").is_ok_and(|wanted| wanted == point) {
        std::process::abort();
    }
    #[cfg(not(feature = "container-tests"))]
    let _ = point;
}

/// The write mode of one commit: the planned one, with a `delete_insert` narrowed to the commit's window or partitions (its `replace_key` kept).
pub(in crate::loader) fn commit_mode(planned: &WriteMode, range: Option<CommitRange>) -> WriteMode {
    let narrowed = match range {
        Some(CommitRange::Window { from, to }) => (from, to, Vec::new()),
        Some(CommitRange::Values(values)) if !values.is_empty() => (None, None, values),
        _ => return planned.clone(),
    };
    match planned {
        WriteMode::DeleteInsert {
            cursor_column,
            replace_key,
            not_null_keys,
            ..
        } => WriteMode::DeleteInsert {
            cursor_column: cursor_column.clone(),
            from: narrowed.0,
            to: narrowed.1,
            values: narrowed.2,
            replace_key: replace_key.clone(),
            not_null_keys: not_null_keys.clone(),
        },
        planned => planned.clone(),
    }
}

/// The write mode of a piece: a `delete_insert` replaces only the rows of the `replace_key` values, or cursor values, its rows hold; its commit's range goes when the commit ends.
pub(in crate::loader) fn piece_mode(planned: &WriteMode) -> WriteMode {
    match planned {
        WriteMode::DeleteInsert {
            cursor_column,
            replace_key,
            not_null_keys,
            ..
        } => WriteMode::DeleteInsert {
            cursor_column: None,
            from: None,
            to: None,
            values: Vec::new(),
            replace_key: match replace_key.is_empty() {
                true => cursor_column.iter().cloned().collect(),
                false => replace_key.clone(),
            },
            not_null_keys: match replace_key.is_empty() {
                true => Vec::new(),
                false => not_null_keys.clone(),
            },
        },
        planned => planned.clone(),
    }
}

/// Whether the table's commits delete a cursor range (`delete_insert` with a cursor), so its last range commits even without rows: rows deleted upstream go too.
pub(in crate::loader) fn deletes_range(mode: &WriteMode) -> bool {
    matches!(
        mode,
        WriteMode::DeleteInsert {
            cursor_column: Some(_),
            ..
        }
    )
}

/// Each row's value of `columns` in `batch`, as one text; `None` when the batch lacks one.
fn row_keys(batch: &RecordBatch, columns: &[String]) -> Option<Vec<String>> {
    let options = FormatOptions::default().with_null("\u{0}");
    let formatters: Vec<ArrayFormatter<'_>> = columns
        .iter()
        .map(|name| {
            let column = batch.column_by_name(name)?;
            ArrayFormatter::try_new(column.as_ref(), &options).ok()
        })
        .collect::<Option<_>>()?;
    Some(
        (0..batch.num_rows())
            .map(|row| {
                formatters
                    .iter()
                    .map(|formatter| formatter.value(row).to_string())
                    .collect::<Vec<String>>()
                    .join("\u{1f}")
            })
            .collect(),
    )
}

impl Publisher {
    /// Write `batch` where the table's batches go. Under a DuckDB memory share, a batch with large values (see [`large_values`]) is written a piece at a time, each piece committed once the next row would take the commit past the share's commit bytes (a quarter of that while the watchdog holds the load), cut only where the table's commit may be cut (see [`Cuts`]); rows that must commit together past DuckDB's `memory_limit`, or a commit that can't be cut past it, stop the table, naming the fix.
    pub(in crate::loader) async fn write_rows(
        &mut self,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        let share = self.writer.memory_share();
        let large = share.and_then(|_| large_values(&batch));
        let (Some(share), Some(large)) = (share, large) else {
            return self.write_part(batch).await;
        };
        self.pieces.rows += batch.num_rows() as u64;
        self.pieces.large += large.iter().sum::<u64>();
        let staged = self.staging.is_some() && !self.builds;
        let copies = match staged {
            true => STAGED_COPIES,
            false => 1,
        };
        let cuts = self.cuts();
        let keys = match &cuts {
            Cuts::Between(columns) => row_keys(&batch, columns),
            _ => None,
        };
        let mut start = 0;
        let mut boundary: Option<usize> = None;
        let mut carried = self.pieces.held;
        let mut written = 0;
        for (row, bytes) in large.iter().enumerate() {
            let changed = match (&cuts, &keys) {
                (Cuts::Anywhere, _) => true,
                (Cuts::Between(_), Some(keys)) => {
                    self.pieces.last_key.as_ref() != Some(&keys[row])
                }
                _ => false,
            };
            if let Some(keys) = &keys {
                self.pieces.last_key = Some(keys[row].clone());
            }
            if changed {
                self.pieces.run = 0;
                if row > start || carried > 0 {
                    boundary = Some(row);
                }
            }
            self.pieces.run += bytes;
            if self.pieces.run > share.memory_limit {
                return Err(self.too_large(&cuts, share));
            }
            written += bytes * copies;
            let held = carried + written;
            if held <= self.commit_bytes(share) {
                continue;
            }
            let Some(at) = boundary.take() else {
                continue;
            };
            if at > start {
                self.write_part(batch.slice(start, at - start)).await?;
            }
            let rest = (batch.num_rows() - at) as u64;
            self.batch_rows -= rest;
            self.commit_piece().await?;
            self.batch_rows = rest;
            carried = 0;
            written = large[at..=row].iter().sum::<u64>() * copies;
            start = at;
        }
        self.pieces.held = carried + written;
        self.report_held();
        match start {
            0 => self.write_part(batch).await,
            start if start < batch.num_rows() => {
                let rest = batch.num_rows() - start;
                self.write_part(batch.slice(start, rest)).await
            }
            _ => Ok(()),
        }
    }

    /// The most one piece holds: the share's commit bytes, or a quarter of them while the watchdog holds the load near its budget.
    fn commit_bytes(&self, share: DuckDbShare) -> u64 {
        let holding = table_memory().is_some_and(|watched| watched.guard.holding.load(Ordering::Relaxed));
        match holding {
            true => share.commit_bytes / HOLDING_DIVISOR,
            false => share.commit_bytes,
        }
    }

    /// Tell the watchdog what the table holds: its open piece's large values.
    fn report_held(&self) {
        if let Some(watched) = table_memory() {
            watched.table.held.store(self.pieces.held, Ordering::Relaxed);
        }
    }

    /// Where the table's commits may be cut: anywhere into a table this load builds or creates, or straight into the table; a `delete_insert` into a table with rows only between its values, so no value is half replaced.
    fn cuts(&self) -> Cuts {
        if self.children.is_some() {
            return Cuts::Never("a table with unnest's child tables commits whole");
        }
        match &self.table.mode {
            _ if self.builds || self.direct || !self.table.exists => Cuts::Anywhere,
            WriteMode::Append | WriteMode::Merge { .. } => Cuts::Anywhere,
            WriteMode::DeleteInsert { replace_key, .. } if !replace_key.is_empty() => {
                Cuts::Between(replace_key.clone())
            }
            WriteMode::DeleteInsert {
                cursor_column: Some(cursor),
                ..
            } => Cuts::Between(vec![cursor.clone()]),
            WriteMode::Changes(_) => Cuts::Never("captured changes commit whole"),
            WriteMode::MarkMissing(_) => Cuts::Never("a key scan commits whole"),
            _ => Cuts::Never("this write commits whole"),
        }
    }

    /// Why the table stops: rows that must commit together hold more large values than DuckDB's `memory_limit`.
    fn too_large(&self, cuts: &Cuts, share: DuckDbShare) -> LoaderError {
        let what = match cuts {
            Cuts::Between(columns) => format!(
                "the rows of one {} value hold more than {} of values over {}, which a delete_insert replaces together",
                columns.join(", "),
                memory_text(share.memory_limit),
                memory_text(LARGE_VALUE_BYTES as u64)
            ),
            Cuts::Never(why) => format!(
                "one commit holds more than {} of values over {}, and {why}",
                memory_text(share.memory_limit),
                memory_text(LARGE_VALUE_BYTES as u64)
            ),
            Cuts::Anywhere => format!(
                "one row holds more than {} of values",
                memory_text(share.memory_limit)
            ),
        };
        LoaderError::Invalid(format!(
            "{}: {what}, past DuckDB's {} memory_limit; {}",
            self.table.spec.destination.table,
            memory_text(share.memory_limit),
            needed_budget(self.pieces.run.saturating_mul(100) / DUCKDB_COMMIT_PERCENT)
        ))
    }

    /// Write `batch` where the table's batches go.
    async fn write_part(&mut self, batch: RecordBatch) -> Result<(), LoaderError> {
        let columns = staged_columns(&self.table);
        let target = write_target(
            &self.table,
            self.staging.as_ref(),
            &columns,
            ((&self.load_id, self.batch), (self.direct, self.builds)),
        );
        self.writer.write_batch(&target, batch).await
    }

    /// Commit the rows written so far as a piece: a build table's or a direct append's transaction commits; staged rows commit into the table as a commit does, recording no run and keeping the load's position. An append's first piece marks the load unfinished, in the same transaction. Then DuckDB checkpoints once enough waits for it.
    async fn commit_piece(&mut self) -> Result<(), LoaderError> {
        let marker = self.marker();
        if let Some(marker) = &marker {
            execute_ddl(&self.destination, vec![marker.create.clone()]).await?;
        }
        match self.direct || self.builds {
            true => {
                if let Some(marker) = &marker {
                    execute(&self.destination, vec![marker.mark.clone()]).await?;
                }
                self.flush_batch().await?;
            }
            false => {
                self.flush_batch().await?;
                self.pieces.piece = true;
                let committed = Box::pin(self.commit(false)).await;
                self.pieces.piece = false;
                committed?;
            }
        }
        self.pieces.unfinished |= marker.is_some();
        self.pieces.held = 0;
        self.pieces.committed += 1;
        self.report_held();
        self.writer.checkpoint(false).await?;
        crash_point("after-first-piece");
        Ok(())
    }

    /// The recovery marker the next piece writes: an append's first piece, whose rows a rerun would not replace.
    fn marker(&self) -> Option<UnfinishedMarker> {
        let marks = !self.pieces.unfinished && self.table.mode == WriteMode::Append && !self.builds;
        marks.then(|| self.unfinished())
    }

    /// This load's recovery marker statements.
    fn unfinished(&self) -> UnfinishedMarker {
        let spec = &self.table.spec;
        let column = metadata_name(spec.compat, MetadataRole::LoadId);
        unfinished_marker(
            &spec.destination,
            (column, &self.load_id, &self.loaded_at),
            spec.warehouse,
        )
    }

    /// The statement a staged commit adds to its transaction for the marker: marking the load at its first piece, removing the mark at its last commit; `None` otherwise.
    pub(in crate::loader) fn marker_statement(&self, last: bool) -> Option<String> {
        match (self.pieces.piece, self.pieces.unfinished && last) {
            (true, _) => self.marker().map(|marker| marker.mark),
            (false, true) => Some(self.unfinished().unmark),
            (false, false) => None,
        }
    }

    /// Before the end of a direct append or a build: remove the marker inside the last transaction, so the table's last rows and the mark's removal commit together.
    pub(in crate::loader) async fn finish_pieces(&mut self) -> Result<(), LoaderError> {
        if self.pieces.unfinished && (self.direct || self.builds) {
            execute(&self.destination, vec![self.unfinished().unmark]).await?;
            self.pieces.unfinished = false;
        }
        Ok(())
    }

    /// After a failed load that marked itself unfinished: remove the rows its pieces committed, its run records and its mark, as the next load would.
    pub(in crate::loader) async fn abandon_pieces(&mut self) {
        if !self.pieces.unfinished {
            return;
        }
        self.writer.rollback_batch().await;
        let spec = &self.table.spec;
        let statements = crate::loader::main::recover_unfinished::recovery_statements(
            &spec.destination,
            (metadata_name(spec.compat, MetadataRole::LoadId), &self.load_id),
            (true, self.run.is_some()),
        );
        if let Err(error) = execute(&self.destination, statements).await {
            tracing::warn!(%error, table = %spec.destination.table, "cannot remove the rows of the failed load; the next load removes them");
        }
        self.pieces.unfinished = false;
    }

    /// Before a commit's transaction ends: under a DuckDB memory share, once the load wrote large values, check the table's last row group, which the next checkpoint rewrites whole, still fits DuckDB's `memory_limit` (see [`DUCKDB_ROW_GROUP_PERCENT`]); past it the commit rolls back and the table stops, naming its row width and the `memory_limit` it needs.
    pub(in crate::loader) async fn check_row_group(&mut self, into: &TableName) -> Result<(), LoaderError> {
        let Some(share) = self.writer.memory_share() else {
            return Ok(());
        };
        if self.pieces.large == 0 || self.pieces.rows == 0 {
            return Ok(());
        }
        let adapter = crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter(
            self.table.spec.warehouse,
        );
        let count = format!(
            "SELECT CAST(count(*) AS VARCHAR) FROM {}",
            adapter.render_qualified_name(into)
        );
        let rows: u64 = single_text(&self.destination, count)
            .await?
            .and_then(|text| text.parse().ok())
            .unwrap_or_default();
        let before = self.pieces.table_rows.unwrap_or(rows).min(rows);
        let group_rows = rows - before / DUCKDB_ROW_GROUP_ROWS * DUCKDB_ROW_GROUP_ROWS;
        let width = self.pieces.large / self.pieces.rows;
        let group = group_rows.saturating_mul(width);
        if group <= share.row_group_bytes {
            self.pieces.table_rows = Some(rows);
            return Ok(());
        }
        let needed = group.saturating_mul(100) / DUCKDB_ROW_GROUP_PERCENT;
        let full = (DUCKDB_ROW_GROUP_ROWS * width).saturating_mul(100) / DUCKDB_ROW_GROUP_PERCENT;
        Err(LoaderError::Invalid(format!(
            "{}: its last row group would hold {} rows with about {} each of values over {} ({}), more than a DuckDB checkpoint rewrites within its {} memory_limit; {} (a row group holds up to {} rows, so a table of rows this wide needs up to {} for DuckDB, {})",
            self.table.spec.destination.table,
            thousands(group_rows),
            memory_text(width),
            memory_text(LARGE_VALUE_BYTES as u64),
            memory_text(group),
            memory_text(share.memory_limit),
            needed_budget(needed),
            thousands(DUCKDB_ROW_GROUP_ROWS),
            memory_text(full),
            needed_budget(full)
        )))
    }

    /// Run a staged commit's `statements`, checking the table's last row group (see [`Self::check_row_group`]) before the transaction ends.
    pub(in crate::loader) async fn execute_checked(
        &mut self,
        mut statements: Vec<String>,
        into: &TableName,
    ) -> Result<(), LoaderError> {
        let checks = self.writer.memory_share().is_some()
            && self.pieces.large > 0
            && statements.last().is_some_and(|last| last == COMMIT_STATEMENT);
        if !checks {
            return execute(&self.destination, statements).await;
        }
        let commit = statements.pop();
        execute(&self.destination, statements).await?;
        if let Err(error) = self.check_row_group(into).await {
            let _ = execute(&self.destination, vec![ROLLBACK_STATEMENT.to_owned()]).await;
            return Err(error);
        }
        execute(&self.destination, commit.into_iter().collect()).await
    }
}
