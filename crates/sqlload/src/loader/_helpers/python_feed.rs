use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, SchemaRef};

use crate::batch_plan::models::CursorWindow;
use crate::loader::_helpers::destination::execute;
use crate::loader::_helpers::publish::staged_columns;
use crate::loader::_helpers::python::{
    YieldedData, change_lines, declared_problems, inferred_table, typed_batch,
};
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::run_log::run_entry;
use crate::loader::_helpers::table_plan::{plan_table, schema_statements};
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    CommitRange, CommitRule, Destination, Publisher, PythonFeed, PythonLoad, ReadWindow, TablePlan,
};
use crate::python_sources::types::{BatchStream, Streamed};
use crate::row_batches::main::merge_schema::merge_schema;
use crate::row_batches::main::settled_schema::settled_schema;
use crate::write_sql::main::staging_changes::staging_changes;

/// The JSON a table function's `ctx.state` holds when it holds nothing.
const NO_STATE: &str = "null";

impl PythonFeed {
    /// Read one call of the table function to its end: each batch is typed and staged as it arrives (the plan and the table grow with new columns first), a logged table's `ctx.state` follows the rows, and a table without a cursor commits about every 1 GB.
    pub(crate) async fn read(
        &mut self,
        load: &PythonLoad<'_>,
        mut stream: Box<dyn BatchStream>,
        window: Option<&CursorWindow>,
    ) -> Result<(), LoaderError> {
        loop {
            let (returned, next) = tokio::task::spawn_blocking(move || {
                let next = stream.next_batch();
                (stream, next)
            })
            .await
            .map_err(|error| {
                LoaderError::Invalid(format!("the table function's thread failed: {error}"))
            })?;
            stream = returned;
            match next {
                None => return Ok(()),
                Some(Err(reason)) => return Err(LoaderError::Invalid(reason)),
                Some(Ok(Streamed::State(state))) => {
                    let state = (state != NO_STATE).then_some(state);
                    if let Some(publisher) = &mut self.publisher {
                        publisher.state = state.clone();
                    }
                    self.state = state;
                }
                Some(Ok(Streamed::Batch(batch))) => self.batch(load, batch, window).await?,
            }
        }
    }

    /// Stage one batch: when it changes the merged schema, plan the table for the data so far first (one read of the live columns, the schema changes applied once).
    async fn batch(
        &mut self,
        load: &PythonLoad<'_>,
        batch: RecordBatch,
        window: Option<&CursorWindow>,
    ) -> Result<(), LoaderError> {
        let merged = merge_schema(&self.merged, &batch.schema());
        self.rows += batch.num_rows() as u64;
        if merged != self.merged || self.publisher.is_none() {
            self.merged = merged;
            self.plan_data(load, false).await?;
        }
        let schema: SchemaRef = Arc::new(settled_schema(&self.merged));
        let read = window.map(|window| self.read_window(window));
        let (typed, outside) = typed_batch(&batch, &self.table, &schema, read.as_ref())?;
        self.outside += outside as u64;
        let sized = self.table.commits == CommitRule::Size;
        let publisher = self.publisher(load).await?;
        publisher.stage(typed, None).await?;
        match (sized, publisher.full()) {
            (true, true) => publisher.cut(CommitRange::Planned).await,
            _ => Ok(()),
        }
    }

    /// Plan the table for the data seen so far and bring the table (and the staging table) in line: created on the first batch, altered as later batches add or widen columns; columns that only held NULL wait for a typed value, or become text at the end (`settle`).
    pub(crate) async fn plan_data(
        &mut self,
        load: &PythonLoad<'_>,
        settle: bool,
    ) -> Result<(), LoaderError> {
        let nulls: BTreeSet<String> = self
            .merged
            .fields()
            .iter()
            .filter(|field| *field.data_type() == DataType::Null)
            .map(|field| field.name().clone())
            .collect();
        let live = self.table.live.as_deref();
        let typed_elsewhere = |name: &str| {
            live.is_some_and(|live| {
                live.iter()
                    .any(|column| column.name.eq_ignore_ascii_case(name))
            }) || load.table.spec.pinned.keys().any(|pinned| pinned == name)
        };
        let fields: Vec<_> = self
            .merged
            .fields()
            .iter()
            .filter(|field| {
                settle || !nulls.contains(field.name()) || typed_elsewhere(field.name())
            })
            .cloned()
            .collect();
        let schema = settled_schema(&Schema::new(fields));
        let data = YieldedData {
            schema: &schema,
            null_columns: &nulls,
            rows: self.rows,
        };
        let destination = match &self.publisher {
            Some(publisher) => &publisher.destination,
            None => load.destination,
        };
        let replanned = replan(load, &self.table, &data, destination).await?;
        let old = staged_columns(&self.table);
        let statements = schema_statements(&replanned);
        if let Some(statements) = statements {
            execute(destination, statements).await?;
            self.changes.extend(change_lines(&replanned));
        }
        if let Some(publisher) = &mut self.publisher {
            if let Some(staging) = &publisher.staging {
                let changes = staging_changes(
                    staging,
                    (&old, &staged_columns(&replanned)),
                    replanned.spec.warehouse,
                );
                execute(&publisher.destination, changes).await?;
            }
            publisher.table = replanned.clone();
        }
        self.table = TablePlan {
            live: replanned.live.clone().or(self.table.live.clone()),
            ..replanned
        };
        Ok(())
    }

    /// The publisher, opened on first use with the table as planned for the data.
    pub(crate) async fn publisher(
        &mut self,
        load: &PythonLoad<'_>,
    ) -> Result<&mut Publisher, LoaderError> {
        if self.publisher.is_none() {
            let run = run_entry(
                load,
                &self.table,
                (self.state.clone(), self.reconciled.clone()),
            );
            let mut publisher = Publisher::open(self.table.clone(), load.destination, run).await?;
            publisher.state = self.state.clone();
            self.publisher = Some(publisher);
        }
        self.publisher
            .as_mut()
            .ok_or_else(|| LoaderError::Invalid("the table's load could not start".to_owned()))
    }

    fn read_window(&self, window: &CursorWindow) -> ReadWindow {
        ReadWindow {
            column: self
                .table
                .spec
                .cursor
                .as_ref()
                .map(|cursor| cursor.column.clone())
                .unwrap_or_default(),
            from: window.from.clone(),
            to: window.to.clone(),
            inclusive: window.inclusive,
            maximum: None,
        }
    }
}

/// `planned` planned again for the columns `data` has; the cursor window, write mode, commits and run log stay as planned before the read, so they match what the function was asked for.
async fn replan(
    load: &PythonLoad<'_>,
    planned: &TablePlan,
    data: &YieldedData<'_>,
    destination: &Destination,
) -> Result<TablePlan, LoaderError> {
    let spec = &planned.spec;
    let problems = declared_problems(spec, data);
    if !problems.is_empty() {
        return Err(LoaderError::Invalid(problems.join("; ")));
    }
    let declared = spec
        .python
        .as_ref()
        .map(|python| python.declared.clone())
        .unwrap_or_default();
    let table = inferred_table(
        &load.table.spec.source_table,
        &declared,
        data,
        planned.live.as_deref(),
    );
    let mut project = load.project.clone();
    if let Some(source) = project.sources.get_mut(&spec.source_name) {
        source.tables.retain(|name, _| *name == table.name);
    }
    let resolved = resolve_tables(&project, &spec.source_name, &[table]);
    let problems: Vec<String> = resolved
        .problems
        .into_iter()
        .chain(resolved.skipped.into_iter().map(|skipped| skipped.reason))
        .collect();
    let mut inferred = match (problems.is_empty(), resolved.specs.into_iter().next()) {
        (true, Some(inferred)) => inferred,
        _ => return Err(LoaderError::Invalid(problems.join("; "))),
    };
    inferred.python = spec.python.clone();
    inferred.listed = true;
    let replanned = plan_table(&inferred, destination, load.options).await?;
    if let Some(blocked) = &replanned.schema.blocked {
        return Err(LoaderError::Invalid(format!(
            "the yielded data does not fit the table: {}",
            blocked.reasons.join("; ")
        )));
    }
    Ok(TablePlan {
        mode: planned.mode.clone(),
        window: planned.window.clone(),
        cursor_note: planned.cursor_note.clone(),
        logged: planned.logged.clone(),
        commits: planned.commits.clone(),
        ..replanned
    })
}
