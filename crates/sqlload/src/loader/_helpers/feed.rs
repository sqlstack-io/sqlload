use arrow_array::RecordBatch;

use crate::batch_plan::main::cut_index::cut_index;
use crate::batch_plan::main::last_value::last_value;
use crate::batch_plan::models::CursorWindow;
use crate::loader::_helpers::sources::{ReadPart, read_source};
use crate::loader::_helpers::window::source_filter;
use crate::loader::constants::{EXCLUSIVE_BOUND, INCLUSIVE_BOUND};
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    CommitRange, CommitRule, Destination, Publisher, RunEntry, SourceReader, TablePlan,
};
use crate::postgres_source::_helpers::type_map::quote_identifier;

/// Load a table's rows from `reader` commit by commit, as its plan cuts them; a failure stops the table there, keeping what committed before.
pub(crate) async fn publish_table(
    reader: &SourceReader,
    table: &TablePlan,
    destination: &Destination,
    run: Option<&RunEntry>,
) -> Result<u64, LoaderError> {
    let publisher = Publisher::open(table.clone(), destination, run.cloned()).await?;
    let published = feed(reader, table, publisher).await;
    let (mut publisher, result) = published;
    publisher.close().await;
    result
}

/// Pass the rows to `publisher`, cutting commits where the plan says; returns the publisher for its clean-up, and the rows committed.
async fn feed(
    reader: &SourceReader,
    table: &TablePlan,
    mut publisher: Publisher,
) -> (Publisher, Result<u64, LoaderError>) {
    let fed = async {
        match &table.commits {
            CommitRule::Windows(windows) => {
                for window in windows {
                    let part = ReadPart {
                        filter: window_filter(table, window),
                        ..ReadPart::default()
                    };
                    read_source(reader, table, part, async |batch| {
                        stage(&mut publisher, batch).await
                    })
                    .await?;
                    publisher
                        .cut(CommitRange::Window {
                            from: window.from.clone(),
                            to: window.to.clone(),
                        })
                        .await?;
                }
            }
            CommitRule::FileGroups(groups) => {
                let planned = table.files.as_ref().map(|files| files.files.as_slice());
                for group in groups {
                    let files = group
                        .files
                        .iter()
                        .filter_map(|index| planned.and_then(|files| files.get(*index)).cloned())
                        .collect();
                    let part = ReadPart {
                        files: Some(files),
                        ..ReadPart::default()
                    };
                    read_source(reader, table, part, async |batch| {
                        stage(&mut publisher, batch).await
                    })
                    .await?;
                    let range = match group.values.is_empty() {
                        true => CommitRange::Planned,
                        false => CommitRange::Values(group.values.clone()),
                    };
                    publisher.cut(range).await?;
                }
            }
            CommitRule::Ordered { column } => {
                let part = ReadPart {
                    order_by: Some(quote_identifier(column)),
                    ..ReadPart::default()
                };
                let mut last: Option<String> = None;
                read_source(reader, table, part, async |batch| {
                    let values = batch
                        .column_by_name(column)
                        .cloned()
                        .ok_or_else(|| format!("the read has no cursor column {column}"))?;
                    let at = match publisher.full() {
                        true => cut_index(values.as_ref(), last.as_deref())?,
                        false => batch.num_rows(),
                    };
                    last = last_value(values.as_ref())?.or(last.take());
                    stage(&mut publisher, batch.slice(0, at)).await?;
                    if at < batch.num_rows() {
                        publisher
                            .cut(CommitRange::Planned)
                            .await
                            .map_err(|error| error.to_string())?;
                        stage(&mut publisher, batch.slice(at, batch.num_rows() - at)).await?;
                    }
                    Ok(())
                })
                .await?;
            }
            CommitRule::Size => {
                read_source(reader, table, ReadPart::default(), async |batch| {
                    stage(&mut publisher, batch).await?;
                    match publisher.full() {
                        true => publisher
                            .cut(CommitRange::Planned)
                            .await
                            .map_err(|error| error.to_string()),
                        false => Ok(()),
                    }
                })
                .await?;
            }
            CommitRule::Whole => {
                read_source(reader, table, ReadPart::default(), async |batch| {
                    stage(&mut publisher, batch).await
                })
                .await?;
            }
        }
        publisher.finish(None).await
    }
    .await;
    (publisher, fed)
}

async fn stage(publisher: &mut Publisher, batch: RecordBatch) -> Result<(), String> {
    publisher
        .stage(batch, None)
        .await
        .map_err(|error| error.to_string())
}

/// The Postgres filter reading one cursor window.
fn window_filter(table: &TablePlan, window: &CursorWindow) -> Option<String> {
    let cursor = table.spec.cursor.as_ref()?;
    let expression = table
        .read_columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(&cursor.column))
        .map(|column| column.expression.clone())?;
    let operator = match window.inclusive {
        true => INCLUSIVE_BOUND,
        false => EXCLUSIVE_BOUND,
    };
    source_filter(
        &expression,
        window.from.as_deref().map(|from| (operator, from)),
        window.to.as_deref(),
    )
}
