//! Load the plan's `postgres_cdc` sources, each as one unit: its tables share one slot, which confirms a batch only once all of them have written it.

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::loader::_helpers::cdc_stream::CdcSource;
use crate::loader::models::{LoadRun, TableLoaded, TableOutcome, TablePlan};

/// Load each source of `tables` (unblocked `postgres_cdc` tables) in turn; every table of a source reports the source's error when it fails.
pub(in crate::loader) async fn load_cdc(
    run: &LoadRun<'_>,
    tables: &[&TablePlan],
) -> Vec<TableOutcome> {
    let mut sources: BTreeMap<&str, Vec<&TablePlan>> = BTreeMap::new();
    for table in tables {
        sources
            .entry(table.spec.source_name.as_str())
            .or_default()
            .push(table);
    }
    let mut outcomes: Vec<TableOutcome> = Vec::new();
    for (name, tables) in sources {
        let started = std::time::Instant::now();
        let first = tables.first().map(|table| &table.spec);
        let loaded = match (
            run.project.sources.get(name),
            first.and_then(|spec| spec.cdc.as_ref()),
            first.and_then(|spec| run.destinations.get(&spec.target)),
        ) {
            (Some(source), Some(cdc), Some(destination)) => {
                let unit = CdcSource {
                    run,
                    name,
                    source,
                    cdc,
                    tables: tables.clone(),
                    destination,
                };
                unit.load().await.map_err(|error| error.to_string())
            }
            _ => Err(format!(
                "source {name} is not a connected postgres_cdc source"
            )),
        };
        for table in &tables {
            let spec = &table.spec;
            outcomes.push(TableOutcome {
                table: sql_adapter(spec.warehouse).render_display_name(&spec.destination),
                loaded: loaded
                    .as_ref()
                    .map_err(Clone::clone)
                    .map(|rows| TableLoaded {
                        rows: rows
                            .get(&spec.destination.table)
                            .copied()
                            .unwrap_or_default(),
                        ..TableLoaded::default()
                    }),
                elapsed: started.elapsed(),
            });
        }
    }
    outcomes
}
