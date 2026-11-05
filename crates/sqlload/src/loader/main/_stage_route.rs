//! Choose how one table's rows reach Snowflake, trying what the role can use without changing anything.

use std::future::Future;

use crate::column_types::models::Warehouse;
use crate::external_stage::main::create_stage::create_stage;
use crate::external_stage::main::pick_route::pick_route;
use crate::external_stage::main::route_reference::table_stage;
use crate::external_stage::main::stage_lookup::stage_lookup;
use crate::external_stage::main::stage_problem::stage_problem;
use crate::external_stage::main::stage_reference::stage_reference;
use crate::external_stage::models::{RouteChoice, StepOutcome};
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::probe_folder::probe_folder;
use crate::loader::models::{PlannedStage, TableSpec};
use crate::snowflake_destination::constants::{STAGE_PROBE_TABLE, USER_STAGE};
use crate::snowflake_destination::main::show_stages::show_stages;
use crate::snowflake_destination::models::{
    ExternalStage, ForcedStage, SnowflakeDestination, StageObject, StageRoute,
};
use crate::sql_api::main::execute_sql::execute_sql;
use crate::streaming_api::main::ingest_host::ingest_host;
use crate::streaming_api::models::StreamingClient;
use crate::write_sql::main::create_schema::create_schema;
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::models::TableName;

/// What decides a table's route besides its spec.
#[derive(Debug, Clone, Copy)]
pub(in crate::loader) struct RouteRequest<'a> {
    pub(in crate::loader) spec: &'a TableSpec,
    pub(in crate::loader) table_exists: bool,
    /// The load would `COPY` straight into the target (`append`, not logged).
    pub(in crate::loader) direct_append: bool,
    pub(in crate::loader) snowflake: &'a SnowflakeDestination,
}

/// The table's route: what the connection's `stage` forces (failing with the reason if it cannot be used), else the first of `@~`, `@%table`, Snowpipe Streaming and `INSERT` the role can use; each probe changes nothing and is asked once per run. Also the stage `apply` must create, if any.
pub(in crate::loader) async fn stage_route(
    request: &RouteRequest<'_>,
) -> Result<(RouteChoice, Option<PlannedStage>), String> {
    match &request.snowflake.stage {
        None => {
            let mut outcomes: Vec<(String, StepOutcome)> = Vec::new();
            for step in [
                ForcedStage::User,
                ForcedStage::Table,
                ForcedStage::Streaming,
            ] {
                let outcome = forced_step(request, &step, true).await;
                let usable = outcome.is_ok();
                outcomes.push((step_name(&step), outcome));
                if usable {
                    return Ok((pick_route(false, outcomes)?, None));
                }
            }
            outcomes.push((step_name(&ForcedStage::Insert), Ok(StageRoute::Insert)));
            Ok((pick_route(false, outcomes)?, None))
        }
        Some(ForcedStage::External(stage)) => {
            let target = &request.spec.destination;
            let outcome = checked_external(request.snowflake, stage, target).await;
            let creates = matches!(outcome, Ok(true));
            let route = outcome.map(|_| StageRoute::External(stage.clone()));
            let choice = pick_route(true, vec![(stage.location.to_string(), route)])?;
            let planned = creates.then(|| planned_stage(stage, target)).flatten();
            Ok((choice, planned))
        }
        Some(step) => {
            let outcome = forced_step(request, step, false).await;
            Ok((pick_route(true, vec![(step_name(step), outcome)])?, None))
        }
    }
}

fn step_name(step: &ForcedStage) -> String {
    match step {
        ForcedStage::User => format!("@{USER_STAGE}"),
        ForcedStage::Table => "@%table".to_owned(),
        ForcedStage::Streaming => "Snowpipe Streaming".to_owned(),
        ForcedStage::Insert => "INSERT".to_owned(),
        ForcedStage::Internal { name } => format!("@{name}"),
        ForcedStage::External(stage) => stage.location.to_string(),
    }
}

/// What `apply` runs to create the stage SQLLoad maintains in `target`'s schema.
fn planned_stage(stage: &ExternalStage, target: &TableName) -> Option<PlannedStage> {
    let create = create_stage(stage, target)?;
    Some(PlannedStage {
        name: stage_reference(stage, target),
        url: stage.location.to_string(),
        statements: vec![create_schema(target, Warehouse::Snowflake), create],
    })
}

async fn forced_step(
    request: &RouteRequest<'_>,
    step: &ForcedStage,
    in_chain: bool,
) -> StepOutcome {
    let snowflake = request.snowflake;
    match step {
        ForcedStage::User => checked(snowflake, format!("@{USER_STAGE}"), async {
            probe_put(snowflake, USER_STAGE).await
        })
        .await
        .map(|_| StageRoute::User),
        ForcedStage::Internal { name } => checked(snowflake, format!("@{name}"), async {
            probe_put(snowflake, name).await
        })
        .await
        .map(|_| StageRoute::Internal { name: name.clone() }),
        ForcedStage::Table => table_step(request).await,
        ForcedStage::Streaming => checked(snowflake, "streaming".to_owned(), async {
            let client = streaming_client(snowflake, &request.spec.destination);
            ingest_host(&client)
                .await
                .map(|_| false)
                .map_err(|error| error.to_string())
        })
        .await
        .map(|_| StageRoute::Streaming {
            may_fall_back: in_chain,
        }),
        ForcedStage::Insert => Ok(StageRoute::Insert),
        ForcedStage::External(_) => Err("an external stage is not a step of the chain".to_owned()),
    }
}

/// `@%table`: the target's for an owned direct append (a table this load creates will be owned), otherwise the staging table's, which the load always owns. Neither of those two exists yet, so a temporary table stands in for them.
async fn table_step(request: &RouteRequest<'_>) -> StepOutcome {
    let snowflake = request.snowflake;
    let target = &request.spec.destination;
    if request.direct_append
        && request.table_exists
        && probe_put(snowflake, &table_stage(target)).await.is_ok()
    {
        return Ok(StageRoute::TargetTable);
    }
    let probe = TableName {
        table: STAGE_PROBE_TABLE.to_owned(),
        ..target.clone()
    };
    let key = format!("@%{}.{}", target.schema, STAGE_PROBE_TABLE);
    checked(snowflake, key, async {
        let name = qualified_name(&probe, Warehouse::Snowflake);
        snowflake
            .driver
            .run(&format!(
                "CREATE TEMPORARY TABLE IF NOT EXISTS {name} (ID INT)"
            ))
            .await
            .map_err(|error| error.to_string())?;
        probe_put(snowflake, &table_stage(&probe)).await
    })
    .await?;
    Ok(match request.direct_append && !request.table_exists {
        true => StageRoute::TargetTable,
        false => StageRoute::StagingTable,
    })
}

/// The no-bytes `PUT` that fetches a stage's upload credentials.
async fn probe_put(snowflake: &SnowflakeDestination, stage: &str) -> Result<bool, String> {
    snowflake
        .driver
        .stage_info(stage)
        .await
        .map(|_| false)
        .map_err(|error| error.to_string())
}

/// A streaming client for the schema of `table` on this account.
pub(crate) fn streaming_client(
    snowflake: &SnowflakeDestination,
    table: &TableName,
) -> StreamingClient {
    let stored =
        |name: &str| crate::write_sql::main::stored_name::stored_name(name, Warehouse::Snowflake);
    let mut config = snowflake.config.clone();
    config.database = stored(table.database.as_deref().unwrap_or(&snowflake.database));
    config.schema = stored(&table.schema);
    StreamingClient::new(snowflake.http.clone(), snowflake.auth.clone(), &config)
}

/// `probe`'s answer for `key`, asked once per run.
async fn checked(
    snowflake: &SnowflakeDestination,
    key: String,
    probe: impl Future<Output = Result<bool, String>>,
) -> Result<bool, String> {
    let known = match snowflake.stage_checks.lock() {
        Ok(checks) => checks.get(&key).cloned(),
        Err(_) => None,
    };
    if let Some(known) = known {
        return known;
    }
    let answer = probe.await;
    if let Ok(mut checks) = snowflake.stage_checks.lock() {
        checks.insert(key, answer.clone());
    }
    answer
}

/// Whether the external stage serving `target`'s schema works: SQLLoad lists its URL; the stage reads that URL and lists, or (one SQLLoad maintains) is missing, its integration visible, so `apply` creates it (`Ok(true)`).
async fn checked_external(
    snowflake: &SnowflakeDestination,
    stage: &ExternalStage,
    target: &TableName,
) -> Result<bool, String> {
    let reference = stage_reference(stage, target);
    checked(snowflake, reference.clone(), async {
        let failed = |error: &dyn std::fmt::Display| format!("{reference}: {error}");
        let folder = open_folder(&stage.location).map_err(|error| failed(&error))?;
        probe_folder(&folder).await.map_err(|error| {
            format!("SQLLoad cannot list {}: {error}", stage.location)
        })?;
        let lookup = stage_lookup(stage, target);
        let shown = match show_stages(snowflake, &lookup.query).await {
            Ok(shown) => shown,
            Err(error) if error.to_string().contains("does not exist") => Vec::new(),
            Err(error) => return Err(failed(&error)),
        };
        let exists = shown
            .iter()
            .any(|found| found.name.eq_ignore_ascii_case(&lookup.name));
        if let (false, StageObject::Managed { integration }) = (exists, &stage.object) {
            let query = format!("SHOW INTEGRATIONS LIKE '{}'", integration.replace('\'', "''"));
            let found = execute_sql(&snowflake.sql, &query)
                .await
                .map_err(|error| failed(&error))?;
            return match found.rows.is_empty() {
                true => Err(format!(
                    "storage integration {integration} does not exist or this role has no USAGE on it"
                )),
                false => Ok(true),
            };
        }
        if let Some(problem) = stage_problem(stage, &lookup, &shown) {
            return Err(problem);
        }
        execute_sql(&snowflake.sql, &format!("LIST {reference}/"))
            .await
            .map(|_| false)
            .map_err(|error| failed(&error))
    })
    .await
}
