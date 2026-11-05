use std::future::Future;

use crate::column_types::models::Warehouse;
use crate::external_stage::_helpers::create_stage::create_stage;
use crate::external_stage::_helpers::step_failure::step_failure;
use crate::external_stage::_helpers::table_stage::table_stage;
use crate::external_stage::main::is_not_found::is_not_found;
use crate::external_stage::main::stage_lookup::stage_lookup;
use crate::external_stage::main::stage_problem::stage_problem;
use crate::external_stage::main::stage_reference::stage_reference;
use crate::external_stage::models::{PlannedStage, RouteRequest, StepFailure};
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::probe_folder::probe_folder;
use crate::adapters::snowflake::constants::{
    EXTERNAL_STAGE_FOLDER, STAGE_PROBE_TABLE, USER_STAGE,
};
use crate::adapters::snowflake::main::show_stages::show_stages;
use crate::adapters::snowflake::main::streaming_client::streaming_client;
use crate::adapters::snowflake::models::{
    ExternalStage, ForcedStage, SnowflakeDestination, StageObject, StageRoute,
};
use crate::sql_api::main::execute_sql::execute_sql;
use crate::streaming_api::main::ingest_host::ingest_host;
use crate::adapters::contract::main::create_schema::create_schema;
use crate::adapters::contract::main::qualified_name::qualified_name;
use crate::adapters::contract::models::TableName;

pub(crate) fn step_name(step: &ForcedStage) -> String {
    match step {
        ForcedStage::User => format!("@{USER_STAGE}"),
        ForcedStage::Table => "@%table".to_owned(),
        ForcedStage::Streaming => "Snowpipe Streaming".to_owned(),
        ForcedStage::Insert => "INSERT".to_owned(),
        ForcedStage::Internal { name } => format!("@{name}"),
        ForcedStage::External(stage) => stage.location.to_string(),
    }
}

/// What `apply` runs to create the stage SQLLoad maintains, and the loader schema it needs.
pub(crate) fn planned_stage(stage: &ExternalStage) -> Option<PlannedStage> {
    let create = create_stage(stage)?;
    Some(PlannedStage {
        name: stage_reference(stage),
        url: stage.location.to_string(),
        statements: vec![create_schema(&stage.home, Warehouse::Snowflake), create],
    })
}

pub(crate) async fn probed_step(
    request: &RouteRequest<'_>,
    step: &ForcedStage,
    in_chain: bool,
) -> Result<StageRoute, StepFailure> {
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
            let client = streaming_client(snowflake, request.destination);
            ingest_host(&client)
                .await
                .map(|_| false)
                .map_err(|error| step_failure(&error))
        })
        .await
        .map(|_| StageRoute::Streaming {
            may_fall_back: in_chain,
        }),
        ForcedStage::Insert => Ok(StageRoute::Insert),
        ForcedStage::External(_) => Err(StepFailure::Failed(
            "an external stage is not a step of the chain".to_owned(),
        )),
    }
}

/// `@%table`: the target's for an owned direct append (a table this load creates will be owned), otherwise the staging table's, which the load always owns. Neither of those two exists yet, so a temporary table stands in for them.
async fn table_step(request: &RouteRequest<'_>) -> Result<StageRoute, StepFailure> {
    let snowflake = request.snowflake;
    let target = request.destination;
    if request.direct_append && request.table_exists {
        match probe_put(snowflake, &table_stage(target)).await {
            Ok(_) => return Ok(StageRoute::TargetTable),
            Err(StepFailure::Unusable(reason)) => {
                tracing::debug!(%reason, table = %target.table, "the table's own stage cannot be used");
            }
            Err(failed) => return Err(failed),
        }
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
            .map_err(|error| step_failure(&error))?;
        probe_put(snowflake, &table_stage(&probe)).await
    })
    .await?;
    Ok(match request.direct_append && !request.table_exists {
        true => StageRoute::TargetTable,
        false => StageRoute::StagingTable,
    })
}

/// The no-bytes `PUT` that fetches a stage's upload credentials.
async fn probe_put(snowflake: &SnowflakeDestination, stage: &str) -> Result<bool, StepFailure> {
    snowflake
        .driver
        .stage_info(stage)
        .await
        .map(|_| false)
        .map_err(|error| step_failure(&error))
}

/// `probe`'s answer for `key`, asked once per run.
async fn checked(
    snowflake: &SnowflakeDestination,
    key: String,
    probe: impl Future<Output = Result<bool, StepFailure>>,
) -> Result<bool, StepFailure> {
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

/// Whether the external stage works: SQLLoad lists its URL; the stage reads that URL and lists its own folder, or (one SQLLoad maintains) is missing, its integration visible, so `apply` creates it (`Ok(true)`). Everything here fails a forced route, so the reason is all that matters.
pub(crate) async fn checked_external(
    snowflake: &SnowflakeDestination,
    stage: &ExternalStage,
) -> Result<bool, StepFailure> {
    let reference = stage_reference(stage);
    checked(snowflake, reference.clone(), async {
        external_state(snowflake, stage, &reference)
            .await
            .map_err(StepFailure::Failed)
    })
    .await
}

async fn external_state(
    snowflake: &SnowflakeDestination,
    stage: &ExternalStage,
    reference: &str,
) -> Result<bool, String> {
    let failed = |error: &dyn std::fmt::Display| format!("{reference}: {error}");
    let folder = open_folder(&stage.location).map_err(|error| failed(&error))?;
    probe_folder(&folder)
        .await
        .map_err(|error| format!("SQLLoad cannot list {}: {error}", stage.location))?;
    let lookup = stage_lookup(stage);
    let shown = match show_stages(snowflake, &lookup.query).await {
        Ok(shown) => shown,
        Err(error) if is_not_found(&error) => Vec::new(),
        Err(error) => return Err(failed(&error)),
    };
    let exists = shown
        .iter()
        .any(|found| found.name.eq_ignore_ascii_case(&lookup.name));
    if let (false, StageObject::Managed { integration }) = (exists, &stage.object) {
        let query = format!(
            "SHOW INTEGRATIONS LIKE '{}'",
            integration.replace('\'', "''")
        );
        let found = execute_sql(&snowflake.sql, &query)
            .await
            .map_err(|error| failed(&error))?;
        let visible = found.rows.iter().any(|row| {
            found
                .value(row, "name")
                .is_some_and(|name| name.eq_ignore_ascii_case(integration))
        });
        return match visible {
            false => Err(format!(
                "storage integration {integration} does not exist or this role has no USAGE on it"
            )),
            true => Ok(true),
        };
    }
    if let Some(problem) = stage_problem(stage, &lookup, &shown) {
        return Err(problem);
    }
    execute_sql(
        &snowflake.sql,
        &format!("LIST {reference}/{EXTERNAL_STAGE_FOLDER}/"),
    )
    .await
    .map(|_| false)
    .map_err(|error| failed(&error))
}
