use std::collections::BTreeMap;

use crate::batching::models::{Batch, Partition};
use crate::config::models::LoadErrorPolicy;
use crate::delivery::_helpers::stage::split_parts;
use crate::delivery::models::{CheckTiming, Delivery, PendingLoads, StagedBatch};
use crate::stage_api::models::{FileLoad, LoadStatus};

pub(crate) const SENT_MS: i64 = 1_710_072_000_000;
pub(crate) const MINUTE_MS: i64 = 60_000;
pub(crate) const SECOND_MS: i64 = 1_000;
/// The default check timing: five minutes, then every ten.
pub(crate) const TIMING: CheckTiming = CheckTiming {
    check_after: std::time::Duration::from_secs(300),
    recheck_every: std::time::Duration::from_secs(600),
};

/// A batch's file keys and its last offset per partition.
pub(crate) type BatchSpec<'a> = (&'a [&'a str], &'a [(i32, i64)]);

pub(crate) fn partition(number: i32) -> Partition {
    Partition {
        topic: "orders".to_owned(),
        partition: number,
    }
}

pub(crate) fn offsets(pairs: &[(i32, i64)]) -> BTreeMap<Partition, i64> {
    pairs
        .iter()
        .map(|(number, offset)| (partition(*number), *offset))
        .collect()
}

/// A report entry; failures carry the first error `bad row`.
pub(crate) fn load(path: &str, status: LoadStatus) -> FileLoad {
    let failed = status != LoadStatus::Loaded && status != LoadStatus::InProgress;
    let first_error = failed.then(|| "bad row".to_owned());
    FileLoad {
        path: path.to_owned(),
        status,
        rows_inserted: 0,
        rows_parsed: 3,
        first_error,
    }
}

pub(crate) fn reports(reports: &[(&str, LoadStatus)]) -> Vec<FileLoad> {
    reports
        .iter()
        .map(|(path, status)| load(path, *status))
        .collect()
}

/// Batches sent at [`SENT_MS`], none of them received by the pipe before.
pub(crate) fn pending(batches: &[BatchSpec<'_>], policy: LoadErrorPolicy) -> PendingLoads {
    let mut loads = PendingLoads::new(policy, TIMING);
    for (keys, last) in batches {
        let keys: Vec<String> = keys.iter().map(|key| (*key).to_owned()).collect();
        loads.add(&keys, &[], offsets(last), SENT_MS);
    }
    loads
}

/// Envelope value columns with these names and types.
pub(crate) fn value_columns(columns: &[(&str, &str)]) -> Vec<crate::config::models::ColumnConfig> {
    columns
        .iter()
        .map(|(name, sql_type)| crate::config::models::ColumnConfig {
            name: (*name).to_owned(),
            field: crate::config::models::EnvelopeField::Value,
            sql_type: (*sql_type).to_owned(),
        })
        .collect()
}

/// The file names a Snowflake stage delivery stages `batch` under, kept for a retry as a failed upload keeps them.
pub(crate) fn stage_keys(delivery: &mut Delivery, batch: &Batch) -> Result<Vec<String>, String> {
    let Delivery::Stage(stage) = delivery else {
        return Err(format!("not a stage delivery: {delivery:?}"));
    };
    let staged = stage.staged_batch(batch, &split_parts(batch, false));
    stage.staged = Some(staged.clone());
    Ok(file_names(&staged))
}

/// The file names a Databricks `COPY INTO` delivery stages `batch` under, kept for a retry as a failed upload keeps them.
pub(crate) fn copy_keys(delivery: &mut Delivery, batch: &Batch) -> Result<Vec<String>, String> {
    let Delivery::Copy(copy) = delivery else {
        return Err(format!("not a COPY INTO delivery: {delivery:?}"));
    };
    let staged = copy.staged_batch(batch, &split_parts(batch, false));
    copy.staged = Some(staged.clone());
    Ok(file_names(&staged))
}

/// Each staged file's name, without its dated folder.
fn file_names(staged: &StagedBatch) -> Vec<String> {
    staged
        .files
        .iter()
        .map(|file| file.key.rsplit('/').next().unwrap_or_default().to_owned())
        .collect()
}
