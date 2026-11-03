use std::collections::BTreeMap;

use crate::batching::models::Partition;
use crate::config::models::LoadErrorPolicy;
use crate::delivery::models::PendingLoads;
use crate::stage_api::models::{FileLoad, LoadStatus};

pub(crate) const SENT_MS: i64 = 1_710_072_000_000;
pub(crate) const MINUTE_MS: i64 = 60_000;

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
    let mut loads = PendingLoads::new(policy);
    for (keys, last) in batches {
        let keys: Vec<String> = keys.iter().map(|key| (*key).to_owned()).collect();
        loads.add(&keys, &[], offsets(last), SENT_MS);
    }
    loads
}
