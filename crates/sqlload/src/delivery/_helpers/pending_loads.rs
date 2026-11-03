use std::collections::BTreeMap;

use crate::batching::models::Partition;
use crate::config::models::LoadErrorPolicy;
use crate::delivery::constants::{CHECK_RETRY, HISTORY_MARGIN, RECEIVED_BEFORE_LOOKBACK};
use crate::delivery::models::{
    CheckTiming, FileState, PendingBatch, PendingFile, PendingLoads, Settled,
};
use crate::stage_api::models::{FileLoad, LoadStatus};

impl PendingLoads {
    pub(crate) fn new(policy: LoadErrorPolicy, timing: CheckTiming) -> Self {
        Self {
            batches: Default::default(),
            policy,
            timing,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Files sent but not committed yet, including loaded files behind an older pending batch.
    pub(crate) fn file_count(&self) -> usize {
        self.files().count()
    }

    pub(crate) fn oldest_sent_ms(&self) -> Option<i64> {
        self.files().map(|file| file.first_sent_ms).min()
    }

    /// Track a batch sent to `insertFiles` at `now`; `skipped` are paths the pipe said it had received before, checked on the next poll.
    pub(crate) fn add(
        &mut self,
        keys: &[String],
        skipped: &[String],
        offsets: BTreeMap<Partition, i64>,
        now: i64,
    ) {
        let check_after = self.timing.check_after.as_millis() as i64;
        let files: Vec<PendingFile> = keys
            .iter()
            .map(|key| {
                let received_before = is_listed(skipped, key);
                PendingFile {
                    key: key.clone(),
                    first_sent_ms: now,
                    resent: false,
                    received_before,
                    next_check_ms: match received_before {
                        true => now,
                        false => now + check_after,
                    },
                    last_check_ms: None,
                    failed_checks: 0,
                    state: FileState::Waiting,
                }
            })
            .collect();
        self.batches.push_back(PendingBatch { files, offsets });
    }

    /// Record reported loads for files still waiting or loading.
    pub(crate) fn apply(&mut self, loads: &[FileLoad]) {
        for load in loads {
            for file in self.files_mut() {
                if !same_path(&load.path, &file.key) || !is_unsettled(file) {
                    continue;
                }
                file.state = match load.status {
                    LoadStatus::Loaded => FileState::Loaded,
                    LoadStatus::InProgress => FileState::InProgress,
                    LoadStatus::Failed | LoadStatus::PartiallyLoaded => {
                        FileState::Failed(failure(load))
                    }
                };
            }
        }
    }

    /// Unsettled files, reported as loading or not at all, whose next check is due.
    pub(crate) fn overdue(&self, now: i64) -> Vec<String> {
        self.files()
            .filter(|file| is_unsettled(file) && now >= file.next_check_ms)
            .map(|file| file.key.clone())
            .collect()
    }

    /// The report missed events: check every unsettled file on this poll unless it was checked recently.
    pub(crate) fn mark_all_due(&mut self, now: i64) {
        let recent = self.retry_ms();
        for file in self.files_mut().filter(|file| is_unsettled(file)) {
            if file.last_check_ms.is_none_or(|last| now - last >= recent) {
                file.next_check_ms = file.next_check_ms.min(now);
            }
        }
    }

    /// A check found no report and the object in place: check again after `pending_recheck_every`.
    pub(crate) fn checked(&mut self, keys: &[String], now: i64) {
        let recheck = self.timing.recheck_every.as_millis() as i64;
        for file in self.files_mut().filter(|file| keys.contains(&file.key)) {
            file.last_check_ms = Some(now);
            file.next_check_ms = now + recheck;
            file.failed_checks = 0;
        }
    }

    /// A check could not run: retry with a doubling delay, at most `pending_recheck_every`.
    pub(crate) fn check_failed(&mut self, keys: &[String], now: i64) {
        let retry = self.retry_ms();
        let recheck = self.timing.recheck_every.as_millis() as i64;
        for file in self.files_mut().filter(|file| keys.contains(&file.key)) {
            let factor = 1i64 << file.failed_checks.min(16);
            file.failed_checks += 1;
            file.next_check_ms = now + retry.saturating_mul(factor).min(recheck);
        }
    }

    /// Where a history scan for `keys` starts: a day back for files the pipe had received before, otherwise just before the first send.
    pub(crate) fn history_start(&self, keys: &[String]) -> Option<i64> {
        self.files()
            .filter(|file| keys.contains(&file.key))
            .map(|file| {
                let lookback = match file.received_before {
                    true => RECEIVED_BEFORE_LOOKBACK,
                    false => HISTORY_MARGIN,
                };
                file.first_sent_ms - lookback.as_millis() as i64
            })
            .min()
    }

    /// True when the file was sent again, or the pipe already had it, so another `insertFiles` cannot help.
    pub(crate) fn is_acknowledged(&self, key: &str) -> bool {
        self.files()
            .any(|file| file.key == key && (file.resent || file.received_before))
    }

    pub(crate) fn resend(&mut self, key: &str, skipped: bool) {
        for file in self.files_mut().filter(|file| file.key == key) {
            file.resent = true;
            file.received_before |= skipped;
        }
    }

    pub(crate) fn fail(&mut self, key: &str, reason: &str) {
        for file in self.files_mut().filter(|file| file.key == key) {
            file.state = FileState::Failed(format!("{}: {reason}", file.key));
        }
    }

    /// Apply the failure policy, then take the oldest batches whose files are all settled, stopping at the first unsettled one.
    pub(crate) fn settle(&mut self) -> Settled {
        let mut settled = Settled::default();
        let policy = self.policy;
        for file in self.files_mut() {
            let FileState::Failed(reason) = &file.state else {
                continue;
            };
            match policy {
                LoadErrorPolicy::Halt => {
                    settled.halted.get_or_insert_with(|| reason.clone());
                }
                LoadErrorPolicy::Skip => {
                    settled.skipped.push((file.key.clone(), reason.clone()));
                    file.state = FileState::Skipped;
                }
            }
        }
        while let Some(batch) = self.batches.front() {
            if !batch.files.iter().all(is_done) {
                break;
            }
            if let Some(batch) = self.batches.pop_front() {
                for (partition, offset) in batch.offsets {
                    let entry = settled.confirmed.entry(partition).or_insert(offset);
                    *entry = (*entry).max(offset);
                }
            }
        }
        settled
    }

    /// Stop tracking revoked partitions; batches left with no offsets are dropped.
    pub(crate) fn forget(&mut self, partitions: &[Partition]) {
        for batch in &mut self.batches {
            batch
                .offsets
                .retain(|partition, _| !partitions.contains(partition));
        }
        self.batches.retain(|batch| !batch.offsets.is_empty());
    }

    pub(crate) fn clear(&mut self) {
        self.batches.clear();
    }

    /// The first delay after a check that could not run: a minute, or less when checks are configured sooner.
    fn retry_ms(&self) -> i64 {
        CHECK_RETRY.min(self.timing.check_after).as_millis() as i64
    }

    fn files(&self) -> impl Iterator<Item = &PendingFile> {
        self.batches.iter().flat_map(|batch| batch.files.iter())
    }

    fn files_mut(&mut self) -> impl Iterator<Item = &mut PendingFile> {
        self.batches
            .iter_mut()
            .flat_map(|batch| batch.files.iter_mut())
    }
}

fn is_listed(paths: &[String], key: &str) -> bool {
    paths.iter().any(|path| same_path(path, key))
}

fn is_done(file: &PendingFile) -> bool {
    matches!(file.state, FileState::Loaded | FileState::Skipped)
}

fn is_unsettled(file: &PendingFile) -> bool {
    matches!(file.state, FileState::Waiting | FileState::InProgress)
}

/// Report paths are relative to the pipe's location, as `insertFiles` paths are; a leading `/` is ignored.
pub(crate) fn same_path(reported: &str, key: &str) -> bool {
    reported.trim_start_matches('/') == key.trim_start_matches('/')
}

fn failure(load: &FileLoad) -> String {
    let status = match load.status {
        LoadStatus::PartiallyLoaded => format!(
            "partially loaded ({} of {} rows)",
            load.rows_inserted, load.rows_parsed
        ),
        _ => "load failed".to_owned(),
    };
    match &load.first_error {
        Some(error) => format!("{}: {status}: {error}", load.path),
        None => format!("{}: {status}", load.path),
    }
}
