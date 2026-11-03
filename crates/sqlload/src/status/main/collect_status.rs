//! The `status` command.

use std::collections::BTreeMap;

use crate::config::models::{Config, StagingKind};
use crate::exporter::models::Clients;
use crate::kafka::main::group_lag::group_lag;
use crate::kafka::main::list_topics::list_topics;
use crate::naming::main::resolve_streams::resolve_streams;
use crate::status::_helpers::channels::{partition_status, stream_channels};
use crate::status::models::{PartitionStatus, StreamStatus};

/// Kafka lag for every stream and, in streaming mode, each channel's committed offset and rejected rows.
pub async fn collect_status(
    config: &Config,
    clients: &Clients,
) -> Result<Vec<StreamStatus>, String> {
    let topics = list_topics(config).await?;
    let streams = resolve_streams(config, &topics).map_err(|problems| problems.join("; "))?;
    let mut statuses: Vec<StreamStatus> = Vec::with_capacity(streams.len());
    for stream in streams {
        let lags = group_lag(config, &stream.consumer_group, &stream.topics).await?;
        let channels = match config.staging.kind {
            StagingKind::None => stream_channels(clients, &stream, &lags).await,
            _ => BTreeMap::new(),
        };
        let partitions: Vec<PartitionStatus> = lags
            .into_iter()
            .map(|lag| partition_status(&stream, lag, &channels))
            .collect();
        statuses.push(StreamStatus {
            stream: stream.name.clone(),
            table: stream.table.clone(),
            consumer_group: stream.consumer_group.clone(),
            partitions,
        });
    }
    Ok(statuses)
}
