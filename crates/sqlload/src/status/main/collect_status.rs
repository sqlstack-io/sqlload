//! The `status` command.

use std::collections::BTreeMap;

use crate::config::models::{Config, StagingKind};
use crate::exporter::models::Clients;
use crate::kafka::main::group_lag::group_lag;
use crate::kafka::main::list_topics::list_topics;
use crate::naming::main::resolve_streams::resolve_streams;
use crate::naming::models::Stream;
use crate::status::models::{PartitionStatus, StreamStatus};
use crate::streaming_api::models::ChannelStatus;

/// Kafka lag for every stream and, in streaming mode, each channel's committed offset and rejected rows.
pub async fn collect_status(
    config: &Config,
    clients: &Clients,
) -> Result<Vec<StreamStatus>, String> {
    let topics = list_topics(config).await?;
    let streams = resolve_streams(config, &topics).map_err(|problems| problems.join("; "))?;
    let mut statuses = Vec::with_capacity(streams.len());
    for stream in streams {
        let lags = group_lag(config, &stream.consumer_group, &stream.topics).await?;
        let channels = match config.staging.kind {
            StagingKind::None => channel_statuses(clients, &stream, &lags).await,
            _ => BTreeMap::new(),
        };
        let partitions = lags
            .into_iter()
            .map(|lag| {
                let channel = channels.get(&stream.channel(&lag.topic, lag.partition));
                PartitionStatus {
                    lag: lag.lag(),
                    kafka_committed: lag.committed,
                    high_watermark: lag.high_watermark,
                    channel_committed: channel.and_then(ChannelStatus::committed_offset),
                    channel_error_rows: channel.and_then(|status| status.rows_error_count),
                    channel_state: channel.and_then(|status| status.channel_status_code.clone()),
                    topic: lag.topic,
                    partition: lag.partition,
                }
            })
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

async fn channel_statuses(
    clients: &Clients,
    stream: &Stream,
    lags: &[crate::kafka::models::PartitionLag],
) -> BTreeMap<String, ChannelStatus> {
    let names: Vec<String> = lags
        .iter()
        .map(|lag| stream.channel(&lag.topic, lag.partition))
        .collect();
    match clients
        .streaming
        .channel_statuses(&stream.streaming_pipe, &names)
        .await
    {
        Ok(statuses) => statuses,
        Err(error) => {
            tracing::warn!(stream = %stream.name, %error, "cannot read channel status");
            BTreeMap::new()
        }
    }
}
