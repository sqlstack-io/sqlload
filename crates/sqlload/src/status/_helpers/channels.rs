use std::collections::BTreeMap;

use crate::exporter::models::Clients;
use crate::kafka::models::PartitionLag;
use crate::naming::models::Stream;
use crate::status::models::PartitionStatus;
use crate::streaming_api::main::channel_statuses::channel_statuses;
use crate::streaming_api::models::ChannelStatus;

/// Channel status for every partition of `stream`, empty when Snowflake cannot say.
pub(crate) async fn stream_channels(
    clients: &Clients,
    stream: &Stream,
    lags: &[PartitionLag],
) -> BTreeMap<String, ChannelStatus> {
    let names: Vec<String> = lags
        .iter()
        .map(|lag| stream.channel(&lag.topic, lag.partition))
        .collect();
    match channel_statuses(&clients.streaming, &stream.streaming_pipe, &names).await {
        Ok(statuses) => statuses,
        Err(error) => {
            tracing::warn!(stream = %stream.name, %error, "cannot read channel status");
            BTreeMap::new()
        }
    }
}

/// One partition's Kafka lag joined with its channel status.
pub(crate) fn partition_status(
    stream: &Stream,
    lag: PartitionLag,
    channels: &BTreeMap<String, ChannelStatus>,
) -> PartitionStatus {
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
}
