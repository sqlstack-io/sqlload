use crate::batching::models::Partition;
use crate::kafka::main::partitions_of::partitions_of;

/// A partition assignment change seen by a stream's consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebalanceEvent {
    Assigned(Vec<Partition>),
    Revoked(Vec<Partition>),
}

/// How far a consumer group trails one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub committed: Option<i64>,
    pub high_watermark: i64,
}

impl PartitionLag {
    /// Records not yet committed by the group.
    pub fn lag(&self) -> i64 {
        (self.high_watermark - self.committed.unwrap_or(0)).max(0)
    }
}

/// Forwards a consumer's partition assignment changes to its stream task.
#[derive(Debug)]
pub struct RebalanceForwarder {
    pub events: tokio::sync::mpsc::UnboundedSender<RebalanceEvent>,
}

impl rdkafka::ClientContext for RebalanceForwarder {}

impl rdkafka::consumer::ConsumerContext for RebalanceForwarder {
    fn post_rebalance(
        &self,
        _consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        let event = match rebalance {
            rdkafka::consumer::Rebalance::Assign(list) => {
                RebalanceEvent::Assigned(partitions_of(list))
            }
            rdkafka::consumer::Rebalance::Revoke(list) => {
                RebalanceEvent::Revoked(partitions_of(list))
            }
            rdkafka::consumer::Rebalance::Error(error) => {
                tracing::warn!(%error, "consumer rebalance failed");
                return;
            }
        };
        let _ = self.events.send(event);
    }
}
