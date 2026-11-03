/// One partition's progress in Kafka and, in streaming mode, in Snowflake.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStatus {
    pub topic: String,
    pub partition: i32,
    pub kafka_committed: Option<i64>,
    pub high_watermark: i64,
    pub lag: i64,
    pub channel_committed: Option<i64>,
    pub channel_error_rows: Option<i64>,
    pub channel_state: Option<String>,
}

/// One stream's progress.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamStatus {
    pub stream: String,
    pub table: String,
    pub consumer_group: String,
    pub partitions: Vec<PartitionStatus>,
}

impl StreamStatus {
    /// Records not yet committed across partitions.
    pub fn total_lag(&self) -> i64 {
        self.partitions.iter().map(|partition| partition.lag).sum()
    }
}
