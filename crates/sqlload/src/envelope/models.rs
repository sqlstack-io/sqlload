/// One Kafka record as sqlload sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KafkaRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub timestamp_ms: Option<i64>,
    pub timestamp_type: Option<i64>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<RecordHeader>,
}

/// One Kafka record header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub name: String,
    pub value: Option<Vec<u8>>,
}
