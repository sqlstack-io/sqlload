/// How one topic got its table name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NameSource {
    Map,
    Rule(String),
    Default,
    Fallback,
}

/// The table a topic exports to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicTable {
    pub topic: String,
    pub table: String,
    pub source: NameSource,
}

/// One table and every topic that exports to it, with the warehouse and Kafka object names it uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stream {
    pub table: String,
    pub name: String,
    pub topics: Vec<String>,
    pub consumer_group: String,
    pub stage: String,
    pub pipe: String,
    pub streaming_pipe: String,
}

impl Stream {
    /// The Snowpipe Streaming channel for one topic partition; Snowflake upper-cases channel names.
    pub fn channel(&self, topic: &str, partition: i32) -> String {
        format!(
            "{}_P{partition}",
            crate::naming::_helpers::streams::channel_prefix(topic)
        )
    }
}

/// A compiled naming rule; no template means matching topics are skipped.
#[derive(Debug)]
pub(crate) struct NameRule {
    pub(crate) pattern: crate::naming::_helpers::patterns::TopicPattern,
    pub(crate) template: Option<String>,
}

/// What the naming rules decide for one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Resolution {
    Table(TopicTable),
    Skipped,
    Unmatched,
}
