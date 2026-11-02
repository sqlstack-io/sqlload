//! Convert consumed messages.

use rdkafka::Message;
use rdkafka::message::{BorrowedMessage, Headers, Timestamp};

use crate::envelope::models::{KafkaRecord, RecordHeader};

/// The message as a [`KafkaRecord`]; timestamp type 0 is create time, 1 log-append time.
pub fn to_record(message: &BorrowedMessage<'_>) -> KafkaRecord {
    let (timestamp_ms, timestamp_type) = match message.timestamp() {
        Timestamp::CreateTime(ms) => (Some(ms), Some(0)),
        Timestamp::LogAppendTime(ms) => (Some(ms), Some(1)),
        Timestamp::NotAvailable => (None, None),
    };
    let headers = message
        .headers()
        .map(|headers| {
            headers
                .iter()
                .map(|header| RecordHeader {
                    name: header.key.to_owned(),
                    value: header.value.map(<[u8]>::to_vec),
                })
                .collect()
        })
        .unwrap_or_default();
    KafkaRecord {
        topic: message.topic().to_owned(),
        partition: message.partition(),
        offset: message.offset(),
        key: message.key().map(<[u8]>::to_vec),
        timestamp_ms,
        timestamp_type,
        value: message.payload().map(<[u8]>::to_vec),
        headers,
    }
}
