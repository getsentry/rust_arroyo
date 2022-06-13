#[non_exhaustive]
#[derive(Debug)]
pub enum KafkaErrorCode {
    Unknown = -1,
    OffsetOutOfRange = 1,
    InvalidMessage = 2,
    UnknownTopicOrPartition = 3,
    // TODO: add more
}
