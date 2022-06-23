use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use crate::backends::{ConsumerError, ProducerError};

impl From<KafkaError> for ConsumerError {
    fn from(err: KafkaError) -> Self {
        match err {
            KafkaError::OffsetFetch(RDKafkaErrorCode::OffsetOutOfRange) => {
                ConsumerError::OffsetOutOfRange {
                    source: Box::new(err),
                }
            }
            other => ConsumerError::BrokerError(Box::new(other)),
        }
    }
}

impl From<KafkaError> for ProducerError {
    fn from(err: KafkaError) -> Self {
        match err {
            KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) => ProducerError::QueueFull,
            other => ProducerError::Other(Box::new(other)),
        }
    }
}
