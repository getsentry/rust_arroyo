use crate::types::errors::KafkaErrorCode;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use crate::backends::ConsumerError;

fn map_error_code(code: RDKafkaErrorCode) -> KafkaErrorCode {
    match code {
        RDKafkaErrorCode::OffsetOutOfRange => KafkaErrorCode::OffsetOutOfRange,
        _ => KafkaErrorCode::Unknown,
    }
}

impl From<KafkaError> for ConsumerError {
    fn from(err: KafkaError) -> Self {
        match err {
            KafkaError::OffsetFetch(code) => ConsumerError::OffsetFetch(map_error_code(code)),
            other => ConsumerError::BrokerError(Box::new(other)),
        }
    }
}
