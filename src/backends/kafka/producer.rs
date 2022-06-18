use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer;
use crate::types::TopicOrPartition;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, BaseRecord};

struct KafkaProducer {
    producer: Option<BaseProducer>,
}

impl KafkaProducer {
    #[allow(dead_code)] // TODO: Figure out why this is needed and remove
    pub fn new(config: KafkaConfig) -> Self {
        let config_obj: ClientConfig = config.into();
        let base_producer: BaseProducer<_> = config_obj.create().unwrap();

        Self {
            producer: Some(base_producer),
        }
    }
}

impl Producer<KafkaPayload> for KafkaProducer {
    fn produce(&self, destination: &TopicOrPartition, payload: &KafkaPayload) {
        let topic = match destination {
            TopicOrPartition::Topic(topic) => topic.name.as_ref(),
            TopicOrPartition::Partition(partition) => partition.topic.name.as_ref(),
        };

        // TODO: Fix the KafkaPayload type to avoid all this cloning
        let msg_payload = payload.clone().payload.unwrap_or_default();
        let msg_key = payload.clone().key.unwrap_or_default();

        let mut base_record = BaseRecord::to(topic).payload(&msg_payload).key(&msg_key);

        let partition = match destination {
            TopicOrPartition::Topic(_) => None,
            TopicOrPartition::Partition(partition) => Some(partition.index),
        };

        if let Some(index) = partition {
            base_record = base_record.partition(index as i32)
        }

        let producer = self.producer.as_ref().expect("Not closed");

        producer.send(base_record).expect("Something went wrong");
    }
    fn close(&mut self) {
        self.producer = None;
    }
}

#[cfg(test)]
mod tests {
    use super::KafkaProducer;
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::types::KafkaPayload;
    use crate::backends::Producer;
    use crate::types::{Topic, TopicOrPartition};
    #[test]
    fn test_producer() {
        let topic = Topic {
            name: "test".to_string(),
        };
        let destination = TopicOrPartition::Topic(topic);
        let configuration =
            KafkaConfig::new_producer_config(vec!["localhost:9092".to_string()], None);

        let mut producer = KafkaProducer::new(configuration);

        let payload = KafkaPayload {
            key: None,
            headers: None,
            payload: Some("asdf".as_bytes().to_vec()),
        };
        producer.produce(&destination, &payload);
        producer.close();
    }
}
