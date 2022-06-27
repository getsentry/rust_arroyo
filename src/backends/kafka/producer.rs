use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::Producer as ArroyoProducer;
use crate::backends::ProducerError;
use crate::types::TopicOrPartition;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::config::FromClientConfigAndContext;
use rdkafka::producer::{BaseRecord, DeliveryResult, Producer, ProducerContext, ThreadedProducer};
use rdkafka::util::IntoOpaque;
use std::sync::Mutex;
use std::time::Duration;

struct CustomContext<T> {
    callbacks: Mutex<Box<dyn DeliveryCallbacks<T>>>,
}

struct EmptyCallbacks {}

impl<T> DeliveryCallbacks<T> for EmptyCallbacks {
    fn on_delivery(&mut self, _: T) {}
}

impl<T> ClientContext for CustomContext<T> {}

impl<T: IntoOpaque> ProducerContext for CustomContext<T> {
    type DeliveryOpaque = T;

    fn delivery(
        &self,
        delivery_result: &DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        match delivery_result.as_ref() {
            Ok(_) => {
                self.callbacks.lock().unwrap().on_delivery(delivery_opaque);
            }
            Err(_) => println!("Failed to deliver message"),
        }
    }
}

// TODO: We should probably use async functions instead of these complicated callbacks
// Keeping it this way in line with the Kafka consumer implementation
pub trait DeliveryCallbacks<T>: Send + Sync {
    fn on_delivery(&mut self, msg_id: T);
}

pub struct KafkaProducer<T: 'static + IntoOpaque> {
    producer: Option<ThreadedProducer<CustomContext<T>>>,
}

impl<T: 'static + IntoOpaque> KafkaProducer<T> {
    pub fn new(
        config: KafkaConfig,
        delivery_callbacks: Option<Box<dyn DeliveryCallbacks<T>>>,
    ) -> Self {
        let config_obj: ClientConfig = config.into();

        let callbacks = delivery_callbacks.unwrap_or(Box::new(EmptyCallbacks {}));

        let producer = ThreadedProducer::from_config_and_context(
            &config_obj,
            CustomContext {
                callbacks: Mutex::new(callbacks),
            },
        )
        .unwrap();

        Self {
            producer: Some(producer),
        }
    }
}

impl<T: IntoOpaque> KafkaProducer<T> {
    pub fn poll(&self) {
        let producer = self.producer.as_ref().unwrap();
        producer.poll(Duration::ZERO);
    }

    pub fn flush(&self) {
        let producer = self.producer.as_ref().unwrap();
        producer.flush(Duration::from_millis(5000));
    }
}

impl<T: IntoOpaque> ArroyoProducer<KafkaPayload, T> for KafkaProducer<T> {
    fn produce(
        &self,
        destination: &TopicOrPartition,
        payload: &KafkaPayload,
        msg_id: T,
    ) -> Result<(), ProducerError> {
        let topic = match destination {
            TopicOrPartition::Topic(topic) => topic.name.as_ref(),
            TopicOrPartition::Partition(partition) => partition.topic.name.as_ref(),
        };

        // TODO: Fix the KafkaPayload type to avoid all this cloning
        let payload_copy = payload.clone();
        let msg_key = payload_copy.key.unwrap_or_default();
        let msg_payload = payload_copy.payload.unwrap_or_default();

        let mut base_record = BaseRecord::with_opaque_to(topic, msg_id)
            .payload(&msg_payload)
            .key(&msg_key);

        let partition = match destination {
            TopicOrPartition::Topic(_) => None,
            TopicOrPartition::Partition(partition) => Some(partition.index),
        };

        if let Some(index) = partition {
            base_record = base_record.partition(index as i32)
        }

        let producer = self.producer.as_ref().expect("Not closed");

        let res = producer.send(base_record);

        if let Err(err) = res {
            let t = err.0;
            return Err(t.into());
        }

        Ok(())
    }

    fn close(&mut self) {
        self.producer = None;
    }
}

#[cfg(test)]
mod tests {
    use super::{DeliveryCallbacks, KafkaProducer};
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

        struct MyCallbacks {}

        impl DeliveryCallbacks<usize> for MyCallbacks {
            fn on_delivery(&mut self, msg_id: usize) {
                println!("Message ID {}", msg_id);
            }
        }

        let mut producer: KafkaProducer<usize> =
            KafkaProducer::new(configuration, Box::new(MyCallbacks {}));

        let payload = KafkaPayload {
            key: None,
            headers: None,
            payload: Some("asdf".as_bytes().to_vec()),
        };
        producer.produce(&destination, &payload, 1).unwrap();
        producer.close();
    }
}
