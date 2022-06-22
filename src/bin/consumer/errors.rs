extern crate rust_arroyo;

use crate::rust_arroyo::backends::Producer;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::backends::AssignmentCallbacks;
use rust_arroyo::processing::strategies::ProcessingStrategyFactory;
use rust_arroyo::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::Message;
use rust_arroyo::types::{Partition, Topic, TopicOrPartition};
use std::collections::HashMap;

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
    fn on_revoke(&mut self, _: Vec<Partition>) {}
}

struct Next {
    destination: TopicOrPartition,
    producer: KafkaProducer,
}
impl ProcessingStrategy<KafkaPayload> for Next {
    fn poll(&mut self) -> Option<CommitRequest> {
        None
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), MessageRejected> {
        self.producer.produce(&self.destination, &message.payload);
        Ok(())
    }

    fn close(&mut self) {}

    fn terminate(&mut self) {}

    fn join(&mut self, _timeout: Option<f64>) -> Option<CommitRequest> {
        None
    }
}

struct StrategyFactory {}
impl ProcessingStrategyFactory<KafkaPayload> for StrategyFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        let config = KafkaConfig::new_producer_config(vec!["localhost:9092".to_string()], None);
        let producer = KafkaProducer::new(config);
        Box::new(Next {
            destination: TopicOrPartition::Topic({
                Topic {
                    name: "test-dest".to_string(),
                }
            }),
            producer,
        })
    }
}

fn main() {
    let config = KafkaConfig::new_consumer_config(
        vec!["localhost:9092".to_string()],
        "my_group".to_string(),
        "earliest".to_string(),
        false,
        None,
    );
    let consumer = KafkaConsumer::new(config);
    let topic = Topic {
        name: "test-static".to_string(),
    };
    let mut stream_processor =
        StreamProcessor::new(Box::new(consumer), Box::new(StrategyFactory {}));

    stream_processor.subscribe(topic);

    stream_processor.run().unwrap();
}
