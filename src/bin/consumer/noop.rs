extern crate rust_arroyo;

use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::backends::AssignmentCallbacks;
use rust_arroyo::processing::strategies::ProcessingStrategy;
use rust_arroyo::processing::strategies::{noop, ProcessingStrategyFactory};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{Partition, Topic};
use std::collections::HashMap;
use std::time::Duration;

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
    fn on_revoke(&mut self, _: Vec<Partition>) {}
}

struct StrategyFactory {}
impl ProcessingStrategyFactory<KafkaPayload> for StrategyFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        Box::new(noop::new(Duration::from_secs(1)))
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
