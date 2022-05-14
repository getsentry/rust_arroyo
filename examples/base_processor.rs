extern crate rust_arroyo;

use rdkafka::message::OwnedMessage;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::backends::AssignmentCallbacks;
use rust_arroyo::backends::Consumer;
use rust_arroyo::processing::strategies::{
    CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory,
};
use rust_arroyo::processing::{RunError, StreamProcessor};
use rust_arroyo::types::{Message, Partition, Position, Topic};
use std::collections::{HashMap, HashSet};

struct TestStrategy {
    partitions: HashMap<Partition, Position>,
}
impl ProcessingStrategy<OwnedMessage> for TestStrategy {
    fn poll(&mut self) -> Option<CommitRequest> {
        println!("POLL");
        if self.partitions.len() > 0 {
            // TODO: Actually make commit work. It does not seem
            // to work now.
            let ret = Some(CommitRequest {
                positions: self.partitions.clone(),
            });
            self.partitions.clear();
            ret
        } else {
            None
        }
    }

    fn submit(&mut self, message: Message<OwnedMessage>) -> Result<(), MessageRejected> {
        println!("SUBMIT {}", message);
        self.partitions.insert(
            message.partition,
            Position {
                offset: message.offset,
                timestamp: message.timestamp,
            },
        );
        Ok(())
    }

    fn close(&mut self) {}

    fn terminate(&mut self) {}

    fn join(&mut self, timeout: Option<f64>) -> Option<CommitRequest> {
        None
    }
}

struct TestFactory {}
impl ProcessingStrategyFactory<OwnedMessage> for TestFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<OwnedMessage>> {
        Box::new(TestStrategy {
            partitions: HashMap::new(),
        })
    }
}

fn main() {
    let config = HashMap::from([
        ("group.id".to_string(), "my_group".to_string()),
        (
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        ),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);
    let mut consumer = Box::new(KafkaConsumer::new("my_group".to_string(), config));
    let topic = Topic {
        name: "test_static".to_string(),
    };

    let mut processor = StreamProcessor::new(consumer, Box::new(TestFactory {}));
    processor.subscribe(topic);
    for x in 0..20 {
        let _ = processor.run_once();
    }
}
