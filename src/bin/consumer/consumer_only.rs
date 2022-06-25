extern crate rust_arroyo;

use clap::{App, Arg};
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::backends::AssignmentCallbacks;
use rust_arroyo::backends::Consumer;
use rust_arroyo::processing::strategies::ProcessingStrategy;
use rust_arroyo::processing::strategies::{noop, ProcessingStrategyFactory};
use rust_arroyo::types::{Partition, Position, Topic};
use std::collections::HashMap;
use std::time::Duration;

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
    fn on_revoke(&mut self, _: Vec<Partition>) {}
}

struct StrategyFactory {
    batch_time: u64,
}
impl ProcessingStrategyFactory<KafkaPayload> for StrategyFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        Box::new(noop::new(Duration::from_millis(self.batch_time)))
    }
}

fn main() {
    let matches = App::new("noop consumer")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple noop consumer")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("source-topic")
                .long("source-topic")
                .help("source topic name")
                .default_value("test_source")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("batch-time")
                .long("batch-time")
                .help("time of the batch for flushing")
                .default_value("100")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("offset-reset")
                .long("offset-reset")
                .help("kafka auto.offset.reset param")
                .default_value("earliest")
                .takes_value(true),
        )
        .get_matches();

    env_logger::init();
    let source_topic = matches.value_of("source-topic").unwrap();
    let offset_reset = matches.value_of("offset-reset").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    let config = KafkaConfig::new_consumer_config(
        vec![brokers.to_string()],
        group_id.to_string(),
        offset_reset.to_string(),
        false,
        None,
    );
    let mut consumer = KafkaConsumer::new(config);
    let topic = Topic {
        name: source_topic.to_string(),
    };

    struct EmptyCallbacks {}
    impl AssignmentCallbacks for EmptyCallbacks {
        fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
        fn on_revoke(&mut self, _: Vec<Partition>) {}
    }

    consumer
        .subscribe(&[topic.clone()], Box::new(EmptyCallbacks {}))
        .unwrap();

    let mut last_offset: u64 = 0;

    loop {
        let res = consumer.poll(None);
        match res {
            Err(e) => println!("{}", e),
            Ok(val) => match val {
                Some(message) => {
                    // Do nothing
                    last_offset = message.offset;

                    // Commit every 100k messages
                    if last_offset % 100_000 == 0 {
                        let mut positions = HashMap::new();
                        positions.insert(
                            Partition {
                                topic: topic.clone(),
                                index: 0, // One partition hardcoded
                            },
                            Position {
                                offset: last_offset + 1,
                                timestamp: chrono::Utc::now(),
                            },
                        );
                    }
                }
                None => {
                    if last_offset == 0 {
                        continue;
                    }
                    println!("Timed out waiting for message, committing offsets");
                    // If we got here we should have burned the backlog
                    let mut positions = HashMap::new();
                    positions.insert(
                        Partition {
                            topic: topic.clone(),
                            index: 0, // One partition hardcoded
                        },
                        Position {
                            offset: last_offset + 1,
                            timestamp: chrono::Utc::now(),
                        },
                    );
                    consumer.stage_positions(positions).unwrap();

                    consumer.commit_positions().unwrap();
                }
            },
        }
    }
}
