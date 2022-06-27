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
use std::mem;
use std::time::{Duration, SystemTime};

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

const COMMIT_FREQUENCY: Duration = Duration::from_secs(5);

fn commit_offsets(
    consumer: &mut KafkaConsumer,
    topic: Topic,
    offsets_to_commit: HashMap<u16, u64>,
) {
    let mut positions = HashMap::new();

    for (partition, last_offset) in offsets_to_commit {
        positions.insert(
            Partition {
                topic: topic.clone(),
                index: partition,
            },
            Position {
                offset: last_offset + 1,
                timestamp: chrono::Utc::now(),
            },
        );
    }
    consumer.stage_positions(positions).unwrap();
    consumer.commit_positions().unwrap();
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

    let mut last_offsets: HashMap<u16, u64> = HashMap::new();
    let mut last_commit_time = SystemTime::UNIX_EPOCH;

    loop {
        let res = consumer.poll(None);
        match res {
            Err(e) => println!("{}", e),
            Ok(val) => match val {
                Some(message) => {
                    // Do nothing
                    last_offsets.insert(message.partition.index, message.offset);
                    let now = SystemTime::now();
                    if SystemTime::now() > last_commit_time.checked_add(COMMIT_FREQUENCY).unwrap() {
                        let offsets_to_commit = mem::take(&mut last_offsets);
                        commit_offsets(&mut consumer, topic.clone(), offsets_to_commit);
                        last_commit_time = now;
                    }
                }
                None => {
                    if last_offsets.is_empty() {
                        continue;
                    }
                    // If we got here we should have burned the backlog
                    let offsets_to_commit = mem::take(&mut last_offsets);
                    commit_offsets(&mut consumer, topic.clone(), offsets_to_commit);
                }
            },
        }
    }
}
