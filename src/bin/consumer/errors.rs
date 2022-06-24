extern crate rust_arroyo;

use crate::rust_arroyo::backends::Producer;
use clap::{App, Arg};
// use log::debug;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::DeliveryCallbacks;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::backends::AssignmentCallbacks;
use rust_arroyo::backends::ProducerError;
use rust_arroyo::processing::strategies::ProcessingStrategyFactory;
use rust_arroyo::processing::strategies::{CommitRequest, ProcessingError, ProcessingStrategy};
use rust_arroyo::processing::StreamProcessor;
use rust_arroyo::types::{Message, Partition, Position, Topic, TopicOrPartition};
use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const COMMIT_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug)]
struct CommitData {
    partition: Partition,
    position: Position,
}

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
    fn on_revoke(&mut self, _: Vec<Partition>) {}
}

// TODO: We probably want to replace this all with async
struct ProducerCallbacks {
    offsets: Arc<Mutex<HashMap<Partition, Position>>>,
}

impl DeliveryCallbacks<Box<CommitData>> for ProducerCallbacks {
    fn on_delivery(&mut self, msg_data: Box<CommitData>) {
        let mut offsets = self.offsets.lock().unwrap();
        offsets.insert(msg_data.partition, msg_data.position);
    }
}

struct Next {
    destination: TopicOrPartition,
    producer: KafkaProducer<Box<CommitData>>,
    last_commit: SystemTime,
    offsets: Arc<Mutex<HashMap<Partition, Position>>>,
}

impl Next {
    pub fn new(destination: TopicOrPartition, broker: String) -> Self {
        let config = KafkaConfig::new_producer_config(vec![broker], None);

        let offsets = Arc::new(Mutex::new(HashMap::new()));
        let producer = KafkaProducer::new(
            config,
            Box::new(ProducerCallbacks {
                offsets: offsets.clone(),
            }),
        );

        Self {
            destination,
            producer,
            last_commit: SystemTime::now(),
            offsets,
        }
    }
}

impl ProcessingStrategy<KafkaPayload> for Next {
    fn poll(&mut self) -> Option<CommitRequest> {
        let now = SystemTime::now();
        let diff = now.duration_since(self.last_commit).unwrap();
        if diff > COMMIT_INTERVAL && self.offsets.lock().unwrap().keys().len() > 0 {
            let prev = mem::take(&mut self.offsets);

            let mut positions_to_commit = HashMap::new();
            for (k, v) in prev.lock().unwrap().iter() {
                positions_to_commit.insert(k.clone(), v.clone());
            }

            return Some(CommitRequest {
                positions: positions_to_commit,
            });
        }

        None
    }

    fn submit(&mut self, message: Message<KafkaPayload>) -> Result<(), ProcessingError> {
        let offset_to_commit = message.next_offset();
        let res = self.producer.produce(
            &self.destination,
            &message.payload,
            Box::new(CommitData {
                partition: message.partition,
                position: Position {
                    offset: offset_to_commit,
                    timestamp: message.timestamp,
                },
            }),
        );

        // TODO: MessageRejected should be handled by the StreamProcessor but
        // is not currently.
        if let Err(ProducerError::QueueFull) = res {
            return Err(ProcessingError::MessageRejected);
        }

        Ok(())
    }

    fn close(&mut self) {}

    fn terminate(&mut self) {}

    fn join(&mut self, _timeout: Option<Duration>) -> Option<CommitRequest> {
        None
    }
}

struct StrategyFactory {
    broker: String,
    destination_topic: String,
}
impl ProcessingStrategyFactory<KafkaPayload> for StrategyFactory {
    fn create(&self) -> Box<dyn ProcessingStrategy<KafkaPayload>> {
        Box::new(Next::new(
            TopicOrPartition::Topic({
                Topic {
                    name: self.destination_topic.clone(),
                }
            }),
            self.broker.clone(),
        ))
    }
}

fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
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
                .long("source")
                .help("source topic name")
                .default_value("test_source")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("dest-topic")
                .long("dest")
                .help("destination topic name")
                .default_value("test_dest")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("batch-size")
                .long("batch_size")
                .help("size of the batch for flushing")
                .default_value("10")
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

    let source_topic = matches.value_of("source-topic").unwrap();
    let offset_reset = matches.value_of("offset-reset").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let dest_topic = matches.value_of("dest-topic").unwrap();
    // env_logger::init();
    let config = KafkaConfig::new_consumer_config(
        vec![brokers.to_string()],
        group_id.to_string(),
        offset_reset.to_string(),
        false,
        None,
    );
    let consumer = KafkaConsumer::new(config);
    let topic = Topic {
        name: source_topic.to_string(),
    };
    let mut stream_processor = StreamProcessor::new(
        Box::new(consumer),
        Box::new(StrategyFactory {
            destination_topic: dest_topic.to_string(),
            broker: brokers.to_string(),
        }),
    );

    stream_processor.subscribe(topic);
    stream_processor.run().unwrap();
}
