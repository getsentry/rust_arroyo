use clap::{App, Arg};
use futures::future::{try_join_all, Future};
use log::warn;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::get_rdkafka_version;
use std::boxed::Box;
use std::cmp::max;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;
type FutureBatch<T> = Vec<Pin<Box<T>>>;

async fn flush_batch(
    consumer: &LoggingConsumer,
    batch: &mut FutureBatch<impl Future<Output = OwnedDeliveryResult>>,
    batch_size: usize,
    source_topic: String,
) {
    if batch.len() > batch_size {
        let results = try_join_all(batch.iter_mut()).await;
        match results {
            Err(e) => panic!("{:?}", e),
            Ok(result_vec) => {
                let mut positions: HashMap<(&str, i32), i64> = HashMap::new();
                for (partition, position) in result_vec.iter() {
                    let offset_to_commit =
                        match positions.get_mut(&(source_topic.as_str(), *partition)) {
                            None => *position,
                            Some(v) => max(*v, *position),
                        };
                    match positions.insert((source_topic.as_str(), *partition), offset_to_commit) {
                        Some(_) => {}
                        None => {}
                    };
                }

                let topic_map = positions
                    .iter()
                    .map(|(k, v)| ((String::from(k.0), k.1), Offset::from_raw(*v)))
                    .collect();
                let partition_list = TopicPartitionList::from_topic_map(&topic_map).unwrap();
                consumer.commit(&partition_list, CommitMode::Sync).unwrap();
            }
        }
        batch.clear();
    }
}

async fn consume_and_produce(
    brokers: &str,
    group_id: &str,
    source_topic: &str,
    dest_topic: &str,
    batch_size: usize,
) {
    let context = CustomContext {};
    let mut batch = Vec::new();

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Warning)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[source_topic])
        .expect("Can't subscribe to specified topics");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("couldn't create producer");
    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload_str = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                          m.key(), payload_str, m.topic(), m.partition(), m.offset(), m.timestamp());
                let payload_clone = m.detach();
                // this is only a pointer clone, it doesn't clone tha underlying producer
                let tmp_producer = producer.clone();

                batch.push(Box::pin(async move {
                    return tmp_producer
                        .send(
                            FutureRecord::to(dest_topic)
                                .payload(payload_clone.payload().unwrap())
                                .key("None"),
                            Duration::from_secs(0),
                        )
                        .await;
                }));
                flush_batch(
                    &consumer,
                    &mut batch,
                    batch_size,
                    String::from(source_topic),
                )
                .await;
            }
        }
    }
}

#[tokio::main]
async fn main() {
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
            Arg::with_name("source_topic")
                .long("source")
                .help("source topic name")
                .default_value("test_source")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("dest_topic")
                .long("dest")
                .help("destination topic name")
                .default_value("test_dest")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("batch_size")
                .long("batch_size")
                .help("size of the batch for flushing")
                .default_value("10")
                .takes_value(true),
        )
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let source_topic = matches.value_of("source_topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let dest_topic = matches.value_of("dest_topic").unwrap();
    let batch_size = matches
        .value_of("batch_size")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    consume_and_produce(brokers, group_id, source_topic, dest_topic, batch_size).await;
}
