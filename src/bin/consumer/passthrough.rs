use clap::{App, Arg};
use futures::future::join_all;
use futures::future::try_join_all;
use futures::{stream::FuturesUnordered, Future, StreamExt};
use log::warn;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::message::OwnedMessage;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::DeliveryFuture;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;
use std::boxed::Box;
use std::rc::Rc;
use std::time::Duration;
use tokio::spawn;
use tokio::sync::mpsc;

// use crate::example_utils::setup_logger;

// mod example_utils;
//
//

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
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

async fn consume_and_print(
    sender: mpsc::Sender<OwnedMessage>,
    brokers: &str,
    group_id: &str,
    source_topic: &str,
    dest_topic: &str,
) {
    let context = CustomContext {};
    //let mut batch = Vec::new();

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // TODO: disable auto commit
        .set("enable.auto.commit", "true")
        //.set("statistics.interval.ms", "30000")
        .set("auto.offset.reset", "smallest")
        // TODO: this should probably not be DEBUG
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&vec![source_topic])
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let payload = match m.payload_view::<str>() {
                    None => "",
                    Some(Ok(s)) => s,
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        ""
                    }
                };
                println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());

                // batch.push(m.detach());
                sender.send(m.detach()).await.unwrap();
                consumer.commit_message(&m, CommitMode::Async).unwrap();
            }
        };
    }
}

async fn produce_kafka_message<'a>(
    producer: &'a FutureProducer,
    record: Box<FutureRecord<'a, String, String>>,
) -> OwnedDeliveryResult {
    return producer.send(*record, Duration::from_secs(0)).await;
}

async fn produce_messages(
    mut rx: mpsc::Receiver<OwnedMessage>,
    brokers: String,
    dest_topic: String,
) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("couldn't create producer");
    let mut batch = Vec::new();
    loop {
        let m = rx.recv().await.unwrap();
        let payload = match m.payload() {
            Some(p) => p,
            None => panic!("No payload"),
        };
        let payload_str = match m.payload_view::<str>() {
            None => "",
            Some(Ok(s)) => s,
            Some(Err(e)) => {
                warn!("Error while deserializing message payload: {:?}", e);
                ""
            }
        };
        println!("PRODUCE: key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
                      m.key(), payload_str, m.topic(), m.partition(), m.offset(), m.timestamp());
        let stupid_string = String::from_utf8(payload.to_vec()).unwrap();
        let key = String::from("None");
        let tmp_producer = producer.clone();
        let tmp_dest_topic = dest_topic.clone();
        batch.push(Box::pin(async move {
            return tmp_producer
                .send(
                    FutureRecord::to(tmp_dest_topic.as_str())
                        .payload(&stupid_string)
                        .key(&key),
                    Duration::from_secs(0),
                )
                .await;
        }));
        if batch.len() > 10 {
            for r in try_join_all(batch.iter_mut()).await.iter() {
                println!("RESULT: {:?}", r);
            }
            batch.clear();
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
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    println!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let source_topic = matches.value_of("source_topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let dest_topic = matches.value_of("dest_topic").unwrap();
    let (sender, receiver) = mpsc::channel::<OwnedMessage>(1000);
    spawn(produce_messages(
        receiver,
        String::from(brokers),
        String::from(dest_topic),
    ));
    consume_and_print(sender, brokers, group_id, source_topic, dest_topic).await
}
