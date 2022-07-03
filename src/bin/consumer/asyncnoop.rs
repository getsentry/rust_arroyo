use clap::{App, Arg};
use log::{debug, info};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
//use rdkafka::consumer::stream_consumer::StreamConsumer;
//use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureProducer;
use rdkafka::util::get_rdkafka_version;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::AssignmentCallbacks;
use rust_arroyo::processing::create_streaming;
use rust_arroyo::processing::strategies::async_noop::AsyncNoopCommit;
use rust_arroyo::processing::strategies::async_noop::CustomContext;
use rust_arroyo::types::{Partition, Topic};
use std::collections::HashMap;
use std::time::SystemTime;

// A type alias with your custom consumer can be created for convenience.
//type LoggingConsumer = StreamConsumer<CustomContext>;

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {
        println!("Assignment");
    }
    fn on_revoke(&mut self, _: Vec<Partition>) {
        println!("Revoked");
    }
}

async fn consume_and_produce(
    brokers: &str,
    group_id: &str,
    source_topic: &str,
    dest_topic: &str,
    batch_size: usize,
) {
    //let context = CustomContext {};

    //let consumer: LoggingConsumer = ClientConfig::new()
    //    .set("group.id", group_id)
    //    .set("bootstrap.servers", brokers)
    //    .set("enable.partition.eof", "false")
    //    .set("session.timeout.ms", "6000")
    //    .set("enable.auto.commit", "false")
    //    //.set("statistics.interval.ms", "30000")
    //    .set("auto.offset.reset", "earliest")
    //    .set_log_level(RDKafkaLogLevel::Warning)
    //    .create_with_context(context)
    //    .expect("Consumer creation failed");

    //consumer
    //    .subscribe(&[source_topic])
    //    .expect("Can't subscribe to specified topics");

    let topic = Topic {
        name: source_topic.to_string(),
    };
    let topic_clone = topic.clone();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("couldn't create producer");
    info!(
        "Beginning poll {:?}",
        vec![brokers, group_id, source_topic, dest_topic]
    );
    let batch = Vec::new();

    let strategy = AsyncNoopCommit {
        topic: topic_clone,
        producer,
        batch,
        last_batch_flush: SystemTime::now(),
        batch_size,
        dest_topic: dest_topic.to_string(),
        source_topic: source_topic.to_string(),
    };

    let config = KafkaConfig::new_consumer_config(
        vec![brokers.to_string()],
        group_id.to_string(),
        "earliest".to_string(),
        false,
        None,
    );

    let mut processor = create_streaming(config, strategy, topic);

    match processor.run().await {
        Ok(_) => {}
        Err(_) => panic!("Kafka error"),
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
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    env_logger::init();
    debug!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let source_topic = matches.value_of("source-topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();
    let dest_topic = matches.value_of("dest-topic").unwrap();
    let batch_size = matches
        .value_of("batch-size")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    consume_and_produce(brokers, group_id, source_topic, dest_topic, batch_size).await;
}
