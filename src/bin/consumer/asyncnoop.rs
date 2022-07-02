use clap::{App, Arg};
use log::{debug, error, info};
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use rdkafka::util::get_rdkafka_version;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::KafkaConsumer;
use rust_arroyo::backends::{AssignmentCallbacks, Consumer};
use rust_arroyo::processing::strategies::async_noop::AsyncNoopCommit;
use rust_arroyo::types::{Partition, Topic};
use std::collections::HashMap;
use std::time::Duration;
use std::time::SystemTime;

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
    let config = KafkaConfig::new_consumer_config(
        vec![brokers.to_string()],
        group_id.to_string(),
        "earliest".to_string(),
        false,
        None,
    );
    let mut consumer = KafkaConsumer::new(config);
    let topic = Topic {
        name: source_topic.to_string(),
    };
    let topic_clone = topic.clone();
    let _ = consumer.subscribe(&[topic], Box::new(EmptyCallbacks {}));

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

    let mut strategy = AsyncNoopCommit {
        topic: topic_clone,
        producer,
        batch,
        last_batch_flush: SystemTime::now(),
        batch_size,
        dest_topic: dest_topic.to_string(),
        source_topic: source_topic.to_string(),
    };
    loop {
        match consumer.poll(Some(Duration::ZERO)).await {
            Ok(result) => match result {
                None => {
                    match strategy.poll().await {
                        Some(request) => {
                            consumer.stage_positions(request.positions).await.unwrap();
                            consumer.commit_positions().await.unwrap();
                            //info!("Committed: {:?}", request);
                        }
                        None => {}
                    }
                }
                Some(m) => {
                    match strategy.poll().await {
                        Some(request) => {
                            consumer.stage_positions(request.positions).await.unwrap();
                            consumer.commit_positions().await.unwrap();
                            //info!("Committed: {:?}", request);
                        }
                        None => {}
                    }
                    strategy.submit(m).await;
                }
            },
            Err(_) => {
                error!("timeoout, flushing batch");
                match strategy.poll().await {
                    Some(request) => {
                        consumer.stage_positions(request.positions).await.unwrap();
                        consumer.commit_positions().await.unwrap();
                        //info!("Committed: {:?}", request);
                    }
                    None => {}
                }
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
