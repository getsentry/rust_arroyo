extern crate rust_arroyo;

use clap::{App, Arg};
use futures::executor::block_on;
use rand::Rng;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rust_arroyo::backends::kafka::config::KafkaConfig;
use rust_arroyo::backends::kafka::producer::KafkaProducer;
use rust_arroyo::backends::kafka::types::KafkaPayload;
use rust_arroyo::backends::Producer;
use rust_arroyo::types::{Topic, TopicOrPartition};
use serde::Serialize;
use std::collections::HashMap;
use std::str;
use std::time::SystemTime;

async fn recreate_topic(brokers: &str, topic: &str) {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers".to_string(), brokers);

    let admin_client: AdminClient<DefaultClientContext> = config.create().unwrap();

    admin_client
        .delete_topics(&[topic], &AdminOptions::new())
        .await
        .unwrap();

    let topics = [NewTopic::new(topic, 1, TopicReplication::Fixed(1))];
    admin_client
        .create_topics(&topics, &AdminOptions::new())
        .await
        .unwrap();
}

#[derive(Serialize)]
struct Metric {
    org_id: u64,
    project_id: u64,
    metric_id: u64,
    timestamp: u64,
    tags: HashMap<String, String>,
}

fn get_rand_string(len: usize) -> String {
    let mut rng = rand::thread_rng();
    let data: Vec<u8> = (0..len).map(|_| rng.gen_range(b'a'..b'z')).collect();
    str::from_utf8(&data).unwrap().to_string()
}

fn generate_metric() -> String {
    let org_id = rand::random();
    let project_id = rand::random();
    let metric_id = rand::random();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let mut tags = HashMap::new();

    for _ in 0..2 {
        tags.insert(get_rand_string(2), get_rand_string(4));
    }

    let metric = Metric {
        org_id,
        project_id,
        metric_id,
        timestamp: now,
        tags,
    };

    serde_json::to_string(&metric).unwrap()
}

fn main() {
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("brokers")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("topic")
                .long("topic")
                .help("Kafka topic")
                .takes_value(true)
                .default_value("test_source"),
        )
        .arg(
            Arg::with_name("number")
                .long("number")
                .help("number of events to generate")
                .default_value("1000")
                .takes_value(true),
        )
        .get_matches();

    let topic = matches.value_of("topic").unwrap();
    let brokers = matches.value_of("brokers").unwrap();
    let number = matches.value_of("number").unwrap().parse::<u32>().unwrap();

    // Deletes and recreates topic
    block_on(recreate_topic(brokers, topic));

    // Create producer
    let topic = Topic {
        name: topic.to_string(),
    };
    let destination = TopicOrPartition::Topic(topic);
    let config = KafkaConfig::new_producer_config(vec!["localhost:9092".to_string()], None);

    let mut producer = KafkaProducer::new(config);

    for i in 0..number {
        let payload = KafkaPayload {
            key: None,
            headers: None,
            payload: Some(generate_metric().as_bytes().to_vec()),
        };

        // HACK: just flush periodically to avoid QueueFull error
        if i % 100_000 == 0 {
            println!("Flushing {}", i);
            producer.flush();
        }
        producer.produce(&destination, &payload);
        producer.poll();
    }

    producer.flush();

    producer.close();
}
