extern crate core;

use crate::MetricValue::Vector;
use clap::{App, Arg};
use futures::future::{try_join_all, Future};
use log::{debug, error, info};
use rand::Rng;
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
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::boxed::Box;
use std::cmp::max;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;
use std::time::SystemTime;
use tokio::time::timeout;

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;
type FutureBatch<T> = Vec<Pin<Box<T>>>;

async fn flush_batch(
    consumer: &LoggingConsumer,
    batch: &mut FutureBatch<impl Future<Output = OwnedDeliveryResult>>,
    source_topic: String,
) {
    if batch.is_empty() {
        println!("batch is empty, nothing to flush");
        return;
    }

    batch.clear();
}

#[derive(strum_macros::Display, Deserialize, Serialize, Debug)]
enum MetricType {
    c,
    d,
    s,
}

// '{"org_id": 1, "project_id": 1, "name": "sentry.sessions.session.duration", "unit": "s", "type": "d", "value": [948.7285023840417, 229.7264210041775, 855.1960305024135, 475.592711958219, 825.5422355278084, 916.3170826715101], "timestamp": 1655940182, "tags": {"environment": "env-1", "release": "v1.1.1", "session.status": "exited"}}
//
//

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum MetricValue {
    Vector(Vec<f64>),
    Float(f64),
}

#[derive(Debug, Serialize, Deserialize)]
struct MetricsInPayload {
    org_id: i64,
    project_id: i64,
    name: String,
    unit: String,
    r#type: MetricType,
    value: MetricValue,
    timestamp: i64,
    tags: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MetricsOutPayload {
    use_case_id: String,
    org_id: u64,
    project_id: u64,
    metric_id: u64,
    timestamp: u64,
    #[serde(rename = "tags.key")]
    tags_key: Vec<u64>,
    #[serde(rename = "tags.value")]
    tags_value: Vec<u64>,
    metric_type: String,
    set_values: Vec<u64>,
    count_value: f64,
    distribution_values: Vec<f64>,
    materialization_version: u8,
    retention_days: u16,
    partition: u16,
    offset: u64,
}

pub fn newMetricsOut(payload: &MetricsInPayload) -> MetricsOutPayload {
    let mut out = MetricsOutPayload {
        use_case_id: "1".parse().unwrap(),
        org_id: payload.org_id as u64,
        project_id: payload.project_id as u64,
        metric_id: rand::thread_rng().gen(),
        timestamp: payload.timestamp as u64,
        tags_key: vec![],
        tags_value: vec![],
        metric_type: payload.r#type.to_string(),
        set_values: vec![],
        count_value: 0.0,
        distribution_values: vec![],
        materialization_version: 1,
        retention_days: 90,
        partition: 1,
        offset: 1,
    };

    out.tags_key = payload
        .tags
        .keys()
        .map(|tag| rand::thread_rng().gen())
        .collect();
    out.tags_value = payload
        .tags
        .values()
        .map(|tag| rand::thread_rng().gen())
        .collect();

    let value = &payload.value;

    match &payload.r#type {
        MetricType::c => {
            out.count_value = match value {
                MetricValue::Float(v) => *v,
                _ => 1.0,
            }
        }
        MetricType::d => {
            out.distribution_values = match value {
                Vector(v) => v,
                _ => &vec![0.0_f64],
            }
            .to_owned()
        }
        MetricType::s => {
            out.set_values = match value {
                Vector(v) => *v.iter().map(|v| *v as u64).collect(),
                _ => {
                    vec![1_u64]
                }
            }
            .to_owned()
        }
    }

    return out;
}

fn deserialize_incoming(payload: &str) -> MetricsInPayload {
    let out = match serde_json::from_str::<MetricsInPayload>(&payload) {
        Ok(p) => p,
        Err(e) => {
            error!("Could not parse payload! {:?}, {:?}", payload, e);
            MetricsInPayload {
                org_id: 1,
                project_id: 1,
                name: String::from("fail"),
                unit: String::from("fail"),
                r#type: MetricType::c,
                value: MetricValue::Float(4.2069),
                timestamp: 1234,
                tags: HashMap::new(),
            }
        }
    };
    return out;
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
        .set("auto.offset.reset", "earliest")
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
    info!(
        "Beginning poll {:?}",
        vec![brokers, group_id, source_topic, dest_topic]
    );
    let mut last_batch_flush = SystemTime::now();
    loop {
        match timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(result) => {
                match result {
                    Err(e) => panic!("Kafka error: {}", e),
                    Ok(m) => {
                        let m_clone = m.detach();
                        let payload_str = match m_clone.payload_view::<str>() {
                            None => "",
                            Some(Ok(s)) => s,
                            Some(Err(e)) => {
                                error!("Error while deserializing message payload: {:?}", e);
                                ""
                            }
                        };
                        // this is only a pointer clone, it doesn't clone tha underlying producer
                        let tmp_producer = producer.clone();
                        let deserialized_input = deserialize_incoming(&payload_str);
                        let metrics_out = newMetricsOut(&deserialized_input);
                        batch.push(metrics_out);

                        if batch.len() > batch_size
                            || SystemTime::now()
                            .duration_since(last_batch_flush)
                            .unwrap()
                            .as_secs()
                            // TODO: make batch flush time an arg
                            > 1
                        {
                            flush_batch(&consumer, &mut batch, String::from(source_topic)).await;
                            last_batch_flush = SystemTime::now();
                        }
                    }
                }
            }
            Err(_) => {
                error!("timeoout, flushing batch");
                flush_batch(&consumer, &mut batch, String::from(source_topic)).await;
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
                .long("batch-size")
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

#[cfg(test)]
mod tests {
    use crate::MetricsOutPayload;

    fn test_clickhouse_write() {
        let row = MetricsOutPayload {
            use_case_id: "a".parse().unwrap(),
            org_id: 1,
            project_id: 1,
            metric_id: 1,
            timestamp: 1656401964,
            tags_key: vec![1],
            tags_value: vec![1],
            metric_type: "c".parse().unwrap(),
            set_values: vec![],
            count_value: 1_f64,
            distribution_values: vec![],
            materialization_version: 1,
            retention_days: 90,
            partition: 1,
            offset: 1,
        };

        let client = reqwest::Client::new();
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("Connection", "keep-alive".parse().unwrap());
        headers.insert("Accept-Encoding", "gzip,deflate".parse().unwrap());

        let res = client
            .post("http://localhost:8123/post")
            .body("the exact body that is sent")
            .headers(headers)
            .send()
            .await?;
    }
}
