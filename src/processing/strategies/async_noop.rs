use crate::backends::kafka::types::KafkaPayload;
use crate::processing::strategies::CommitRequest;
use crate::types::{Message, Partition, Position, Topic};
use async_trait::async_trait;
use chrono::Utc;
use futures::future::{try_join_all, Future};
use log::info;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::producer::future_producer::OwnedDeliveryResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::cmp::max;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

type FutureBatch<T> = Vec<Pin<Box<T>>>;
pub struct CustomContext;
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

#[async_trait]
pub trait ProcessingStrategy {
    async fn poll(&mut self) -> Option<TopicPartitionList>;

    async fn submit(&mut self, message: Message<KafkaPayload>);
}

fn build_commit_request(partitions: TopicPartitionList, topic: Topic) -> CommitRequest {
    let elements = partitions.elements_for_topic(&topic.name);
    let mut ret: HashMap<Partition, Position> = HashMap::new();
    for element in elements {
        ret.insert(
            Partition {
                topic: topic.clone(),
                index: element.partition() as u16,
            },
            Position {
                offset: element.offset().to_raw().unwrap() as u64,
                timestamp: Utc::now(),
            },
        );
    }
    CommitRequest { positions: ret }
}

pub fn build_topic_partitions(commit_request: CommitRequest) -> TopicPartitionList {
    let mut topic_map = HashMap::new();
    for (partition, position) in commit_request.positions.iter() {
        topic_map.insert(
            (partition.topic.name.clone(), partition.index as i32),
            Offset::from_raw(position.offset as i64),
        );
    }

    TopicPartitionList::from_topic_map(&topic_map).unwrap()
}

pub struct AsyncNoopCommit {
    pub topic: Topic,
    pub producer: FutureProducer,
    pub last_batch_flush: SystemTime,
    pub batch: FutureBatch<dyn Future<Output = OwnedDeliveryResult>>,
    pub batch_size: usize,
    pub dest_topic: String,
    pub source_topic: String,
}

impl AsyncNoopCommit {
    pub async fn poll(&mut self) -> Option<CommitRequest> {
        if self.batch.len() > self.batch_size
            || SystemTime::now()
                .duration_since(self.last_batch_flush)
                .unwrap()
                .as_secs()
                // TODO: make batch flush time an arg
                > 1
        {
            match self.flush_batch().await {
                Some(partition_list) => {
                    self.last_batch_flush = SystemTime::now();
                    return Some(partition_list);
                }
                None => {
                    return None;
                }
            }
        }
        None
    }

    pub async fn submit(&mut self, message: Message<KafkaPayload>) {
        let tmp_producer = self.producer.clone();
        let msg_clone = message.payload;
        let topic_clone = self.dest_topic.clone();
        self.batch.push(Box::pin(async move {
            return tmp_producer
                .send(
                    FutureRecord::to(&topic_clone)
                        .payload(&msg_clone.payload.unwrap())
                        .key("None"),
                    Duration::from_secs(0),
                )
                .await;
        }));
    }

    async fn flush_batch(&mut self) -> Option<CommitRequest> {
        if self.batch.is_empty() {
            //println!("batch is empty, nothing to flush");
            return None;
        }
        let results = try_join_all(self.batch.iter_mut()).await;
        match results {
            Err(e) => panic!("{:?}", e),
            Ok(result_vec) => {
                let mut positions: HashMap<(&str, i32), i64> = HashMap::new();
                for (partition, position) in result_vec.iter() {
                    let offset_to_commit =
                        match positions.get_mut(&(&self.source_topic, *partition)) {
                            None => *position,
                            Some(v) => max(*v, *position),
                        };
                    match positions.insert((&self.source_topic, *partition), offset_to_commit) {
                        Some(_) => {}
                        None => {}
                    };
                }

                let topic_map = positions
                    .iter()
                    .map(|(k, v)| ((String::from(k.0), k.1), Offset::from_raw(*v + 1)))
                    .collect();
                let partition_list = TopicPartitionList::from_topic_map(&topic_map).unwrap();
                self.batch.clear();
                Some(build_commit_request(partition_list, self.topic.clone()))
            }
        }
    }
}
