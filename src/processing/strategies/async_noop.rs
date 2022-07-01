use futures::future::{try_join_all, Future};
use log::info;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::Message;
use rdkafka::message::OwnedMessage;
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

pub struct AsyncNoopCommit {
    pub batch: FutureBatch<dyn Future<Output = OwnedDeliveryResult>>,
    pub last_batch_flush: SystemTime,
    pub batch_size: usize,
    pub dest_topic: String,
    pub source_topic: String,
}

pub async fn process_message(
    message: OwnedMessage,
    producer: &FutureProducer,
    batch: &mut FutureBatch<dyn Future<Output = OwnedDeliveryResult>>,
    last_batch_flush: SystemTime,
    batch_size: usize,
    dest_topic: String,
    source_topic: String,
) -> Option<TopicPartitionList> {
    let tmp_producer = producer.clone();
    let msg_clone = message;
    batch.push(Box::pin(async move {
        return tmp_producer
            .send(
                FutureRecord::to(&dest_topic)
                    .payload(msg_clone.payload().unwrap())
                    .key("None"),
                Duration::from_secs(0),
            )
            .await;
    }));
    if batch.len() > batch_size
        || SystemTime::now()
            .duration_since(last_batch_flush)
            .unwrap()
            .as_secs()
            // TODO: make batch flush time an arg
            > 1
    {
        return flush_batch(batch, source_topic).await;
    }
    None
}

pub async fn flush_batch(
    batch: &mut FutureBatch<dyn Future<Output = OwnedDeliveryResult>>,
    source_topic: String,
) -> Option<TopicPartitionList> {
    if batch.is_empty() {
        println!("batch is empty, nothing to flush");
        return None;
    }
    let results = try_join_all(batch.iter_mut()).await;
    match results {
        Err(e) => panic!("{:?}", e),
        Ok(result_vec) => {
            let mut positions: HashMap<(&str, i32), i64> = HashMap::new();
            for (partition, position) in result_vec.iter() {
                let offset_to_commit = match positions.get_mut(&(source_topic.as_str(), *partition))
                {
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
                .map(|(k, v)| ((String::from(k.0), k.1), Offset::from_raw(*v + 1)))
                .collect();
            let partition_list = TopicPartitionList::from_topic_map(&topic_map).unwrap();
            batch.clear();
            Some(partition_list)
        }
    }
}
