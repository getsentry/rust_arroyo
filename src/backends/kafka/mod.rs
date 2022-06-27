use super::kafka::config::KafkaConfig;
use super::AssignmentCallbacks;
use super::Consumer as ArroyoConsumer;
use super::ConsumerError;
use crate::backends::kafka::types::KafkaPayload;
use crate::types::Message as ArroyoMessage;
use crate::types::{Partition, Position, Topic};
use chrono::{DateTime, NaiveDateTime, Utc};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::collections::HashSet;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub mod config;
mod errors;
pub mod producer;
pub mod types;

#[derive(Eq, Hash, PartialEq)]
enum KafkaConsumerState {
    NotSubscribed,
    Consuming,
    #[allow(dead_code)]
    Error,
    #[allow(dead_code)]
    Closed,
    #[allow(dead_code)]
    Assigning,
    #[allow(dead_code)]
    Revoking,
}

impl KafkaConsumerState {
    fn assert_consuming_state(&self) -> Result<(), ConsumerError> {
        match self {
            KafkaConsumerState::Closed => Err(ConsumerError::ConsumerClosed),
            KafkaConsumerState::NotSubscribed => Err(ConsumerError::NotSubscribed),
            KafkaConsumerState::Error => Err(ConsumerError::ConsumerErrored),
            _ => Ok(()),
        }
    }
}

fn create_kafka_message(msg: BorrowedMessage) -> ArroyoMessage<KafkaPayload> {
    let topic = Topic {
        name: msg.topic().to_string(),
    };
    let partition = Partition {
        topic,
        index: msg.partition() as u16,
    };
    let time_millis = msg.timestamp().to_millis().unwrap_or(0);

    ArroyoMessage::new(
        partition,
        msg.offset() as u64,
        KafkaPayload {
            key: Some(vec![]),
            headers: msg.headers().map(BorrowedHeaders::detach),
            payload: Some(vec![]),
        },
        DateTime::from_utc(NaiveDateTime::from_timestamp(time_millis, 0), Utc),
    )
}

struct CustomContext {
    // This is horrible. I want to mutate callbacks (to invoke on_assign)
    // From the pre_rebalance function.
    // But pre_rebalance gets &self and not &mut self.
    // I am sure there has to be a better way to do this.
    callbacks: Mutex<Box<dyn AssignmentCallbacks>>,
    consumer_offsets: Arc<Mutex<HashMap<Partition, u64>>>,
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        if let Rebalance::Revoke(list) = rebalance {
            let mut partitions: Vec<Partition> = Vec::new();
            for partition in list.elements().iter() {
                let topic = partition.topic();
                let partition_number = partition.partition();
                partitions.push(Partition {
                    topic: Topic {
                        name: topic.to_string(),
                    },
                    index: partition_number as u16,
                });
            }

            let mut offsets = self.consumer_offsets.lock().unwrap();
            for partition in partitions.iter() {
                offsets.remove(partition);
            }

            self.callbacks.lock().unwrap().on_revoke(partitions);
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        if let Rebalance::Assign(list) = rebalance {
            let mut map: HashMap<Partition, u64> = HashMap::new();
            for partition in list.elements().iter() {
                let topic = partition.topic();
                let partition_number = partition.partition();
                let offset = partition.offset().to_raw().unwrap();
                map.insert(
                    Partition {
                        topic: Topic {
                            name: topic.to_string(),
                        },
                        index: partition_number as u16,
                    },
                    offset as u64,
                );
            }
            let mut offsets = self.consumer_offsets.lock().unwrap();
            for (partition, offset) in map.clone() {
                offsets.insert(partition, offset);
            }
            self.callbacks.lock().unwrap().on_assign(map);
        }
    }

    fn commit_callback(&self, _: KafkaResult<()>, _offsets: &TopicPartitionList) {}
}

pub struct KafkaConsumer {
    // TODO: This has to be an option as of now because rdkafka requires
    // callbacks during the instantiation. While the streaming processor
    // can only pass the callbacks during the subscribe call.
    // So we need to build the kafka consumer upon subscribe and not
    // in the constructor.
    consumer: Option<BaseConsumer<CustomContext>>,
    config: KafkaConfig,
    state: KafkaConsumerState,
    offsets: Arc<Mutex<HashMap<Partition, u64>>>,
    staged_offsets: HashMap<Partition, Position>,
}

impl KafkaConsumer {
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            consumer: None,
            config,
            state: KafkaConsumerState::NotSubscribed,
            offsets: Arc::new(Mutex::new(HashMap::new())),
            staged_offsets: HashMap::new(),
        }
    }
}

impl<'a> ArroyoConsumer<'a, KafkaPayload> for KafkaConsumer {
    fn subscribe(
        &mut self,
        topics: &[Topic],
        callbacks: Box<dyn AssignmentCallbacks>,
    ) -> Result<(), ConsumerError> {
        let context = CustomContext {
            callbacks: Mutex::new(callbacks),
            consumer_offsets: self.offsets.clone(),
        };

        let mut config_obj: ClientConfig = self.config.clone().into();

        let consumer: BaseConsumer<CustomContext> = config_obj
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)?;
        let topic_str: Vec<&str> = topics.iter().map(|t| t.name.as_ref()).collect();
        consumer.subscribe(&topic_str)?;
        self.consumer = Some(consumer);
        self.state = KafkaConsumerState::Consuming;
        Ok(())
    }

    fn unsubscribe(&mut self) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;
        let consumer = self.consumer.as_mut().unwrap();
        consumer.unsubscribe();

        Ok(())
    }

    fn poll(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<Option<ArroyoMessage<KafkaPayload>>, ConsumerError> {
        self.state.assert_consuming_state()?;

        let duration = timeout;
        let consumer = self.consumer.as_mut().unwrap();
        let res = consumer.poll(duration);
        match res {
            None => Ok(None),
            Some(res) => {
                let msg = res?;
                Ok(Some(create_kafka_message(msg)))
            }
        }
    }

    fn pause(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;

        let mut topic_map = HashMap::new();
        for partition in partitions {
            let offset = *self
                .offsets
                .lock()
                .unwrap()
                .get(&partition)
                .ok_or(ConsumerError::UnassignedPartition)?;
            topic_map.insert(
                (partition.topic.name, partition.index as i32),
                Offset::from_raw(offset as i64),
            );
        }

        let consumer = self.consumer.as_ref().unwrap();
        let topic_partition_list = TopicPartitionList::from_topic_map(&topic_map).unwrap();
        consumer.pause(&topic_partition_list)?;

        Ok(())
    }

    fn resume(&mut self, partitions: HashSet<Partition>) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;

        let mut topic_partition_list = TopicPartitionList::new();
        for partition in partitions {
            if !self.offsets.lock().unwrap().contains_key(&partition) {
                return Err(ConsumerError::UnassignedPartition);
            }
            topic_partition_list.add_partition(&partition.topic.name, partition.index as i32);
        }

        let consumer = self.consumer.as_mut().unwrap();
        consumer.resume(&topic_partition_list)?;

        Ok(())
    }

    fn paused(&self) -> Result<HashSet<Partition>, ConsumerError> {
        //TODO: Implement this
        Ok(HashSet::new())
    }

    fn tell(&self) -> Result<HashMap<Partition, u64>, ConsumerError> {
        self.state.assert_consuming_state()?;
        Ok(self.offsets.lock().unwrap().clone())
    }

    fn seek(&self, _: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        //TODO: Implement this
        Ok(())
    }

    fn stage_positions(
        &mut self,
        positions: HashMap<Partition, Position>,
    ) -> Result<(), ConsumerError> {
        for (partition, position) in positions {
            self.staged_offsets.insert(partition, position);
        }
        Ok(())
    }

    fn commit_positions(&mut self) -> Result<HashMap<Partition, Position>, ConsumerError> {
        self.state.assert_consuming_state()?;

        let mut topic_map = HashMap::new();
        for (partition, position) in self.staged_offsets.iter() {
            topic_map.insert(
                (partition.topic.name.clone(), partition.index as i32),
                Offset::from_raw(position.offset as i64),
            );
        }

        let consumer = self.consumer.as_mut().unwrap();
        let partitions = TopicPartitionList::from_topic_map(&topic_map).unwrap();
        let _ = consumer.commit(&partitions, CommitMode::Sync).unwrap();

        // Clear staged offsets
        let cleared_map = HashMap::new();
        let prev_offsets = mem::replace(&mut self.staged_offsets, cleared_map);

        Ok(prev_offsets)
    }

    fn close(&mut self) {
        self.state = KafkaConsumerState::Closed;
        self.consumer = None;
    }

    fn closed(&self) -> bool {
        self.state == KafkaConsumerState::Closed
    }
}
