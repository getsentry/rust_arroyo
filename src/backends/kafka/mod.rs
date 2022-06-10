use super::Consumer as ArroyoConsumer;
use super::{AssignmentCallbacks, ConsumeError, ConsumerClosed, PauseError, PollError};
use crate::types::Message as ArroyoMessage;
use crate::types::{Partition, Position, Topic};
use chrono::{DateTime, NaiveDateTime, Utc};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Message, OwnedHeaders};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::collections::HashSet;
use std::mem;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone)]
pub struct KafkaPayload {
    pub key: Option<Vec<u8>>,
    pub headers: Option<OwnedHeaders>,
    pub payload: Option<Vec<u8>>,
}

#[derive(PartialEq)]
enum KafkaConsumerState {
    NotSubscribed,
    Consuming,
    Error,
    Closed,
    #[allow(dead_code)]
    Assigning,
    #[allow(dead_code)]
    Revoking,
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
            key: msg.key().map(|k| k.to_vec()),
            headers: msg.headers().map(BorrowedHeaders::detach),
            payload: msg.payload().map(|p| p.to_vec()),
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
}

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(list) => {
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
                self.callbacks.lock().unwrap().on_assign(map);
            }
            Rebalance::Revoke(list) => {
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
                self.callbacks.lock().unwrap().on_revoke(partitions);
            }
            _ => {}
        }
    }

    fn post_rebalance(&self, _: &Rebalance) {}

    fn commit_callback(&self, _: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("COMMIT");
    }
}

pub struct KafkaConsumer {
    // TODO: This has to be an option as of now because rdkafka requires
    // callbacks during the instantiation. While the streaming processor
    // can only pass the callbacks during the subscribe call.
    // So we need to build the kafka consumer upon subscribe and not
    // in the constructor.
    consumer: Option<BaseConsumer<CustomContext>>,
    group: String,
    config: HashMap<String, String>,
    state: KafkaConsumerState,
    offsets: HashMap<Partition, u64>,
    staged_offsets: HashMap<Partition, Position>,
}

impl KafkaConsumer {
    pub fn new(group: String, config: HashMap<String, String>) -> Self {
        Self {
            consumer: None,
            group,
            config,
            state: KafkaConsumerState::NotSubscribed,
            offsets: HashMap::new(),
            staged_offsets: HashMap::new(),
        }
    }
}

impl<'a> ArroyoConsumer<'a, KafkaPayload> for KafkaConsumer {
    fn subscribe(
        &mut self,
        topics: &[Topic],
        callbacks: Box<dyn AssignmentCallbacks>,
    ) -> Result<(), ConsumerClosed> {
        let context = CustomContext {
            callbacks: Mutex::new(callbacks),
        };
        let mut config_obj = ClientConfig::new();
        for (key, val) in self.config.iter() {
            config_obj.set(key, val);
        }
        config_obj.set("group.id", self.group.clone());
        let consumer: BaseConsumer<CustomContext> = config_obj
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)
            .expect("Consumer creation failed");
        let topic_str: Vec<&str> = topics.iter().map(|t| t.name.as_ref()).collect();
        consumer
            .subscribe(&topic_str)
            .expect("Can't subscribe to specified topics");
        self.consumer = Some(consumer);
        self.state = KafkaConsumerState::Consuming;
        Ok(())
    }

    fn unsubscribe(&mut self) -> Result<(), ConsumerClosed> {
        Ok(())
    }

    fn poll(
        &mut self,
        timeout: Option<Duration>,
    ) -> Result<Option<ArroyoMessage<KafkaPayload>>, PollError> {
        let duration = timeout.unwrap_or(Duration::from_millis(100));

        match self.consumer.as_mut() {
            None => Err(PollError::ConsumerClosed),
            Some(consumer) => {
                let res = consumer.poll(duration);
                match res {
                    None => Ok(None),
                    Some(res) => match res {
                        Ok(msg) => Ok(Some(create_kafka_message(msg))),
                        Err(_) => Err(PollError::ConsumerClosed),
                    },
                }
            }
        }
    }

    fn pause(&mut self, _: HashSet<Partition>) -> Result<(), PauseError> {
        //TODO: Implement this
        Ok(())
    }

    fn resume(&mut self, _: HashSet<Partition>) -> Result<(), PauseError> {
        //TODO: Implement this
        Ok(())
    }

    fn paused(&self) -> Result<HashSet<Partition>, ConsumerClosed> {
        //TODO: Implement this
        Ok(HashSet::new())
    }

    fn tell(&self) -> Result<HashMap<Partition, u64>, ConsumerClosed> {
        if [
            KafkaConsumerState::Closed,
            KafkaConsumerState::Error,
            KafkaConsumerState::NotSubscribed,
        ]
        .contains(&self.state)
        {
            return Err(ConsumerClosed);
        }

        Ok(self.offsets.clone())
    }

    fn seek(&self, _: HashMap<Partition, u64>) -> Result<(), ConsumeError> {
        //TODO: Implement this
        Ok(())
    }

    fn stage_positions(
        &mut self,
        positions: HashMap<Partition, Position>,
    ) -> Result<(), ConsumeError> {
        for (partition, position) in positions {
            self.staged_offsets.insert(partition, position);
        }
        Ok(())
    }

    fn commit_positions(&mut self) -> Result<HashMap<Partition, Position>, ConsumerClosed> {
        let mut topic_map = HashMap::new();
        for (partition, position) in self.staged_offsets.iter() {
            topic_map.insert(
                (partition.topic.name.clone(), partition.index as i32),
                Offset::from_raw(position.offset as i64),
            );
        }

        let consumer = self.consumer.as_mut().ok_or(ConsumerClosed)?;
        let partitions = TopicPartitionList::from_topic_map(&topic_map).unwrap();
        let _ = consumer.commit(&partitions, CommitMode::Sync).unwrap();

        // Clear staged offsets
        let cleared_map = HashMap::new();
        let prev_offsets = mem::replace(&mut self.staged_offsets, cleared_map);

        Ok(prev_offsets)
    }

    fn close(&mut self, _: Option<f64>) {
        //TODO: Implement this
    }

    fn closed(&self) -> bool {
        //TODO: Implement this
        false
    }
}

#[cfg(test)]
mod tests {
    use super::{AssignmentCallbacks, KafkaConsumer};
    use crate::backends::Consumer;
    use crate::types::{Partition, Topic};
    use std::collections::HashMap;

    struct EmptyCallbacks {}
    impl AssignmentCallbacks for EmptyCallbacks {
        fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
        fn on_revoke(&mut self, _: Vec<Partition>) {}
    }

    #[test]
    fn test_subscribe() {
        let mut consumer = KafkaConsumer::new("my-group".to_string(), HashMap::new());
        let topic = Topic {
            name: "test".to_string(),
        };
        let my_callbacks: Box<dyn AssignmentCallbacks> = Box::new(EmptyCallbacks {});
        consumer.subscribe(&[topic], my_callbacks).unwrap();
    }

    #[test]
    fn test_commit() {}

    #[test]
    fn test_tell() {
        let mut consumer = KafkaConsumer::new("my-group".to_string(), HashMap::new());
        let topic = Topic {
            name: "test".to_string(),
        };
        let my_callbacks: Box<dyn AssignmentCallbacks> = Box::new(EmptyCallbacks {});
        assert!(consumer.tell().is_err()); // Not subscribed yet
        consumer.subscribe(&[topic], my_callbacks).unwrap();
        assert_eq!(consumer.tell().unwrap(), HashMap::new());
    }
}
