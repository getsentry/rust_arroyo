use super::AssignmentCallbacks;
use super::{Consumer as ArroyoConsumer, ConsumerError};
use crate::types::Message as ArroyoMessage;
use crate::types::{Partition, Position, Topic};
use chrono::{DateTime, NaiveDateTime, Utc};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Mutex;
use std::time::Duration;

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

impl From<KafkaError> for ConsumerError {
    fn from(err: KafkaError) -> Self {
        ConsumerError::Other(Box::new(err))
    }
}

pub struct KafkaConsumer {
    // TODO: This has to be an option as of now because rdkafka requires
    // callbacks during the instantiation. While the streaming processor
    // can only pass the callbacks during the subscribe call.
    // So we need to build the kafka consumer upon subscribe and not
    // in the constructor.
    consumer: Option<BaseConsumer<CustomContext>>,
    #[allow(dead_code)]
    group: String,
    config: HashMap<String, String>,
    staged_offsets: HashMap<Partition, Position>,
}

impl KafkaConsumer {
    pub fn new(group: String, config: HashMap<String, String>) -> Self {
        Self {
            consumer: None,
            group,
            config,
            staged_offsets: HashMap::new(),
        }
    }
}

impl<'a> ArroyoConsumer<'a, OwnedMessage> for KafkaConsumer {
    fn subscribe(
        &mut self,
        topics: &[Topic],
        callbacks: Box<dyn AssignmentCallbacks>,
    ) -> Result<(), ConsumerError> {
        let context = CustomContext {
            callbacks: Mutex::new(callbacks),
        };
        let mut config_obj = ClientConfig::new();
        for (key, val) in self.config.iter() {
            config_obj.set(key, val);
        }
        let consumer: BaseConsumer<CustomContext> = config_obj
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)?;
        let topic_str: Vec<&str> = topics.iter().map(|t| t.name.as_ref()).collect();
        consumer.subscribe(&topic_str)?;
        self.consumer = Some(consumer);
        Ok(())
    }

    fn unsubscribe(&mut self) -> Result<(), ConsumerError> {
        Ok(())
    }

    fn poll(
        &mut self,
        _: Option<f64>,
    ) -> Result<Option<ArroyoMessage<OwnedMessage>>, ConsumerError> {
        match self.consumer.as_mut() {
            None => Err(ConsumerError::ConsumerClosed),
            Some(consumer) => {
                let res = consumer.poll(Duration::from_secs(1));
                match res {
                    None => Ok(None),
                    Some(res) => {
                        let msg = res?;
                        let owned = msg.detach();
                        let topic = Topic {
                            name: owned.topic().to_string(),
                        };
                        let partition = Partition {
                            topic,
                            index: owned.partition() as u16,
                        };
                        let time_millis = owned.timestamp().to_millis().unwrap_or(0);
                        Ok(Some(ArroyoMessage::new(
                            partition,
                            owned.offset() as u64,
                            owned,
                            DateTime::from_utc(NaiveDateTime::from_timestamp(time_millis, 0), Utc),
                        )))
                    }
                }
            }
        }
    }

    fn pause(&mut self, _: HashSet<Partition>) -> Result<(), ConsumerError> {
        //TODO: Implement this
        Ok(())
    }

    fn resume(&mut self, _: HashSet<Partition>) -> Result<(), ConsumerError> {
        //TODO: Implement this
        Ok(())
    }

    fn paused(&self) -> Result<HashSet<Partition>, ConsumerError> {
        //TODO: Implement this
        Ok(HashSet::new())
    }

    fn tell(&self) -> Result<HashMap<Partition, u64>, ConsumerError> {
        //TODO: Implement this
        Ok(HashMap::new())
    }

    fn seek(&self, _: HashMap<Partition, u64>) -> Result<(), ConsumerError> {
        //TODO: Implement this
        Ok(())
    }

    fn stage_positions(
        &mut self,
        positions: HashMap<Partition, Position>,
    ) -> Result<(), ConsumerError> {
        for (partition, position) in positions.iter() {
            self.staged_offsets
                .insert(partition.clone(), position.clone());
        }
        Ok(())
    }

    fn commit_position(&mut self) -> Result<HashMap<Partition, Position>, ConsumerError> {
        let mut map = HashMap::new();
        for (partition, position) in self.staged_offsets.iter() {
            map.insert(
                (partition.topic.name.clone(), partition.index as i32),
                Offset::from_raw(position.offset as i64),
            );
        }

        match self.consumer.as_mut() {
            None => {
                return Err(ConsumerError::ConsumerClosed);
            }
            Some(consumer) => {
                let partitions = TopicPartitionList::from_topic_map(&map).unwrap();
                let _ = consumer.commit(&partitions, CommitMode::Sync);
            }
        };
        Ok(HashMap::new())
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
    use super::AssignmentCallbacks;
    use crate::types::Partition;
    use std::collections::HashMap;

    struct EmptyCallbacks {}
    impl AssignmentCallbacks for EmptyCallbacks {
        fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
        fn on_revoke(&mut self, _: Vec<Partition>) {}
    }

    #[test]
    fn test_subscribe() {}
}
