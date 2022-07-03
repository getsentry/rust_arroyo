use super::kafka::config::KafkaConfig;
use super::AssignmentCallbacks;
use super::Consumer as ArroyoConsumer;
use super::ConsumerError;
use crate::backends::kafka::types::KafkaPayload;
use crate::types::Message as ArroyoMessage;
use crate::types::{Partition, Position, Topic};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::collections::HashSet;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::timeout;

pub mod config;
mod errors;
pub mod producer;
pub mod types;

#[derive(Eq, Hash, PartialEq)]
enum KafkaConsumerState {
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
            KafkaConsumerState::Error => Err(ConsumerError::ConsumerErrored),
            _ => Ok(()),
        }
    }
}

pub fn create_kafka_message(msg: BorrowedMessage) -> ArroyoMessage<KafkaPayload> {
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

pub struct CustomContext {
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

pub fn create_and_subscribe(
    callbacks: Box<dyn AssignmentCallbacks>,
    config: KafkaConfig,
) -> Result<KafkaConsumer, ConsumerError> {
    let offsets = Arc::new(Mutex::new(HashMap::new()));
    let context = CustomContext {
        callbacks: Mutex::new(callbacks),
        consumer_offsets: offsets.clone(),
    };
    let mut config_obj: ClientConfig = config.into();

    let consumer: StreamConsumer<CustomContext> = config_obj
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)?;

    Ok(KafkaConsumer {
        consumer,
        state: KafkaConsumerState::Consuming,
        offsets,
        staged_offsets: HashMap::new(),
    })
}

pub struct KafkaConsumer {
    // TODO: This has to be an option as of now because rdkafka requires
    // callbacks during the instantiation. While the streaming processor
    // can only pass the callbacks during the subscribe call.
    // So we need to build the kafka consumer upon subscribe and not
    // in the constructor.
    pub consumer: StreamConsumer<CustomContext>,
    state: KafkaConsumerState,
    offsets: Arc<Mutex<HashMap<Partition, u64>>>,
    staged_offsets: HashMap<Partition, Position>,
}

#[async_trait]
impl<'a> ArroyoConsumer<'a, KafkaPayload> for KafkaConsumer {
    fn subscribe(&mut self, topics: &[Topic]) -> Result<(), ConsumerError> {
        let topic_str: Vec<&str> = topics.iter().map(|t| t.name.as_ref()).collect();
        self.consumer.subscribe(&topic_str)?;
        Ok(())
    }

    fn unsubscribe(&mut self) -> Result<(), ConsumerError> {
        self.state.assert_consuming_state()?;
        //let consumer = self.consumer.as_mut().unwrap();
        self.consumer.unsubscribe();

        Ok(())
    }

    async fn poll(
        &mut self,
        ttl: Option<Duration>,
    ) -> Result<Option<ArroyoMessage<KafkaPayload>>, ConsumerError> {
        self.state.assert_consuming_state()?;

        //let consumer = self.consumer.as_mut().unwrap();
        match timeout(ttl.unwrap_or(Duration::from_secs(2)), self.consumer.recv()).await {
            Ok(result) => {
                let msg = result?;
                Ok(Some(create_kafka_message(msg)))
            }
            Err(_) => Ok(None),
        }
    }

    async fn recv(&mut self) -> Result<ArroyoMessage<KafkaPayload>, ConsumerError> {
        //let consumer = self.consumer.as_mut().unwrap();
        match self.consumer.recv().await {
            Ok(result) => Ok(create_kafka_message(result)),
            Err(e) => Err(ConsumerError::BrokerError(Box::new(e))),
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

        //let consumer = self.consumer.as_ref().unwrap();
        let topic_partition_list = TopicPartitionList::from_topic_map(&topic_map).unwrap();
        self.consumer.pause(&topic_partition_list)?;

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

        //let consumer = self.consumer.as_mut().unwrap();
        self.consumer.resume(&topic_partition_list)?;

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

    async fn stage_positions(
        &mut self,
        positions: HashMap<Partition, Position>,
    ) -> Result<(), ConsumerError> {
        for (partition, position) in positions {
            self.staged_offsets.insert(partition, position);
        }
        Ok(())
    }

    async fn commit_positions(&mut self) -> Result<HashMap<Partition, Position>, ConsumerError> {
        self.state.assert_consuming_state()?;

        let mut topic_map = HashMap::new();
        for (partition, position) in self.staged_offsets.iter() {
            topic_map.insert(
                (partition.topic.name.clone(), partition.index as i32),
                Offset::from_raw(position.offset as i64),
            );
        }

        //let consumer = self.consumer.as_mut().unwrap();
        let partitions = TopicPartitionList::from_topic_map(&topic_map).unwrap();
        let _ = self.consumer.commit(&partitions, CommitMode::Sync).unwrap();

        // Clear staged offsets
        let cleared_map = HashMap::new();
        let prev_offsets = mem::replace(&mut self.staged_offsets, cleared_map);

        Ok(prev_offsets)
    }

    fn close(&mut self) {
        self.state = KafkaConsumerState::Closed;
        //self.consumer = None;
    }

    fn closed(&self) -> bool {
        self.state == KafkaConsumerState::Closed
    }
}

#[cfg(test)]
mod tests {
    use super::AssignmentCallbacks;
    use crate::backends::kafka::config::KafkaConfig;
    use crate::backends::kafka::create_and_subscribe;
    use crate::backends::Consumer;
    use crate::types::{Partition, Position, Topic};
    use chrono::Utc;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::config::ClientConfig;
    use std::collections::HashMap;
    use std::thread::sleep;
    use std::time::Duration;

    struct EmptyCallbacks {}
    impl AssignmentCallbacks for EmptyCallbacks {
        fn on_assign(&mut self, _: HashMap<Partition, u64>) {}
        fn on_revoke(&mut self, _: Vec<Partition>) {}
    }

    fn get_admin_client() -> AdminClient<DefaultClientContext> {
        let mut config = ClientConfig::new();
        config.set(
            "bootstrap.servers".to_string(),
            "localhost:9092".to_string(),
        );

        config.create().unwrap()
    }

    async fn create_topic(topic_name: &str, partition_count: i32) {
        let client = get_admin_client();
        let topics = [NewTopic::new(
            topic_name,
            partition_count,
            TopicReplication::Fixed(1),
        )];
        client
            .create_topics(&topics, &AdminOptions::new())
            .await
            .unwrap();
    }
    async fn delete_topic(topic_name: &str) {
        let client = get_admin_client();
        client
            .delete_topics(&[topic_name], &AdminOptions::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_subscribe() {
        let configuration = KafkaConfig::new_consumer_config(
            vec!["localhost:9092".to_string()],
            "my-group".to_string(),
            "latest".to_string(),
            false,
            None,
        );
        let mut consumer =
            create_and_subscribe(Box::new(EmptyCallbacks {}), configuration).unwrap();
        let topic = Topic {
            name: "test".to_string(),
        };
        consumer.subscribe(&[topic]).unwrap();
    }

    #[tokio::test]
    async fn test_tell() {
        create_topic("test", 1).await;
        let configuration = KafkaConfig::new_consumer_config(
            vec!["localhost:9092".to_string()],
            "my-group-1".to_string(),
            "latest".to_string(),
            false,
            None,
        );
        let mut consumer =
            create_and_subscribe(Box::new(EmptyCallbacks {}), configuration).unwrap();
        let topic = Topic {
            name: "test".to_string(),
        };
        assert!(consumer.tell().is_err()); // Not subscribed yet
        consumer.subscribe(&[topic]).unwrap();
        assert_eq!(consumer.tell().unwrap(), HashMap::new());

        // Getting the assignment may take a while, wait up to 5 seconds
        consumer
            .poll(Some(Duration::from_millis(5000)))
            .await
            .unwrap();
        let offsets = consumer.tell().unwrap();
        // One partition was assigned
        assert!(offsets.len() == 1);
        consumer.unsubscribe().unwrap();
        consumer.close();

        delete_topic("test").await;
    }

    #[tokio::test]
    async fn test_commit() {
        create_topic("test", 1).await;

        let configuration = KafkaConfig::new_consumer_config(
            vec!["localhost:9092".to_string()],
            "my-group-2".to_string(),
            "latest".to_string(),
            false,
            None,
        );

        let mut consumer =
            create_and_subscribe(Box::new(EmptyCallbacks {}), configuration).unwrap();
        let topic = Topic {
            name: "test".to_string(),
        };

        consumer.subscribe(&[topic.clone()]).unwrap();

        let positions = HashMap::from([(
            Partition { topic, index: 0 },
            Position {
                offset: 100,
                timestamp: Utc::now(),
            },
        )]);

        consumer.stage_positions(positions.clone()).await.unwrap();

        // Wait until the consumer got an assignment
        for _ in 0..10 {
            consumer
                .poll(Some(Duration::from_millis(5_000)))
                .await
                .unwrap();
            if consumer.tell().unwrap().len() == 1 {
                println!("Received assignment");
                break;
            }
            sleep(Duration::from_millis(200));
        }

        let res = consumer.commit_positions().await.unwrap();
        assert_eq!(res, positions);
        consumer.unsubscribe().unwrap();
        consumer.close();
        delete_topic("test").await;
    }

    #[test]
    fn test_pause() {}

    #[test]
    fn test_resume() {}
}
