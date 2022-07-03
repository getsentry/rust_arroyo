pub mod strategies;

use crate::backends::kafka::config::KafkaConfig;
use crate::backends::kafka::create_and_subscribe;
use crate::backends::kafka::types::KafkaPayload;
use crate::backends::kafka::KafkaConsumer;
use crate::backends::{AssignmentCallbacks, Consumer};
use crate::processing::strategies::async_noop::AsyncNoopCommit;
use crate::types::{Message, Partition, Topic};
use async_mutex::Mutex;
use futures::executor::block_on;
use log::error;
use std::collections::HashMap;
use std::mem::replace;
use std::sync::Arc;
use std::time::Duration;
use strategies::{ProcessingStrategy, ProcessingStrategyFactory};
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub struct InvalidState;

#[derive(Debug, Clone)]
pub struct PollError;

#[derive(Debug, Clone)]
pub struct PauseError;

#[derive(Debug, Clone)]
pub enum RunError {
    InvalidState,
    PollError,
    PauseError,
}

struct Strategies<TPayload: Clone> {
    processing_factory: Box<dyn ProcessingStrategyFactory<TPayload>>,
    strategy: Option<Box<dyn ProcessingStrategy<TPayload>>>,
}

struct Callbacks<TPayload: Clone> {
    strategies: Arc<Mutex<Strategies<TPayload>>>,
}

impl<TPayload: 'static + Clone> AssignmentCallbacks for Callbacks<TPayload> {
    // TODO: Having the initialization of the strategy here
    // means that ProcessingStrategy and ProcessingStrategyFactory
    // have to be Send and Sync, which is really limiting and unnecessary.
    // Revisit this so that it is not the callback that perform the
    // initialization.  But we just provide a signal back to the
    // processor to do that.
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {
        let mut stg = block_on(self.strategies.lock());
        stg.strategy = Some(stg.processing_factory.create());
    }
    fn on_revoke(&mut self, _: Vec<Partition>) {
        let mut stg = block_on(self.strategies.lock());
        match stg.strategy.as_mut() {
            None => {}
            Some(s) => {
                s.close();
                s.join(None);
            }
        }
        stg.strategy = None;
    }
}

impl<TPayload: Clone> Callbacks<TPayload> {
    pub fn new(strategies: Arc<Mutex<Strategies<TPayload>>>) -> Self {
        Self { strategies }
    }
}

/// A stream processor manages the relationship between a ``Consumer``
/// instance and a ``ProcessingStrategy``, ensuring that processing
/// strategies are instantiated on partition assignment and closed on
/// partition revocation.
pub struct StreamProcessor<'a, TPayload: Clone> {
    consumer: Box<dyn Consumer<'a, TPayload> + 'a>,
    strategies: Arc<Mutex<Strategies<TPayload>>>,
    message: Option<Message<TPayload>>,
    shutdown_requested: bool,
}

pub fn create<'a>(
    config: KafkaConfig,
    processing_factory: Box<dyn ProcessingStrategyFactory<KafkaPayload>>,
) -> StreamProcessor<'a, KafkaPayload> {
    let strategies = Arc::new(Mutex::new(Strategies {
        processing_factory,
        strategy: None,
    }));
    let callbacks: Box<dyn AssignmentCallbacks> = Box::new(Callbacks::new(strategies.clone()));

    let consumer = create_and_subscribe(callbacks, config).unwrap();

    StreamProcessor {
        consumer: Box::new(consumer),
        strategies,
        message: None,
        shutdown_requested: false,
    }
}

impl<'a, TPayload: 'static + Clone> StreamProcessor<'a, TPayload> {
    pub fn new(
        consumer: Box<dyn Consumer<'a, TPayload> + 'a>,
        processing_factory: Box<dyn ProcessingStrategyFactory<TPayload>>,
    ) -> Self {
        let strategies = Arc::new(Mutex::new(Strategies {
            processing_factory,
            strategy: None,
        }));

        Self {
            consumer,
            strategies,
            message: None,
            shutdown_requested: false,
        }
    }

    pub fn subscribe(&mut self, topic: Topic) {
        //let callbacks: Box<dyn AssignmentCallbacks> =
        //    Box::new(Callbacks::new(self.strategies.clone()));
        let _ = self.consumer.subscribe(&[topic]);
    }

    pub async fn run_once(&mut self) -> Result<(), RunError> {
        let message_carried_over = self.message.is_some();

        if message_carried_over {
            // If a message was carried over from the previous run, the consumer
            // should be paused and not returning any messages on ``poll``.
            let res = self.consumer.poll(Some(Duration::ZERO)).await.unwrap();
            match res {
                None => {}
                Some(_) => return Err(RunError::InvalidState),
            }
        } else {
            // Otherwise, we need to try fetch a new message from the consumer,
            // even if there is no active assignment and/or processing strategy.
            let msg = self.consumer.poll(Some(Duration::ZERO)).await;
            //TODO: Support errors properly
            match msg {
                Ok(m) => self.message = m,
                Err(_) => return Err(RunError::PollError),
            }
        }

        let mut trait_callbacks = self.strategies.lock().await;
        match trait_callbacks.strategy.as_mut() {
            None => match self.message.as_ref() {
                None => {}
                Some(_) => return Err(RunError::InvalidState),
            },
            Some(strategy) => {
                let commit_request = strategy.poll().await;
                match commit_request {
                    None => {}
                    Some(request) => {
                        self.consumer
                            .stage_positions(request.positions)
                            .await
                            .unwrap();
                        self.consumer.commit_positions().await.unwrap();
                    }
                };

                let msg = replace(&mut self.message, None);
                if let Some(msg_s) = msg {
                    let ret = strategy.submit(msg_s).await;
                    match ret {
                        Ok(()) => {}
                        Err(_) => {
                            // If the processing strategy rejected our message, we need
                            // to pause the consumer and hold the message until it is
                            // accepted, at which point we can resume consuming.
                            let partitions =
                                self.consumer.tell().unwrap().keys().cloned().collect();
                            if message_carried_over {
                                let res = self.consumer.pause(partitions);
                                match res {
                                    Ok(()) => {}
                                    Err(_) => return Err(RunError::PauseError),
                                }
                            } else {
                                let res = self.consumer.resume(partitions);
                                match res {
                                    Ok(()) => {}
                                    Err(_) => return Err(RunError::PauseError),
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// The main run loop, see class docstring for more information.
    pub async fn run(&mut self) -> Result<(), RunError> {
        while !self.shutdown_requested {
            let ret = self.run_once().await;
            match ret {
                Ok(()) => {}
                Err(e) => {
                    let mut trait_callbacks = self.strategies.lock().await;

                    match trait_callbacks.strategy.as_mut() {
                        None => {}
                        Some(strategy) => {
                            strategy.terminate();
                        }
                    }
                    self.consumer.close();
                    return Err(e);
                }
            }
        }
        self.shutdown();
        Ok(())
    }

    pub fn signal_shutdown(&mut self) {
        self.shutdown_requested = true;
    }

    pub fn shutdown(&mut self) {
        self.consumer.close();
    }

    pub fn tell(self) -> HashMap<Partition, u64> {
        self.consumer.tell().unwrap()
    }
}

struct EmptyCallbacks {}
impl AssignmentCallbacks for EmptyCallbacks {
    fn on_assign(&mut self, _: HashMap<Partition, u64>) {
        println!("Assignment");
    }
    fn on_revoke(&mut self, _: Vec<Partition>) {
        println!("Revoked");
    }
}

pub fn create_streaming<'a>(
    config: KafkaConfig,
    strategy: AsyncNoopCommit,
    topic: Topic,
) -> StreamingStreamProcessor {
    let mut consumer = create_and_subscribe(Box::new(EmptyCallbacks {}), config).unwrap();
    consumer.subscribe(&[topic]).unwrap();
    StreamingStreamProcessor {
        consumer,
        strategy,
        message: None,
        shutdown_requested: false,
    }
}

pub struct StreamingStreamProcessor {
    pub consumer: KafkaConsumer,
    pub strategy: AsyncNoopCommit,
    pub message: Option<Message<KafkaPayload>>,
    pub shutdown_requested: bool,
}

impl StreamingStreamProcessor {
    pub async fn run_once(&mut self) -> Result<(), RunError> {
        let message_carried_over = self.message.is_some();
        if message_carried_over {
            // If a message was carried over from the previous run, the consumer
            // should be paused and not returning any messages on ``poll``.
            let res = timeout(Duration::from_secs(2), self.consumer.recv()).await;
            match res {
                Err(_) => {}
                Ok(_) => return Err(RunError::InvalidState),
            }
        } else {
            // Otherwise, we need to try fetch a new message from the consumer,
            // even if there is no active assignment and/or processing strategy.
            let msg = timeout(Duration::from_secs(2), self.consumer.recv()).await;
            //TODO: Support errors properly
            match msg {
                Ok(m) => match m {
                    Ok(msg) => {
                        self.message = Some(msg);
                    }
                    Err(_) => return Err(RunError::PollError),
                },
                Err(_) => self.message = None,
            }
        }

        let commit_request = self.strategy.poll().await;
        match commit_request {
            None => {}
            Some(request) => {
                //let part_list = build_topic_partitions(request);
                self.consumer
                    .stage_positions(request.positions)
                    .await
                    .unwrap();
                self.consumer.commit_positions().await.unwrap();

                //self.consumer.commit(&part_list, CommitMode::Sync).unwrap();
                //info!("Committed: {:?}", part_list);
            }
        };

        let msg = replace(&mut self.message, None);
        if let Some(msg_s) = msg {
            self.strategy.submit(msg_s).await;
        }
        Ok(())
    }

    pub async fn _run_once(&mut self) -> Result<(), RunError> {
        match timeout(Duration::from_secs(2), self.consumer.recv()).await {
            Ok(result) => match result {
                Err(e) => panic!("Kafka error: {}", e),
                Ok(m) => {
                    match self.strategy.poll().await {
                        Some(partition_list) => {
                            //let part_list = build_topic_partitions(partition_list);
                            //self.consumer.commit(&part_list, CommitMode::Sync).unwrap();
                            self.consumer
                                .stage_positions(partition_list.positions)
                                .await
                                .unwrap();
                            self.consumer.commit_positions().await.unwrap();
                            //info!("Committed: {:?}", part_list);
                        }
                        None => {}
                    }

                    self.strategy.submit(m).await;
                }
            },
            Err(_) => {
                error!("timeoout, flushing batch");
                match self.strategy.poll().await {
                    Some(partition_list) => {
                        //let part_list = build_topic_partitions(partition_list);
                        //self.consumer.commit(&part_list, CommitMode::Sync).unwrap();
                        //info!("Committed: {:?}", part_list);
                        self.consumer
                            .stage_positions(partition_list.positions)
                            .await
                            .unwrap();
                        self.consumer.commit_positions().await.unwrap();
                    }
                    None => {}
                }
            }
        }
        Ok(())
    }

    /// The main run loop, see class docstring for more information.
    pub async fn run(&mut self) -> Result<(), RunError> {
        while !self.shutdown_requested {
            match self.run_once().await {
                Ok(()) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    pub fn signal_shutdown(&mut self) {
        self.shutdown_requested = true;
    }
}

#[cfg(test)]
mod tests {
    use super::strategies::{
        CommitRequest, MessageRejected, ProcessingStrategy, ProcessingStrategyFactory,
    };
    use crate::types::{Message, Position};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::time::Duration;

    struct TestStrategy {
        message: Option<Message<String>>,
    }
    #[async_trait]
    impl ProcessingStrategy<String> for TestStrategy {
        #[allow(clippy::manual_map)]
        async fn poll(&mut self) -> Option<CommitRequest> {
            match self.message.as_ref() {
                None => None,
                Some(message) => Some(CommitRequest {
                    positions: HashMap::from([(
                        message.partition.clone(),
                        Position {
                            offset: message.offset,
                            timestamp: message.timestamp,
                        },
                    )]),
                }),
            }
        }

        async fn submit(&mut self, message: Message<String>) -> Result<(), MessageRejected> {
            self.message = Some(message);
            Ok(())
        }

        fn close(&mut self) {}

        fn terminate(&mut self) {}

        fn join(&mut self, _: Option<Duration>) -> Option<CommitRequest> {
            None
        }
    }

    struct TestFactory {}
    impl ProcessingStrategyFactory<String> for TestFactory {
        fn create(&self) -> Box<dyn ProcessingStrategy<String>> {
            Box::new(TestStrategy { message: None })
        }
    }
}
