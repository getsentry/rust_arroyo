use crate::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
};
use crate::types::Message;
use std::time::Duration;

pub struct Transform<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> {
    pub function: fn(&Message<TPayload>) -> Result<TTransformed, InvalidMessage>,
    pub next_step: Box<dyn ProcessingStrategy<TTransformed>>,
}

impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> ProcessingStrategy<TPayload>
    for Transform<TPayload, TTransformed>
{
    fn poll(&mut self) -> Option<CommitRequest> {
        self.next_step.poll()
    }

    fn submit(&mut self, message: &Message<TPayload>) -> Result<(), MessageRejected> {
        // TODO: Handle InvalidMessage
        let transformed = (self.function)(message).unwrap();

        self.next_step.submit(&Message {
            partition: message.partition.clone(),
            offset: message.offset,
            payload: transformed,
            timestamp: message.timestamp,
        })
    }

    fn close(&mut self) {
        self.next_step.close()
    }

    fn terminate(&mut self) {
        self.next_step.terminate()
    }

    fn join(&mut self, timeout: Option<Duration>) -> Option<CommitRequest> {
        self.next_step.join(timeout)
    }
}
