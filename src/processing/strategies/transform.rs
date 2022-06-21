use crate::processing::strategies::{
    CommitRequest, InvalidMessage, MessageRejected, ProcessingStrategy,
};
use crate::types::Message;

pub struct Transform<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> {
    pub function: fn(Message<TPayload>) -> Result<TTransformed, InvalidMessage>,
    pub next_step: Box<dyn ProcessingStrategy<TTransformed>>,
}

impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> ProcessingStrategy<TPayload>
    for Transform<TPayload, TTransformed>
{
    fn poll(&mut self) -> Option<CommitRequest> {
        None
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), MessageRejected> {
        // TODO: Handle InvalidMessage
        let transformed = (self.function)(message.clone()).unwrap();

        self.next_step.submit(Message {
            partition: message.partition,
            offset: message.offset,
            payload: transformed,
            timestamp: message.timestamp,
        })
    }

    fn close(&mut self) {}

    fn terminate(&mut self) {}

    fn join(&mut self, _timeout: Option<f64>) -> Option<CommitRequest> {
        None
    }
}
