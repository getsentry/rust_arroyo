use crate::processing::strategies::{CommitRequest, MessageRejected, ProcessingStrategy};
use crate::types::Message;

pub struct Transform<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> {
    pub function: fn(Message<TPayload>) -> TTransformed,
    pub next_step: Box<dyn ProcessingStrategy<TTransformed>>,
}

impl<TPayload: Clone + Send + Sync, TTransformed: Clone + Send + Sync> ProcessingStrategy<TPayload>
    for Transform<TPayload, TTransformed>
{
    fn poll(&mut self) -> Option<CommitRequest> {
        None
    }

    fn submit(&mut self, message: Message<TPayload>) -> Result<(), MessageRejected> {
        let transformed = (self.function)(message.clone());

        let partition = message.partition;
        let offset = message.offset;
        let timestamp = message.timestamp;

        self.next_step.submit(Message {
            partition,
            offset,
            payload: transformed,
            timestamp,
        })
    }

    fn close(&mut self) {}

    fn terminate(&mut self) {}

    fn join(&mut self, _timeout: Option<f64>) -> Option<CommitRequest> {
        None
    }
}
