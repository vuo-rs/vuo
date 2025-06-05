use crate::stream::StreamMessage;
use actix::prelude::*;
use std::fmt::Debug;
use std::marker::PhantomData;

#[derive(Debug)]
pub(crate) struct FilterActor<T, F>
where
    T: 'static + Send + Unpin + Debug, // Clone removed, item is moved
    F: FnMut(&T) -> bool + Send + 'static + Unpin,
{
    predicate: F,
    downstream: Recipient<StreamMessage<T>>,
    _phantom: PhantomData<T>,
}

impl<T, F> FilterActor<T, F>
where
    T: 'static + Send + Unpin + Debug,
    F: FnMut(&T) -> bool + Send + 'static + Unpin,
{
    pub(crate) fn new(predicate: F, downstream: Recipient<StreamMessage<T>>) -> Self {
        FilterActor {
            predicate,
            downstream,
            _phantom: PhantomData,
        }
    }
}

impl<T, F> Actor for FilterActor<T, F>
where
    T: 'static + Send + Unpin + Debug,
    F: FnMut(&T) -> bool + Send + 'static + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {}

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        let _ = self.downstream.try_send(StreamMessage::End);
    }
}

impl<T, F> Handler<StreamMessage<T>> for FilterActor<T, F>
where
    T: 'static + Send + Unpin + Debug,
    F: FnMut(&T) -> bool + Send + 'static + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<T>, ctx: &mut Context<Self>) {
        match msg {
            StreamMessage::Element(elem) => {
                // Predicate takes a reference. If it passes, elem is moved to downstream.
                if (self.predicate)(&elem) {
                    if self
                        .downstream
                        .try_send(StreamMessage::Element(elem))
                        .is_err()
                    {
                        ctx.stop();
                    }
                }
                // If predicate is false, elem is dropped here.
            }
            StreamMessage::End => {
                ctx.stop();
            }
        }
    }
}

// No StreamConsumer trait impl needed here due to blanket impl in stream.rs
// No specific tests for FilterActor here; its behavior is tested via Stream::filter tests.
// If actor-specific unit tests were desired, they would go in a #[cfg(test)] mod tests {} here.
