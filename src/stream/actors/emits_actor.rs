use crate::stream::{CloneableStreamable, StreamMessage}; // Assuming StreamMessage is in crate::stream
use actix::Recipient;
use actix::prelude::*;
use std::fmt::Debug; // Explicitly import Recipient

// Actor that emits elements from an iterator
#[derive(Debug)] // Added Debug for the actor itself
pub(crate) struct EmitsActor<T>
where
    T: 'static + Send + Unpin + Debug + Clone,
{
    items: std::vec::IntoIter<T>,
    downstream: Recipient<StreamMessage<T>>,
}

impl<T> EmitsActor<T>
where
    T: 'static + Send + Unpin + Debug + Clone,
{
    pub(crate) fn new(items: Vec<T>, downstream: Recipient<StreamMessage<T>>) -> Self {
        EmitsActor {
            items: items.into_iter(),
            downstream,
        }
    }
}

impl<T> Actor for EmitsActor<T>
where
    T: CloneableStreamable,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // println!("EmitsActor started. Emitting items.");
        for item in self.items.by_ref() {
            if self
                .downstream
                .try_send(StreamMessage::Element(item.clone()))
                .is_err()
            {
                // eprintln!("EmitsActor: Downstream receiver is unavailable. Stopping.");
                ctx.stop();
                return;
            }
        }
        if self.downstream.try_send(StreamMessage::End).is_err() {
            // eprintln!("EmitsActor: Failed to send End signal. Downstream might be stopped.");
        }
        ctx.stop();
    }
}

// No explicit Handler<StreamMessage<...>> for EmitsActor as it's a source, not a stage.
// It implements StreamConsumer via the blanket impl if it were to consume something,
// but its role here is to produce, not consume StreamMessages from an upstream.
