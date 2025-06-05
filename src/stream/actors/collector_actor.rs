use crate::stream::{StreamMessage, CloneableStreamable};
use actix::prelude::*; // Changed to prelude for ctx.stop()
use futures::channel::oneshot;
use std::mem;
use std::fmt;

// CollectorActor collects all incoming items into a Vec and sends the Vec
// via a oneshot channel when the input stream ends.
#[derive(fmt::Debug)]
pub(crate) struct CollectorActor<T>
where
    T: CloneableStreamable,
{
    items: Vec<T>,
    // The oneshot sender to send the final Vec<T> when the stream ends.
    result_sender: Option<oneshot::Sender<Vec<T>>>,
}

impl<T> CollectorActor<T>
where
    T: CloneableStreamable,
{
    pub(crate) fn new(result_sender: oneshot::Sender<Vec<T>>) -> Self {
        CollectorActor {
            items: Vec::new(),
            result_sender: Some(result_sender),
        }
    }
}

impl<T> Actor for CollectorActor<T>
where
    T: CloneableStreamable,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("CollectorActor started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("CollectorActor stopped. Sending collected items.");
        // Ensure that the collected items are sent when the actor stops.
        // This handles cases where the actor might be stopped for reasons other
        // than receiving StreamMessage::End (e.g., if the system shuts down).
        if let Some(sender) = self.result_sender.take() {
            // mem::take is used to move items out without needing CollectorActor.items to be Option<Vec<T>>
            let _ = sender.send(mem::take(&mut self.items));
        }
        // If sender.send fails, it means the receiver was dropped, which is acceptable.
    }
}

impl<T> Handler<StreamMessage<T>> for CollectorActor<T>
where
    T: CloneableStreamable,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<T>, ctx: &mut Context<Self>) {
        match msg {
            StreamMessage::Element(elem) => {
                // log::trace!("CollectorActor received element: {:?}", elem);
                self.items.push(elem);
            }
            StreamMessage::End => {
                // log::debug!("CollectorActor received End. Stopping actor to send results via stopped().");
                // The `stopped` method will handle sending the items.
                ctx.stop(); // This should now work with actix::prelude::*
            }
        }
    }
}