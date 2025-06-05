use crate::stream::{CloneableStreamable, StreamMessage, Streamable};
use actix::prelude::*; // Changed to prelude

// TakeWhileActor takes elements from the stream as long as the predicate is true.
// Once the predicate returns false, it stops taking elements and ends the downstream.
#[derive(Debug)]
pub(crate) struct TakeWhileActor<Out, P>
where
    Out: Streamable, // Using Streamable for concise and correct bounds
    P: FnMut(&Out) -> bool + Send + 'static + Unpin, // Predicate function
{
    predicate: P,
    downstream: Recipient<StreamMessage<Out>>,
    is_taking: bool, // State to track if we are still in the "taking" phase
}

impl<Out, P> TakeWhileActor<Out, P>
where
    Out: Streamable,
    P: FnMut(&Out) -> bool + Send + 'static + Unpin,
{
    pub(crate) fn new(predicate: P, downstream: Recipient<StreamMessage<Out>>) -> Self {
        Self {
            predicate,
            downstream,
            is_taking: true, // Start in the taking phase
        }
    }
}

impl<Out, P> Actor for TakeWhileActor<Out, P>
where
    Out: Streamable,
    P: FnMut(&Out) -> bool + Send + 'static + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("TakeWhileActor started. Initial state: is_taking = {}", self.is_taking);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("TakeWhileActor stopped. Ensuring End message is sent if still taking or not properly ended.");
        // If the actor stops for any reason (e.g. downstream closed, or manually stopped)
        // and we were still in the "taking" phase, or if the End wasn't sent explicitly
        // after predicate turned false, ensure downstream gets an End.
        // This handles cases where the actor might stop before the predicate turns false
        // or before an upstream End is received.
        if self.is_taking {
            let _ = self.downstream.try_send(StreamMessage::End);
        }
        // If !self.is_taking, it means either predicate became false (End was sent)
        // or upstream End was received (End was sent).
    }
}

impl<Out, P> Handler<StreamMessage<Out>> for TakeWhileActor<Out, P>
where
    Out: CloneableStreamable, // Clone needed for StreamMessage::Element(item.clone())
    P: FnMut(&Out) -> bool + Send + 'static + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<Out>, ctx: &mut Context<Self>) {
        if !self.is_taking {
            // Already stopped taking, or upstream ended and this actor is shutting down.
            // If upstream sends End after we've stopped taking, we still need to stop the actor.
            if matches!(msg, StreamMessage::End) {
                // log::debug!("TakeWhileActor: Received End after stop_taking. Ensuring actor stops.");
                ctx.stop(); // Will trigger `stopped()` which ensures End to downstream if needed.
            }
            return;
        }

        match msg {
            StreamMessage::Element(item) => {
                // log::trace!("TakeWhileActor received element: {:?}, is_taking: {}", item, self.is_taking);
                if (self.predicate)(&item) {
                    // Predicate is true, forward the item.
                    // log::trace!("TakeWhileActor: Predicate true for {:?}. Forwarding.", item);
                    if self
                        .downstream
                        .try_send(StreamMessage::Element(item.clone()))
                        .is_err()
                    {
                        // log::warn!("TakeWhileActor: Downstream recipient closed. Stopping actor and further takes.");
                        self.is_taking = false; // No longer taking as downstream is gone
                        ctx.stop(); // Stop the actor
                    }
                } else {
                    // Predicate is false, stop taking and end the stream.
                    // log::debug!("TakeWhileActor: Predicate became false for {:?}. Stopping take and sending End.", item);
                    self.is_taking = false; // Transition out of taking state
                    let _ = self.downstream.try_send(StreamMessage::End); // Notify downstream that this stream part is ending
                    ctx.stop(); // Stop this actor
                }
            }
            StreamMessage::End => {
                // log::debug!("TakeWhileActor received End from upstream while still taking.");
                // Upstream ended while we were still taking.
                self.is_taking = false; // No longer in a taking state because upstream ended.
                let _ = self.downstream.try_send(StreamMessage::End); // Propagate End to downstream.
                ctx.stop(); // Stop this actor.
            }
        }
    }
}
