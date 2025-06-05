use crate::stream::{StreamMessage, Streamable};
use actix::prelude::*;

// TakeActor takes a specified number of elements from the stream and then stops.
#[derive(Debug)] // Added Debug for the actor struct itself
pub(crate) struct TakeActor<T>
where
    T: Streamable, // Using Streamable for concise and correct bounds
{
    remaining: u64, // Number of items still to take
    downstream: Recipient<StreamMessage<T>>,
    // Flag to ensure End is sent only once, either when `remaining` hits 0 or when upstream ends.
    downstream_ended: bool,
}

impl<T> TakeActor<T>
where
    T: Streamable,
{
    pub(crate) fn new(count: u64, downstream: Recipient<StreamMessage<T>>) -> Self {
        TakeActor {
            remaining: count,
            downstream,
            downstream_ended: false,
        }
    }

    // Helper to send End to downstream and mark as ended.
    // Returns true if End was successfully sent or was already sent.
    // Returns false if try_send failed, indicating downstream is closed.
    fn try_end_downstream(&mut self) -> bool {
        if !self.downstream_ended {
            if self.downstream.try_send(StreamMessage::End).is_err() {
                // log::warn!(\"TakeActor: Downstream recipient closed while trying to send End.\");
                self.downstream_ended = true; // Mark as ended even if send failed
                return false; // Indicate send failure
            }
            self.downstream_ended = true;
        }
        true
    }
}

impl<T> Actor for TakeActor<T>
where
    T: Streamable,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // log::debug!(\"TakeActor started. Items to take: {}\", self.remaining);
        if self.remaining == 0 {
            // If count is 0, immediately end the downstream and stop.
            // log::debug!(\"TakeActor: Taking 0 items. Sending End immediately.\");
            self.try_end_downstream();
            ctx.stop();
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // log::debug!(\"TakeActor stopped. Ensuring downstream is ended.\");
        // Ensure downstream is ended if the actor stops for any reason
        // (e.g., downstream closed, manual stop, or after normal completion).
        self.try_end_downstream();
    }
}

impl<T> Handler<StreamMessage<T>> for TakeActor<T>
where
    T: Streamable, // Item T is moved in StreamMessage::Element(elem)
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<T>, ctx: &mut Context<Self>) {
        if self.downstream_ended {
            // If downstream has already been ended (either by us or it closed),
            // we just consume and ignore further messages, stopping on End.
            if matches!(msg, StreamMessage::End) {
                ctx.stop(); // Stop the actor if upstream ends after we're done.
            }
            return;
        }

        match msg {
            StreamMessage::Element(elem) => {
                // log::trace!(\"TakeActor received element: {:?}, remaining: {}\", elem, self.remaining);
                if self.remaining > 0 {
                    if self.downstream.try_send(StreamMessage::Element(elem)).is_err() {
                        // log::warn!(\"TakeActor: Downstream recipient closed. Stopping actor.\");
                        // If downstream send fails, end it and stop.
                        self.try_end_downstream(); // Mark as ended, try to send End
                        ctx.stop();
                        return;
                    }
                    self.remaining -= 1;
                    if self.remaining == 0 {
                        // log::debug!(\"TakeActor: Reached take limit. Sending End and stopping.\");
                        // Reached the count, send End to downstream and stop this actor.
                        self.try_end_downstream();
                        // We can stop immediately, or wait for upstream End to stop.
                        // Stopping now is more efficient if upstream is infinite or long.
                        ctx.stop(); 
                    }
                } else {
                    // This case should ideally not be reached if remaining == 0 handling is correct,
                    // as downstream_ended should be true.
                    // However, as a safeguard:
                    // log::trace!(\"TakeActor: Received element but remaining is 0 and downstream not marked ended. Ending downstream.\");
                    self.try_end_downstream();
                    ctx.stop();
                }
            }
            StreamMessage::End => {
                // log::debug!(\"TakeActor received End from upstream.\");
                // Upstream ended. Ensure downstream also gets an End signal, then stop.
                self.try_end_downstream();
                ctx.stop();
            }
        }
    }
}