use crate::stream::{StreamMessage, Streamable};
use actix::prelude::*;
use std::marker::PhantomData;

// DrainActor consumes all items from a stream and emits a single () when the input stream ends.
#[derive(Debug)] // Added Debug for the actor struct itself
pub(crate) struct DrainActor<Out>
where
    Out: Streamable, // Using Streamable for concise and correct bounds
{
    downstream: Recipient<StreamMessage<()>>,
    drained_successfully: bool, // Tracks if the End -> Element(()) -> End sequence was successful
    _phantom: PhantomData<Out>,
}

impl<Out> DrainActor<Out>
where
    Out: Streamable,
{
    pub(crate) fn new(downstream: Recipient<StreamMessage<()>>) -> Self {
        Self {
            downstream,
            drained_successfully: false,
            _phantom: PhantomData,
        }
    }
}

impl<Out> Actor for DrainActor<Out>
where
    Out: Streamable,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("DrainActor started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("DrainActor stopped. Drained successfully: {}.", self.drained_successfully);
        // If the actor stops for any reason other than a successful drain (e.g., downstream closed),
        // ensure an End message is attempted if not already handled by the successful drain path.
        if !self.drained_successfully {
            let _ = self.downstream.try_send(StreamMessage::End);
        }
    }
}

impl<Out> Handler<StreamMessage<Out>> for DrainActor<Out>
where
    Out: Streamable,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<Out>, ctx: &mut Context<Self>) {
        match msg {
            StreamMessage::Element(_item) => {
                // log::trace!("DrainActor consuming element: {:?}", _item);
                // Elements are consumed and discarded.
            }
            StreamMessage::End => {
                // log::debug!("DrainActor received End from upstream.");
                // Try to send the single () element.
                if self.downstream.try_send(StreamMessage::Element(())).is_ok() {
                    // If successful, mark that the primary operation completed.
                    self.drained_successfully = true;
                    // Then send the End message for the () stream.
                    let _ = self.downstream.try_send(StreamMessage::End);
                } else {
                    // log::warn!("DrainActor: Downstream recipient closed before Element(()) could be sent.");
                    // If sending Element(()) fails, downstream is closed.
                    // The `stopped` method will attempt to send End, or it's already implied as closed.
                    // No need to set drained_successfully to true here.
                }
                ctx.stop(); // Stop the actor as the input stream has ended.
            }
        }
    }
}
