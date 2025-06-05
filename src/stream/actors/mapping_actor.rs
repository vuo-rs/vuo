use crate::stream::{StreamMessage, Streamable, CloneableStreamable};
use actix::prelude::*; // Changed to prelude
use std::marker::PhantomData;
// Removed: use std::fmt;

#[derive(Debug)] // Added Debug for the actor struct itself
pub(crate) struct MappingActor<In, Out, F>
where
    In: Streamable,          // Input type, requires Streamable
    Out: CloneableStreamable, // Output type, requires CloneableStreamable for messages
    F: FnMut(In) -> Out + Send + 'static + Clone + Unpin, // Mapping function
{
    map_fn: F,
    downstream: Recipient<StreamMessage<Out>>,
    _phantom_in: PhantomData<In>, // To hold the In type
}

impl<In, Out, F> MappingActor<In, Out, F>
where
    In: Streamable,
    Out: CloneableStreamable,
    F: FnMut(In) -> Out + Send + 'static + Clone + Unpin,
{
    pub(crate) fn new(map_fn: F, downstream: Recipient<StreamMessage<Out>>) -> Self {
        MappingActor {
            map_fn,
            downstream,
            _phantom_in: PhantomData,
        }
    }
}

impl<In, Out, F> Actor for MappingActor<In, Out, F>
where
    In: Streamable,
    Out: CloneableStreamable,
    F: FnMut(In) -> Out + Send + 'static + Clone + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("MappingActor started.");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("MappingActor stopped, sending End to downstream.");
        // Ensure downstream is notified if the actor stops for any reason.
        let _ = self.downstream.try_send(StreamMessage::End);
    }
}

impl<In, Out, F> Handler<StreamMessage<In>> for MappingActor<In, Out, F>
where
    In: Streamable, // In is moved into map_fn
    Out: CloneableStreamable, // Out is produced and sent
    F: FnMut(In) -> Out + Send + 'static + Clone + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<In>, ctx: &mut Context<Self>) {
        // If actor is already stopping/stopped, ignore new messages.
        if ctx.state() == ActorState::Stopping || ctx.state() == ActorState::Stopped {
            return;
        }

        match msg {
            StreamMessage::Element(elem) => {
                // log::trace!("MappingActor received element: {:?}", elem);
                let mapped_elem = (self.map_fn)(elem); // elem is moved here
                // log::trace!("MappingActor mapped to: {:?}", mapped_elem);
                if self.downstream.try_send(StreamMessage::Element(mapped_elem)).is_err() {
                    // log::warn!("MappingActor: Downstream recipient is closed. Stopping actor.");
                    if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                        ctx.stop(); // Should work now
                    }
                }
            }
            StreamMessage::End => {
                // log::debug!("MappingActor received End. Stopping actor.");
                // Upstream ended. The `stopped` method will ensure downstream gets an End message.
                if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop(); // Should work now
                }
            }
        }
    }
}