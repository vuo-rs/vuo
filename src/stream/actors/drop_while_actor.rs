use crate::stream::{CloneableStreamable, StreamMessage, Streamable};
use actix::prelude::*; // Changed to prelude

// DropWhileActor drops elements from the stream while the predicate is true,
// then passes through all subsequent elements.
#[derive(Debug)]
pub(crate) struct DropWhileActor<Out, P>
where
    Out: Streamable,
    P: FnMut(&Out) -> bool + Send + 'static + Unpin,
{
    predicate: P,
    downstream: Recipient<StreamMessage<Out>>,
    is_dropping: bool,
}

impl<Out, P> DropWhileActor<Out, P>
where
    Out: Streamable,
    P: FnMut(&Out) -> bool + Send + 'static + Unpin,
{
    pub(crate) fn new(predicate: P, downstream: Recipient<StreamMessage<Out>>) -> Self {
        Self {
            predicate,
            downstream,
            is_dropping: true,
        }
    }
}

impl<Out, P> Actor for DropWhileActor<Out, P>
where
    Out: Streamable,
    P: FnMut(&Out) -> bool + Send + 'static + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("DropWhileActor started. Initial state: is_dropping = {}", self.is_dropping);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("DropWhileActor stopped. Ensuring End message is sent to downstream.");
        let _ = self.downstream.try_send(StreamMessage::End);
    }
}

impl<Out, P> Handler<StreamMessage<Out>> for DropWhileActor<Out, P>
where
    Out: CloneableStreamable,
    P: FnMut(&Out) -> bool + Send + 'static + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<Out>, ctx: &mut Context<Self>) {
        // If actor is already stopping/stopped, ignore new messages.
        if ctx.state() == ActorState::Stopping || ctx.state() == ActorState::Stopped {
            return;
        }

        match msg {
            StreamMessage::Element(item) => {
                if self.is_dropping {
                    if !(self.predicate)(&item) {
                        self.is_dropping = false;
                        if self
                            .downstream
                            .try_send(StreamMessage::Element(item.clone()))
                            .is_err()
                        {
                            if ctx.state() != ActorState::Stopping
                                && ctx.state() != ActorState::Stopped
                            {
                                ctx.stop();
                            }
                        }
                    }
                } else {
                    if self
                        .downstream
                        .try_send(StreamMessage::Element(item))
                        .is_err()
                    {
                        if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped
                        {
                            ctx.stop();
                        }
                    }
                }
            }
            StreamMessage::End => {
                if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop();
                }
            }
        }
    }
}
