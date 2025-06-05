use crate::stream::CloneableStreamable;
use crate::stream::Stream as VirtaStream;
use crate::stream::StreamMessage;
use crate::stream::Streamable; // Alias our Stream struct

use actix::prelude::*; // Changed to prelude
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;

// Items from the sibling inner_stream_proxy_actor module

use super::inner_stream_proxy_actor::*;

// The S generic parameter is removed. FStream now directly produces VirtaStream<NewOut>.
#[derive(Debug)]
pub(crate) struct ConcatMapActor<Out, NewOut, FStream>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    FStream: FnMut(Out) -> VirtaStream<NewOut> + Send + 'static + Clone + Unpin, // Use aliased VirtaStream
{
    map_to_stream_fn: FStream,
    main_downstream: Recipient<StreamMessage<NewOut>>,
    outer_stream_buffer: VecDeque<Out>,
    is_inner_stream_active: bool,
    outer_stream_ended: bool,
    _phantom: PhantomData<(Out, NewOut)>,
}

impl<Out, NewOut, FStream> ConcatMapActor<Out, NewOut, FStream>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    FStream: FnMut(Out) -> VirtaStream<NewOut> + Send + 'static + Clone + Unpin, // Use aliased VirtaStream
{
    pub(crate) fn new(map_fn: FStream, downstream: Recipient<StreamMessage<NewOut>>) -> Self {
        Self {
            map_to_stream_fn: map_fn,
            main_downstream: downstream,
            outer_stream_buffer: VecDeque::new(),
            is_inner_stream_active: false,
            outer_stream_ended: false,
            _phantom: PhantomData,
        }
    }

    fn try_process_next_outer_item(&mut self, ctx: &mut Context<Self>) {
        if self.is_inner_stream_active || self.outer_stream_buffer.is_empty() {
            if self.outer_stream_ended
                && self.outer_stream_buffer.is_empty()
                && !self.is_inner_stream_active
            {
                let _ = self.main_downstream.try_send(StreamMessage::End);
                ctx.stop();
            }
            return;
        }

        if let Some(outer_item) = self.outer_stream_buffer.pop_front() {
            let inner_stream: VirtaStream<NewOut> = (self.map_to_stream_fn)(outer_item); // Use aliased VirtaStream
            self.is_inner_stream_active = true;

            let items_recipient_for_proxy = ctx.address().recipient::<InnerStreamItemMsg<NewOut>>();
            let completion_recipient_for_proxy =
                ctx.address().recipient::<InnerStreamCompletedMsg>();

            let proxy_actor_addr = InnerStreamProxyActor::new(
                items_recipient_for_proxy,
                completion_recipient_for_proxy,
            )
            .start();

            let recipient_for_inner_stream_setup =
                proxy_actor_addr.recipient::<StreamMessage<NewOut>>();

            let inner_setup_fn = inner_stream.setup_fn; // Access setup_fn from VirtaStream
            let actor_addr_clone_for_task = ctx.address();

            let fut = async move {
                if (inner_setup_fn)(recipient_for_inner_stream_setup)
                    .await
                    .is_err()
                {
                    // If inner stream setup fails, signal its completion to self.
                    let _ = actor_addr_clone_for_task.try_send(InnerStreamCompletedMsg);
                }
            };
            // Spawning the future on the actor's context
            fut.into_actor(self).spawn(ctx);
        } else if self.outer_stream_ended && !self.is_inner_stream_active {
            let _ = self.main_downstream.try_send(StreamMessage::End);
            ctx.stop();
        }
    }
}

impl<Out, NewOut, FStream> Actor for ConcatMapActor<Out, NewOut, FStream>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    FStream: FnMut(Out) -> VirtaStream<NewOut> + Send + 'static + Clone + Unpin, // Use aliased VirtaStream
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.try_process_next_outer_item(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        // Ensure final End message is sent if the actor is stopped prematurely.
        if !(self.outer_stream_ended
            && self.outer_stream_buffer.is_empty()
            && !self.is_inner_stream_active)
        {
            let _ = self.main_downstream.try_send(StreamMessage::End);
        }
    }
}

impl<Out, NewOut, FStream> Handler<StreamMessage<Out>> for ConcatMapActor<Out, NewOut, FStream>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    FStream: FnMut(Out) -> VirtaStream<NewOut> + Send + 'static + Clone + Unpin, // Use aliased VirtaStream
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<Out>, ctx: &mut Context<Self>) {
        match msg {
            StreamMessage::Element(item) => {
                self.outer_stream_buffer.push_back(item);
                self.try_process_next_outer_item(ctx);
            }
            StreamMessage::End => {
                self.outer_stream_ended = true;
                if !self.is_inner_stream_active && self.outer_stream_buffer.is_empty() {
                    let _ = self.main_downstream.try_send(StreamMessage::End);
                    ctx.stop();
                }
            }
        }
    }
}

impl<Out, NewOut, FStream> Handler<InnerStreamItemMsg<NewOut>>
    for ConcatMapActor<Out, NewOut, FStream>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    FStream: FnMut(Out) -> VirtaStream<NewOut> + Send + 'static + Clone + Unpin, // Use aliased VirtaStream
{
    type Result = ();

    fn handle(&mut self, msg: InnerStreamItemMsg<NewOut>, ctx: &mut Context<Self>) {
        // msg.item is pub(crate) in InnerStreamItemMsg from inner_stream_proxy_actor.rs
        if self
            .main_downstream
            .try_send(StreamMessage::Element(msg.item))
            .is_err()
        {
            // Downstream recipient is gone, stop the actor.
            ctx.stop();
        }
    }
}

impl<Out, NewOut, FStream> Handler<InnerStreamCompletedMsg> for ConcatMapActor<Out, NewOut, FStream>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    FStream: FnMut(Out) -> VirtaStream<NewOut> + Send + 'static + Clone + Unpin, // Use aliased VirtaStream
{
    type Result = ();

    fn handle(&mut self, _msg: InnerStreamCompletedMsg, ctx: &mut Context<Self>) {
        self.is_inner_stream_active = false;
        self.try_process_next_outer_item(ctx); // Try to process next outer item or finalize.
    }
}
