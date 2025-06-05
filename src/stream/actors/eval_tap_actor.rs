use crate::stream::{CloneableStreamable, StreamMessage};
use actix::prelude::*;
use std::future::Future;
use std::marker::PhantomData;

/// Actor responsible for the `eval_tap` stream operation.
///
/// For each element in the stream, this actor executes a provided side-effecting future
/// (`effect_fn`). After the future completes, the original element is forwarded
/// to the downstream recipient. The processing is sequential: the effect for an element
/// must complete before that element is passed downstream, and before the effect for
/// the next element begins.
pub(crate) struct EvalTapActor<In, Fut, F>
where
    In: CloneableStreamable,
    Fut: Future<Output = ()> + 'static + Unpin, // Not necessarily Send for boxed_local futures, Unpin required for Actor
    F: FnMut(In) -> Fut + Clone + Unpin + 'static, // Cloned for setup, Unpin for potential across awaits
{
    downstream_recipient: Recipient<StreamMessage<In>>,
    effect_fn: F,
    _phantom_in: PhantomData<In>,
    _phantom_fut: PhantomData<Fut>,
}

impl<In, Fut, F> EvalTapActor<In, Fut, F>
where
    In: CloneableStreamable,
    Fut: Future<Output = ()> + 'static + Unpin,
    F: FnMut(In) -> Fut + Clone + Unpin + 'static,
{
    pub fn new(downstream_recipient: Recipient<StreamMessage<In>>, effect_fn: F) -> Self {
        Self {
            downstream_recipient,
            effect_fn,
            _phantom_in: PhantomData,
            _phantom_fut: PhantomData,
        }
    }
}

impl<In, Fut, F> Actor for EvalTapActor<In, Fut, F>
where
    In: CloneableStreamable + 'static, // 'static for actor messages
    Fut: Future<Output = ()> + 'static + Unpin,
    F: FnMut(In) -> Fut + Clone + Unpin + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // log::trace!("[EvalTapActor] Started");
    }

    fn stopping(&mut self, _ctx: &mut Context<Self>) -> Running {
        // log::trace!("[EvalTapActor] Stopping");
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        // log::trace!("[EvalTapActor] Stopped");
    }
}

impl<In, Fut, F> Handler<StreamMessage<In>> for EvalTapActor<In, Fut, F>
where
    In: CloneableStreamable + 'static,
    Fut: Future<Output = ()> + 'static + Unpin,
    F: FnMut(In) -> Fut + Clone + Unpin + 'static,
{
    type Result = ResponseActFuture<Self, ()>;

    fn handle(&mut self, msg: StreamMessage<In>, ctx: &mut Context<Self>) -> Self::Result {
        match msg {
            StreamMessage::Element(item) => {
                // Call FnMut: self.effect_fn is called before the async block.
                // The resulting future is then awaited within the async block.
                let effect_future = (self.effect_fn)(item.clone()); // Item is cloned for the effect function

                let recipient = self.downstream_recipient.clone();
                // The original 'item' (from the msg) is moved into the main async block
                // to be sent after the effect_future completes.
                async move {
                    effect_future.await; // Await the side-effecting future

                    // Try to send the original item downstream.
                    // If downstream is closed, try_send will return an error.
                    if recipient.try_send(StreamMessage::Element(item)).is_err() {
                        // log::debug!("[EvalTapActor] Downstream send failed.");
                        return Err(()); // Indicate failure to the .map block below
                    }
                    Ok(())
                }
                .into_actor(self) // Attaches the future to this actor's lifecycle and context
                .map(|result, _actor, actor_ctx| {
                    // This closure runs after the above async block completes.
                    if result.is_err() {
                        // log::debug!("[EvalTapActor] Downstream recipient closed or send failed. Stopping actor.");
                        actor_ctx.stop(); // Stop this EvalTapActor if sending failed
                    }
                })
                .boxed_local() // Required for ResponseActFuture as the future is not Send
            }
            StreamMessage::End => {
                // log::trace!("[EvalTapActor] Received End. Propagating and stopping.");
                self.downstream_recipient.do_send(StreamMessage::End);
                ctx.stop();
                // Return an empty future, required for ResponseActFuture
                async {}.into_actor(self).boxed_local()
            }
        }
    }
}