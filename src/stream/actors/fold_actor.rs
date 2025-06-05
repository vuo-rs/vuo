use crate::stream::{StreamMessage, Streamable, CloneableStreamable};
use actix::prelude::*;
use std::marker::PhantomData;

#[derive(Debug)] // Added Debug for the actor struct itself
pub(crate) struct FoldActor<Out, Acc, F>
where
    Out: Streamable, // Input item type
    Acc: CloneableStreamable, // Accumulator type, must be Cloneable
    F: FnMut(Acc, Out) -> Acc + Send + 'static + Unpin, // Fold function
{
    accumulator: Option<Acc>, // Holds the current accumulated value
    initial_value: Acc,       // Initial value for the accumulator
    fold_fn: F,               // The folding function
    downstream: Recipient<StreamMessage<Acc>>, // Where to send the final result
    _phantom_out: PhantomData<Out>, // To use the Out type parameter
}

impl<Out, Acc, F> FoldActor<Out, Acc, F>
where
    Out: Streamable,
    Acc: CloneableStreamable,
    F: FnMut(Acc, Out) -> Acc + Send + 'static + Unpin,
{
    pub(crate) fn new(initial: Acc, fold_fn: F, downstream: Recipient<StreamMessage<Acc>>) -> Self {
        FoldActor {
            accumulator: None, // Accumulator starts empty, initialized with initial_value on first item
            initial_value: initial,
            fold_fn,
            downstream,
            _phantom_out: PhantomData, // Initialize PhantomData
        }
    }
}

impl<Out, Acc, F> Actor for FoldActor<Out, Acc, F>
where
    Out: Streamable,
    Acc: CloneableStreamable,
    F: FnMut(Acc, Out) -> Acc + Send + 'static + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("FoldActor started with initial value: {:?}", self.initial_value);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("FoldActor stopped. Ensuring End is sent to downstream.");
        // If the actor is stopped for any reason before sending the final accumulated value
        // (e.g., downstream closed, panic), ensure downstream gets an End signal.
        // Note: If the stream completes normally (End message handled), this might be redundant
        // but ensures graceful termination in other scenarios.
        // Consider if the accumulator should be sent here if it exists and hasn't been sent.
        // For fold, typically only one value is sent upon completion of the input stream.
        // If it stops prematurely, sending a partial fold might be misleading.
        // So, just sending End is a safe default.
        let _ = self.downstream.try_send(StreamMessage::End);
    }
}

impl<Out, Acc, F> Handler<StreamMessage<Out>> for FoldActor<Out, Acc, F>
where
    Out: Streamable,
    Acc: CloneableStreamable,
    F: FnMut(Acc, Out) -> Acc + Send + 'static + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<Out>, ctx: &mut Context<Self>) {
        // If actor is already stopping/stopped, ignore new messages.
        if ctx.state() == ActorState::Stopping || ctx.state() == ActorState::Stopped {
            return;
        }

        match msg {
            StreamMessage::Element(out_item) => {
                // log::trace!("FoldActor received element: {:?}. Current accumulator: {:?}", out_item, self.accumulator);
                // Take the current accumulator or use initial_value if it's the first element.
                let current_acc = self
                    .accumulator
                    .take()
                    .unwrap_or_else(|| self.initial_value.clone());
                // Apply the fold function.
                self.accumulator = Some((self.fold_fn)(current_acc, out_item));
                // log::trace!("FoldActor new accumulator: {:?}", self.accumulator);
            }
            StreamMessage::End => {
                // log::debug!("FoldActor received End from upstream.");
                // Upstream ended. Take the final accumulated value (or initial if no elements were received).
                let final_value = self
                    .accumulator
                    .take()
                    .unwrap_or_else(|| self.initial_value.clone());

                // log::debug!("FoldActor sending final accumulated value: {:?}", final_value);
                // Send the final value.
                if self.downstream.try_send(StreamMessage::Element(final_value)).is_ok() {
                    // If final value sent successfully, also send End for this stream.
                    let _ = self.downstream.try_send(StreamMessage::End);
                } else {
                    // log::warn!("FoldActor: Downstream recipient closed before final value could be sent.");
                    // Downstream is closed. The stopped() method will attempt to send End.
                }
                // Stop the actor as its work is done.
                if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop();
                }
            }
        }
    }
}