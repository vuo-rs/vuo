use crate::stream::{StreamMessage, Streamable, CloneableStreamable};
use actix::prelude::*; // Changed to prelude
use std::marker::PhantomData; // Added for PhantomData

#[derive(Debug)]
pub(crate) struct ScanActor<Out, Acc, FScan>
where
    Out: Streamable,
    Acc: CloneableStreamable,
    FScan: FnMut(Acc, Out) -> Acc + Send + 'static + Unpin,
{
    current_accumulator: Acc,
    scan_fn: FScan,
    downstream: Recipient<StreamMessage<Acc>>,
    initial_emitted: bool,
    _phantom_out: PhantomData<Out>, // Added PhantomData for Out
}

impl<Out, Acc, FScan> ScanActor<Out, Acc, FScan>
where
    Out: Streamable,
    Acc: CloneableStreamable,
    FScan: FnMut(Acc, Out) -> Acc + Send + 'static + Unpin,
{
    pub(crate) fn new(initial: Acc, scan_fn: FScan, downstream: Recipient<StreamMessage<Acc>>) -> Self {
        ScanActor {
            current_accumulator: initial,
            scan_fn,
            downstream,
            initial_emitted: false,
            _phantom_out: PhantomData, // Initialize PhantomData
        }
    }

    fn emit_current_accumulator(&mut self, ctx: &mut Context<Self>) -> bool {
        if self.downstream.try_send(StreamMessage::Element(self.current_accumulator.clone())).is_err() {
            if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                ctx.stop();
            }
            false
        } else {
            true
        }
    }
}

impl<Out, Acc, FScan> Actor for ScanActor<Out, Acc, FScan>
where
    Out: Streamable,
    Acc: CloneableStreamable,
    FScan: FnMut(Acc, Out) -> Acc + Send + 'static + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        if !self.initial_emitted {
            if self.emit_current_accumulator(ctx) {
                self.initial_emitted = true;
            }
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        let _ = self.downstream.try_send(StreamMessage::End);
    }
}

impl<Out, Acc, FScan> Handler<StreamMessage<Out>> for ScanActor<Out, Acc, FScan>
where
    Out: Streamable,
    Acc: CloneableStreamable,
    FScan: FnMut(Acc, Out) -> Acc + Send + 'static + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<Out>, ctx: &mut Context<Self>) {
        if ctx.state() == ActorState::Stopping || ctx.state() == ActorState::Stopped {
            return;
        }

        if !self.initial_emitted {
            if !self.emit_current_accumulator(ctx) {
                return; 
            }
            self.initial_emitted = true;
        }

        match msg {
            StreamMessage::Element(out_item) => {
                let next_acc = (self.scan_fn)(self.current_accumulator.clone(), out_item);
                self.current_accumulator = next_acc;
                self.emit_current_accumulator(ctx);
            }
            StreamMessage::End => {
                if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop();
                }
            }
        }
    }
}