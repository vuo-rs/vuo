use crate::stream::{CloneableStreamable, StreamMessage};
use actix::prelude::*;
use std::time::Duration;
use std::fmt::Debug; // For #[derive(Debug)]

// Message to trigger the emission of a debounced item
#[derive(Message, Debug)]
#[rtype(result = "()")]
struct EmitDebouncedItem;

#[derive(Debug)]
pub(crate) struct DebounceActor<T>
where
    T: CloneableStreamable,
{
    duration: Duration,
    downstream: Recipient<StreamMessage<T>>,
    last_item: Option<T>,
    timer_handle: Option<SpawnHandle>, // Handle to the active timer
    upstream_ended: bool,
}

impl<T> DebounceActor<T>
where
    T: CloneableStreamable,
{
    pub(crate) fn new(duration: Duration, downstream: Recipient<StreamMessage<T>>) -> Self {
        DebounceActor {
            duration,
            downstream,
            last_item: None,
            timer_handle: None,
            upstream_ended: false,
        }
    }

    fn clear_timer(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.timer_handle.take() {
            ctx.cancel_future(handle);
        }
    }

    fn schedule_emission(&mut self, ctx: &mut Context<Self>) {
        self.clear_timer(ctx); // Cancel any existing timer

        // Schedule EmitDebouncedItem message to be sent to self after duration
        let handle = ctx.run_later(self.duration, |_actor, inner_ctx| {
            // Check if upstream has ended and if this is the absolute last item to send.
            // The main emission logic is in Handler<EmitDebouncedItem>.
            // This timer simply triggers that handler.
            // If the actor is stopped before the timer fires, this future is cancelled.
            inner_ctx.address().do_send(EmitDebouncedItem);
        });
        self.timer_handle = Some(handle);
    }
}

impl<T> Actor for DebounceActor<T>
where
    T: CloneableStreamable,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // log::debug!("DebounceActor started with duration: {:?}", self.duration);
    }

    fn stopping(&mut self, ctx: &mut Context<Self>) -> Running {
        // log::debug!("DebounceActor stopping. Clearing timer.");
        self.clear_timer(ctx);
        // If there's a pending item and upstream hasn't ended yet or just ended,
        // it should have been flushed by StreamMessage::End handler or the last timer.
        // If stopping for other reasons (e.g. downstream closed), just ensure End is sent.
        if !self.upstream_ended { // If upstream didn't end, it means we are stopping prematurely
            let _ = self.downstream.try_send(StreamMessage::End);
        }
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        // log::debug!("DebounceActor stopped.");
    }
}

impl<T> Handler<StreamMessage<T>> for DebounceActor<T>
where
    T: CloneableStreamable,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<T>, ctx: &mut Context<Self>) {
        if self.upstream_ended { // If upstream already ended, ignore further elements
            if matches!(msg, StreamMessage::End) { // But if it's another End, ensure actor stops
                 if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop();
                 }
            }
            return;
        }

        match msg {
            StreamMessage::Element(item) => {
                // log::trace!("DebounceActor received element: {:?}. Scheduling emission.", item);
                self.last_item = Some(item);
                self.schedule_emission(ctx);
            }
            StreamMessage::End => {
                // log::debug!("DebounceActor received End from upstream.");
                self.upstream_ended = true;
                self.clear_timer(ctx); // Cancel any pending emission timer for an older item.

                // If there was a last_item pending when upstream ended, emit it immediately.
                if let Some(item_to_flush) = self.last_item.take() {
                    // log::debug!("Flushing last item due to upstream End: {:?}", item_to_flush);
                    if self.downstream.try_send(StreamMessage::Element(item_to_flush)).is_err() {
                        // Downstream closed, nothing more to do. Stop will send final End.
                         if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                            ctx.stop();
                         }
                        return;
                    }
                }
                // Whether an item was flushed or not, upstream has ended, so send End downstream.
                let _ = self.downstream.try_send(StreamMessage::End);
                if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop();
                }
            }
        }
    }
}

// Handler for the internal EmitDebouncedItem message (timer fired)
impl<T> Handler<EmitDebouncedItem> for DebounceActor<T>
where
    T: CloneableStreamable,
{
    type Result = ();

    fn handle(&mut self, _msg: EmitDebouncedItem, ctx: &mut Context<Self>) {
        self.timer_handle = None; // Timer has fired

        if self.upstream_ended {
            // If upstream has already ended, the StreamMessage::End handler should have
            // flushed the last item and sent End. This timer firing is likely for an item
            // that was superseded by the End signal. Or it's a late timer after flush.
            // Actor should be stopping or stopped.
            // log::trace!("EmitDebouncedItem: Upstream already ended. Actor should be stopping.");
            return;
        }

        if let Some(item_to_emit) = self.last_item.take() {
            // log::debug!("Debounce timer fired. Emitting item: {:?}", item_to_emit);
            if self.downstream.try_send(StreamMessage::Element(item_to_emit)).is_err() {
                // Downstream closed. Stop the actor.
                // log::warn!("DebounceActor: Downstream recipient closed during debounced emit. Stopping.");
                self.upstream_ended = true; // Effectively, we can't process more.
                 if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop();
                 }
            }
            // After emitting, if upstream has not ended, the actor waits for new items or End.
        } else {
            // log::trace!("EmitDebouncedItem: Timer fired, but no last_item to emit (possibly cleared by End or another item).");
        }
    }
}