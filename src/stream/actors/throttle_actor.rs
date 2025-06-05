use crate::stream::{CloneableStreamable, StreamMessage};
use actix::prelude::*;
use std::fmt::Debug;
use std::time::Duration;

// Internal message to signal the end of a throttle period
#[derive(Message, Debug)]
#[rtype(result = "()")]
struct ResetThrottle;

#[derive(Debug)]
pub(crate) struct ThrottleActor<T>
where
    T: CloneableStreamable,
{
    duration: Duration,
    downstream: Recipient<StreamMessage<T>>,
    is_throttling: bool,
    timer_handle: Option<SpawnHandle>,
    upstream_ended: bool,
    downstream_signaled_end: bool, // To prevent sending duplicate End messages
}

impl<T> ThrottleActor<T>
where
    T: CloneableStreamable,
{
    pub(crate) fn new(duration: Duration, downstream: Recipient<StreamMessage<T>>) -> Self {
        ThrottleActor {
            duration,
            downstream,
            is_throttling: false, // Start in a ready state
            timer_handle: None,
            upstream_ended: false,
            downstream_signaled_end: false,
        }
    }

    fn clear_timer(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.timer_handle.take() {
            ctx.cancel_future(handle);
        }
    }

    // Safely sends End to downstream and marks it.
    fn try_send_end_to_downstream(&mut self, ctx: &mut Context<Self>) {
        if !self.downstream_signaled_end {
            let _ = self.downstream.try_send(StreamMessage::End);
            self.downstream_signaled_end = true;
        }
        // Ensure actor stops if not already stopping.
        if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
            ctx.stop();
        }
    }
}

impl<T> Actor for ThrottleActor<T>
where
    T: CloneableStreamable,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("ThrottleActor started with duration: {:?}", self.duration);
    }

    fn stopping(&mut self, ctx: &mut Context<Self>) -> Running {
        // log::debug!("ThrottleActor stopping.");
        self.clear_timer(ctx);
        // Ensure End is sent if not already.
        // This handles cases where the actor is stopped externally or due to downstream failure.
        if !self.downstream_signaled_end {
            let _ = self.downstream.try_send(StreamMessage::End);
            self.downstream_signaled_end = true; 
        }
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("ThrottleActor stopped.");
    }
}

impl<T> Handler<StreamMessage<T>> for ThrottleActor<T>
where
    T: CloneableStreamable,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<T>, ctx: &mut Context<Self>) {
        if self.upstream_ended && matches!(msg, StreamMessage::End) {
            // Already processed End, actor should be stopping or stopped.
            self.try_send_end_to_downstream(ctx); // Ensure final End if somehow missed
            return;
        }
        if ctx.state() == ActorState::Stopping || ctx.state() == ActorState::Stopped {
            return; // Actor is already stopping
        }

        match msg {
            StreamMessage::Element(item) => {
                if self.is_throttling {
                    // log::trace!("ThrottleActor: Throttling. Ignoring item: {:?}", item);
                    // Item is ignored
                } else {
                    // Not throttling, emit this item
                    // log::debug!("ThrottleActor: Emitting item: {:?}", item);
                    if self.downstream.try_send(StreamMessage::Element(item)).is_err() {
                        // Downstream closed, stop everything.
                        // log::warn!("ThrottleActor: Downstream recipient closed. Stopping.");
                        self.try_send_end_to_downstream(ctx); // Will also stop ctx
                        return;
                    }
                    // Start throttling period
                    self.is_throttling = true;
                    self.clear_timer(ctx); // Clear any previous (shouldn't be one if not throttling)
                    let handle = ctx.run_later(self.duration, |_, inner_ctx| {
                        inner_ctx.address().do_send(ResetThrottle);
                    });
                    self.timer_handle = Some(handle);
                }
            }
            StreamMessage::End => {
                // log::debug!("ThrottleActor: Received End from upstream.");
                self.upstream_ended = true;
                if !self.is_throttling {
                    // If not currently throttling, means we are ready for a new item,
                    // but upstream ended. So, end downstream immediately.
                    // log::debug!("ThrottleActor: Upstream ended and not throttling. Ending downstream.");
                    self.try_send_end_to_downstream(ctx);
                }
                // If currently throttling, the active timer will eventually fire ResetThrottle.
                // The ResetThrottle handler will then check upstream_ended and stop.
                // No need to clear timer here; let it complete to reset throttling state correctly
                // before potentially ending the stream.
            }
        }
    }
}

// Handler for the internal ResetThrottle message (timer fired)
impl<T> Handler<ResetThrottle> for ThrottleActor<T>
where
    T: CloneableStreamable,
{
    type Result = ();

    fn handle(&mut self, _msg: ResetThrottle, ctx: &mut Context<Self>) {
        // log::debug!("ThrottleActor: Throttle period ended (ResetThrottle received).");
        self.timer_handle = None;
        self.is_throttling = false;

        if self.upstream_ended {
            // Upstream had ended while we were throttling. Now that throttling period is over,
            // we can signal End to downstream.
            // log::debug!("ThrottleActor: Upstream already ended. Ending downstream now.");
            self.try_send_end_to_downstream(ctx);
        }
        // Else, actor is now ready to emit the next available item from upstream.
    }
}