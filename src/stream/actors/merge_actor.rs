use crate::stream::{CloneableStreamable, StreamMessage};
use actix::prelude::*;
use std::fmt::Debug;

#[derive(Debug)]
pub(crate) struct Merge2Actor<T>
where
    T: CloneableStreamable,
{
    actor_id: usize, // For logging
    downstream: Recipient<StreamMessage<T>>,
    active_upstreams: usize,
    downstream_signaled_end: bool,
}

impl<T> Merge2Actor<T>
where
    T: CloneableStreamable,
{
    pub(crate) fn new(downstream: Recipient<StreamMessage<T>>) -> Self {
        static ACTOR_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let actor_id = ACTOR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log::debug!("[Merge2Actor-{}] Creating new instance.", actor_id);
        Merge2Actor {
            actor_id,
            downstream,
            active_upstreams: 2, // Designed for two input streams
            downstream_signaled_end: false,
        }
    }

    fn try_send_end_to_downstream_and_stop(&mut self, ctx: &mut Context<Self>) {
        if !self.downstream_signaled_end {
            log::debug!("[Merge2Actor-{}] try_send_end_to_downstream_and_stop: Signaling End to downstream.", self.actor_id);
            if self.downstream.try_send(StreamMessage::End).is_err() {
                log::warn!("[Merge2Actor-{}] try_send_end_to_downstream_and_stop: Failed to send End to downstream (already closed).", self.actor_id);
            }
            self.downstream_signaled_end = true;
        } else {
            log::trace!("[Merge2Actor-{}] try_send_end_to_downstream_and_stop: Downstream already signaled end.", self.actor_id);
        }
        if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
            log::debug!("[Merge2Actor-{}] try_send_end_to_downstream_and_stop: Stopping actor.", self.actor_id);
            ctx.stop();
        }
    }
}

impl<T> Actor for Merge2Actor<T>
where
    T: CloneableStreamable,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        log::debug!("[Merge2Actor-{}] Actor started. Expecting 2 upstreams. Active upstreams: {}", self.actor_id, self.active_upstreams);
        if self.active_upstreams == 0 { // Should not happen with current new()
            log::warn!("[Merge2Actor-{}] Actor started with 0 active upstreams. Stopping.", self.actor_id);
            self.try_send_end_to_downstream_and_stop(_ctx);
        }
    }

    fn stopping(&mut self, _ctx: &mut Context<Self>) -> Running {
        log::debug!("[Merge2Actor-{}] Actor stopping. Active upstreams: {}, Downstream signaled end: {}", self.actor_id, self.active_upstreams, self.downstream_signaled_end);
        // Ensure End is sent if not already.
        // try_send_end_to_downstream_and_stop will be called by Actix's stop sequence if not already.
        // However, to be sure, especially if the actor is stopped externally, we can call it.
        // In this case, the existing logic in `try_send_end_to_downstream_and_stop` handles idempotency.
        self.try_send_end_to_downstream_and_stop(_ctx);
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::debug!("[Merge2Actor-{}] Actor stopped.", self.actor_id);
    }
}

// Merge2Actor handles StreamMessage<T> coming from either of its upstreams.
impl<T> Handler<StreamMessage<T>> for Merge2Actor<T>
where
    T: CloneableStreamable,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<T>, ctx: &mut Context<Self>) {
        if self.downstream_signaled_end { // If already ended downstream, ignore further messages.
            log::trace!("[Merge2Actor-{}] Downstream already ended. Ignoring message: {:?}", self.actor_id, msg);
            return;
        }

        match msg {
            StreamMessage::Element(item) => {
                log::trace!("[Merge2Actor-{}] Received element: {:?}. Forwarding to downstream.", self.actor_id, item);
                if self.downstream.try_send(StreamMessage::Element(item.clone())).is_err() { // Added .clone() as item is used in log
                    // Downstream closed, stop everything.
                    log::warn!("[Merge2Actor-{}] Downstream recipient closed while forwarding element {:?}. Stopping.", self.actor_id, item);
                    self.try_send_end_to_downstream_and_stop(ctx);
                }
            }
            StreamMessage::End => {
                log::debug!("[Merge2Actor-{}] Received End from one upstream. Active upstreams before decrement: {}.", self.actor_id, self.active_upstreams);
                self.active_upstreams = self.active_upstreams.saturating_sub(1);
                log::debug!("[Merge2Actor-{}] Active upstreams after decrement: {}.", self.actor_id, self.active_upstreams);
                if self.active_upstreams == 0 {
                    log::debug!("[Merge2Actor-{}] Both upstreams ended. Signaling End downstream and stopping.", self.actor_id);
                    self.try_send_end_to_downstream_and_stop(ctx);
                }
                // If one stream ends but the other is active, Merge2Actor continues to forward
                // items from the remaining active stream.
            }
        }
    }
}