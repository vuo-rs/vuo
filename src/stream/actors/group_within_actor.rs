use crate::stream::{CloneableStreamable, StreamMessage};
use actix::prelude::*;
use std::fmt::Debug;
use std::time::Duration;
use std::mem; // Single import for mem::take

// Message to trigger buffer emission due to timeout
#[derive(Message, Debug)]
#[rtype(result = "()")]
struct EmitBufferTimeout;

#[derive(Debug)]
pub(crate) struct GroupWithinActor<T>
where
    T: CloneableStreamable,
{
    max_chunk_size: usize,
    max_duration: Duration,
    downstream: Recipient<StreamMessage<Vec<T>>>,
    current_buffer: Vec<T>,
    timer_handle: Option<SpawnHandle>,
    upstream_ended: bool,
}

impl<T> GroupWithinActor<T>
where
    T: CloneableStreamable,
{
    pub(crate) fn new(
        max_chunk_size: usize,
        max_duration: Duration,
        downstream: Recipient<StreamMessage<Vec<T>>>,
    ) -> Self {
        let actual_chunk_size = if max_chunk_size == 0 {
            1 // Chunk size must be at least 1
        } else {
            max_chunk_size
        };
        GroupWithinActor {
            max_chunk_size: actual_chunk_size,
            max_duration,
            downstream,
            current_buffer: Vec::with_capacity(actual_chunk_size),
            timer_handle: None,
            upstream_ended: false,
        }
    }

    fn clear_timer(&mut self, ctx: &mut Context<Self>) {
        if let Some(handle) = self.timer_handle.take() {
            ctx.cancel_future(handle);
        }
    }

    fn schedule_timer(&mut self, ctx: &mut Context<Self>) {
        self.clear_timer(ctx); // Always clear previous timer before scheduling a new one
        if !self.current_buffer.is_empty() {
            // Only schedule if buffer has items
            let handle = ctx.run_later(self.max_duration, |_, inner_ctx| {
                inner_ctx.address().do_send(EmitBufferTimeout);
            });
            self.timer_handle = Some(handle);
        }
    }

    fn emit_buffer(&mut self, ctx: &mut Context<Self>) {
        self.clear_timer(ctx); // Buffer is being emitted, so timer is no longer needed for this chunk

        if !self.current_buffer.is_empty() {
            let chunk_to_send = mem::take(&mut self.current_buffer); // Use mem::take
            // Re-initialize buffer with capacity for next potential chunk
            self.current_buffer = Vec::with_capacity(self.max_chunk_size); 

            if self
                .downstream
                .try_send(StreamMessage::Element(chunk_to_send))
                .is_err()
            {
                // log::warn!("GroupWithinActor: Downstream recipient closed. Stopping.");
                if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop();
                }
            }
        }
        // After emitting (or if buffer was empty), if upstream hasn't ended,
        // and we are not stopping, we wait for new items.
        // A new timer will be scheduled when a new item arrives (if buffer becomes non-empty).
    }
}

impl<T> Actor for GroupWithinActor<T>
where
    T: CloneableStreamable,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("GroupWithinActor started. Max size: {}, Max duration: {:?}\", self.max_chunk_size, self.max_duration);
    }

    fn stopping(&mut self, ctx: &mut Context<Self>) -> Running {
        // log::debug!("GroupWithinActor stopping.");
        self.clear_timer(ctx);
        
        // Only flush the buffer if the upstream has definitively ended.
        // If the actor is stopping for other reasons (e.g., downstream closed),
        // flushing a partial, non-timed-out, non-size-complete buffer might be undesirable.
        if self.upstream_ended {
            let chunk_to_send = mem::take(&mut self.current_buffer);
            if !chunk_to_send.is_empty() {
                // log::debug!("GroupWithinActor: Flushing remaining buffer of size {} on stop because upstream ended.", chunk_to_send.len());
                let _ = self.downstream.try_send(StreamMessage::Element(chunk_to_send));
            }
        }
        // Always attempt to send End message to downstream when this actor stops.
        let _ = self.downstream.try_send(StreamMessage::End);
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("GroupWithinActor stopped.");
    }
}

impl<T> Handler<StreamMessage<T>> for GroupWithinActor<T>
where
    T: CloneableStreamable,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<T>, ctx: &mut Context<Self>) {
        if self.upstream_ended && matches!(msg, StreamMessage::End) {
            // Already processed End, actor should be stopping or stopped.
            // This handles redundant End messages.
            if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                ctx.stop();
            }
            return;
        }
        if ctx.state() == ActorState::Stopping || ctx.state() == ActorState::Stopped {
            return; // Actor is already stopping, ignore further messages
        }

        match msg {
            StreamMessage::Element(item) => {
                // log::trace!("GroupWithinActor received element: {:?}", item);
                let buffer_was_empty = self.current_buffer.is_empty();
                self.current_buffer.push(item);

                if buffer_was_empty && !self.upstream_ended {
                    // First item in a new chunk, start the timer.
                    self.schedule_timer(ctx);
                }
                // Note: If a new item arrives while a timer is active for the current chunk,
                // this logic does not reset the timer. This is intentional for "timeout since first item".

                if self.current_buffer.len() >= self.max_chunk_size {
                    // log::debug!("GroupWithinActor: Chunk size reached. Emitting buffer.");
                    self.emit_buffer(ctx); // Emits and clears timer
                    // After emitting due to size, if upstream has not ended,
                    // the next item arriving into the now-empty buffer will start a new timer.
                }
            }
            StreamMessage::End => {
                // log::debug!("GroupWithinActor received End from upstream.");
                self.upstream_ended = true;
                self.emit_buffer(ctx); // Emit any remaining items in the buffer.
                                      // emit_buffer also clears any active timer.
                
                // Now that upstream has ended and final buffer flushed, stop the actor.
                // The `stopped` method (called during ctx.stop()) will send the final StreamMessage::End to downstream.
                if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop();
                }
            }
        }
    }
}

// Handler for the internal EmitBufferTimeout message (timer fired)
impl<T> Handler<EmitBufferTimeout> for GroupWithinActor<T>
where
    T: CloneableStreamable,
{
    type Result = ();

    fn handle(&mut self, _msg: EmitBufferTimeout, ctx: &mut Context<Self>) {
        self.timer_handle = None; // Timer has fired

        if self.upstream_ended {
            // If upstream already ended, the StreamMessage::End handler should have
            // dealt with flushing and stopping. This timer is late/irrelevant.
            // log::trace!("GroupWithinActor: EmitBufferTimeout received but upstream already ended.");
            return;
        }

        // log::debug!("GroupWithinActor: Timeout reached. Emitting buffer: {:?}", self.current_buffer);
        self.emit_buffer(ctx); // Emits current buffer (if any) and clears it.
                               // Also clears self.timer_handle implicitly via clear_timer call.

        // After emitting due to timeout, if upstream has not ended,
        // the actor waits for new items. The next item arriving into the
        // now-empty buffer will start a new timer (via Element handler).
    }
}