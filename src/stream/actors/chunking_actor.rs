use crate::stream::{StreamMessage, CloneableStreamable};
use actix::prelude::*; 

#[derive(Debug)] 
pub(crate) struct ChunkingActor<T>
where
    T: CloneableStreamable, 
{
    chunk_size: usize,
    buffer: Vec<T>,
    downstream: Recipient<StreamMessage<Vec<T>>>,
}

impl<T> ChunkingActor<T>
where
    T: CloneableStreamable,
{
    pub(crate) fn new(chunk_size: usize, downstream: Recipient<StreamMessage<Vec<T>>>) -> Self {
        let actual_chunk_size = if chunk_size == 0 { 1 } else { chunk_size }; 
        ChunkingActor {
            chunk_size: actual_chunk_size,
            buffer: Vec::with_capacity(actual_chunk_size),
            downstream,
        }
    }

    fn send_buffer(&mut self, ctx: &mut Context<Self>) {
        if !self.buffer.is_empty() {
            let chunk_to_send = self.buffer.clone(); 
            self.buffer.clear(); 
            if self.downstream.try_send(StreamMessage::Element(chunk_to_send)).is_err() {
                // If send fails, check if actor is already stopping/stopped to avoid recursive stop calls.
                if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop(); 
                }
            }
        }
    }
}

impl<T> Actor for ChunkingActor<T>
where
    T: CloneableStreamable,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("ChunkingActor started with chunk size: {}", self.chunk_size);
    }

    fn stopped(&mut self, ctx: &mut Context<Self>) {
        // log::debug!("ChunkingActor stopped. Attempting to send any remaining buffer and End message.");
        // Before fully stopping, send any remaining items in the buffer as a final chunk.
        // Check if not already stopping (e.g., from send_buffer failing).
        if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped { 
            self.send_buffer(ctx); 
        }
        // Ensure End message is sent to downstream, if the context isn't already stopped/stopping.
        // If send_buffer caused a stop, state would be Stopping/Stopped.
        if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
             let _ = self.downstream.try_send(StreamMessage::End);
        } else {
            // If already stopping/stopped (e.g. due to send_buffer failing and calling ctx.stop()),
            // a final attempt to send End might still be reasonable if the previous failure
            // was specific to an Element and End might still go through, or if it is to ensure
            // the End signal is robustly sent. However, if the channel is truly closed, this will also fail.
            // For simplicity, if it's already stopping, we assume the reason for stopping
            // will lead to this `stopped` method eventually and one `try_send(End)` is enough.
            // A more elaborate logic could track if End was successfully sent.
            // The current logic in `stopped` (from original code) sends End, which is fine.
            // This additional check here is more about the explicit send_buffer call within stopped.
        }
    }
}

impl<T> Handler<StreamMessage<T>> for ChunkingActor<T>
where
    T: CloneableStreamable,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<T>, ctx: &mut Context<Self>) {
        // If actor is already stopping/stopped, ignore new messages.
        if ctx.state() == ActorState::Stopping || ctx.state() == ActorState::Stopped {
            return;
        }

        match msg {
            StreamMessage::Element(elem) => {
                self.buffer.push(elem); 
                if self.buffer.len() >= self.chunk_size {
                    self.send_buffer(ctx);
                }
            }
            StreamMessage::End => {
                // Check if not already stopping due to a failed send in send_buffer
                if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    self.send_buffer(ctx);
                }
                // After sending the last partial chunk (if any), stop the actor.
                // The stopped() method will then send the final StreamMessage::End if this actor didn't initiate stop from send_buffer.
                if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                    ctx.stop(); 
                }
            }
        }
    }
}