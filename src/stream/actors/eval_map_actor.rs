use crate::stream::{CloneableStreamable, StreamMessage, Streamable};
use actix::prelude::*;
use futures::Future;
use std::collections::VecDeque;

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub(crate) struct EvalMapCompleted<NewOut: CloneableStreamable> {
    result: NewOut,
}

#[derive(Debug)]
pub(crate) struct EvalMapActor<Out, NewOut, Fut, F>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    Fut: Future<Output = NewOut> + Send + 'static,
    F: FnMut(Out) -> Fut + Send + 'static + Unpin + Clone,
{
    actor_id: usize, // For logging
    map_fn: F,
    downstream: Recipient<StreamMessage<NewOut>>,
    input_buffer: VecDeque<Out>,
    is_processing: bool,
    upstream_ended: bool,
    self_addr: Option<Addr<Self>>, // To send EvalMapCompleted to self
}

impl<Out, NewOut, Fut, F> EvalMapActor<Out, NewOut, Fut, F>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    Fut: Future<Output = NewOut> + Send + 'static,
    F: FnMut(Out) -> Fut + Send + 'static + Unpin + Clone,
{
    pub(crate) fn new(map_fn: F, downstream: Recipient<StreamMessage<NewOut>>) -> Self {
        static ACTOR_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let actor_id = ACTOR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log::debug!("[EvalMapActor-{}] Creating new instance", actor_id);
        Self {
            actor_id,
            map_fn,
            downstream,
            input_buffer: VecDeque::new(),
            is_processing: false,
            upstream_ended: false,
            self_addr: None, // Will be set in started()
        }
    }

    fn process_next(&mut self, ctx: &mut Context<Self>) {
        log::trace!(
            "[EvalMapActor-{}] process_next: is_processing: {}, input_buffer_empty: {}, upstream_ended: {}",
            self.actor_id,
            self.is_processing,
            self.input_buffer.is_empty(),
            self.upstream_ended
        );

        if self.is_processing {
            log::trace!("[EvalMapActor-{}] process_next: Already processing, returning.", self.actor_id);
            return;
        }

        if self.input_buffer.is_empty() {
            if self.upstream_ended {
                log::debug!(
                    "[EvalMapActor-{}] process_next: Upstream ended and input buffer empty. Signaling End and stopping.",
                    self.actor_id
                );
                let _ = self.downstream.try_send(StreamMessage::End);
                ctx.stop();
            } else {
                log::trace!("[EvalMapActor-{}] process_next: Input buffer empty, but upstream not ended. Waiting for more items.", self.actor_id);
            }
            return;
        }

        // At this point, not processing and input_buffer is not empty.
        if let Some(item) = self.input_buffer.pop_front() {
            log::debug!("[EvalMapActor-{}] process_next: Popped item {:?} from input_buffer. Spawning future with actix_rt::spawn.", self.actor_id, item);
            self.is_processing = true;
            
            let mut map_fn_clone = self.map_fn.clone(); // Clone map_fn for the spawned task
            let future_to_run = (map_fn_clone)(item);
            
            if let Some(addr) = self.self_addr.as_ref().cloned() {
                actix_rt::spawn(async move {
                    let actual_item = future_to_run.await;
                    // log::trace! is not easily accessible here without actor_id, but the message handler will log.
                    let completed_msg = EvalMapCompleted { result: actual_item };
                    addr.do_send(completed_msg);
                });
            } else {
                // This should not happen if actor is properly started
                log::error!("[EvalMapActor-{}] process_next: self_addr is None. Cannot spawn task.", self.actor_id);
                // Potentially stop or handle error, for now, it might stall.
                // To be safe, reset is_processing if we can't spawn.
                self.is_processing = false; 
            }
        } else {
             // This case should ideally not be reached if self.input_buffer.is_empty() check is done correctly.
            log::warn!("[EvalMapActor-{}] process_next: Input buffer was not empty, but pop_front returned None. This is unexpected.", self.actor_id);
            // Check for completion again in case state is inconsistent
            if self.upstream_ended && !self.is_processing {
                 log::debug!("[EvalMapActor-{}] process_next: (unexpected pop_front None) Upstream ended. Signaling End and stopping.", self.actor_id);
                let _ = self.downstream.try_send(StreamMessage::End);
                ctx.stop();
            }
        }
    }
}

impl<Out, NewOut, Fut, F> Actor for EvalMapActor<Out, NewOut, Fut, F>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    Fut: Future<Output = NewOut> + Send + 'static,
    F: FnMut(Out) -> Fut + Send + 'static + Unpin + Clone,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        log::debug!("[EvalMapActor-{}] Actor started.", self.actor_id);
        self.self_addr = Some(ctx.address()); // Store own address
        self.process_next(ctx);
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        log::debug!("[EvalMapActor-{}] Actor stopped. upstream_ended: {}, input_buffer_empty: {}, is_processing: {}",
            self.actor_id, self.upstream_ended, self.input_buffer.is_empty(), self.is_processing);
        // Ensure End is sent if not all conditions for normal completion in process_next were met.
        // This can happen if the actor is stopped externally or due to an error.
        if !(self.upstream_ended && self.input_buffer.is_empty() && !self.is_processing) {
            log::warn!("[EvalMapActor-{}] Actor stopped abruptly or not fully processed. Sending End downstream.", self.actor_id);
            let _ = self.downstream.try_send(StreamMessage::End);
        }
    }
}

impl<Out, NewOut, Fut, F> Handler<StreamMessage<Out>> for EvalMapActor<Out, NewOut, Fut, F>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    Fut: Future<Output = NewOut> + Send + 'static,
    F: FnMut(Out) -> Fut + Send + 'static + Unpin + Clone,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<Out>, ctx: &mut Context<Self>) {
        log::trace!("[EvalMapActor-{}] Handling StreamMessage: {:?}", self.actor_id, msg);
        match msg {
            StreamMessage::Element(item) => {
                self.input_buffer.push_back(item);
                log::debug!("[EvalMapActor-{}] Received Element, added to input_buffer. New size: {}.", self.actor_id, self.input_buffer.len());
                self.process_next(ctx);
            }
            StreamMessage::End => {
                log::debug!("[EvalMapActor-{}] Received End from upstream.", self.actor_id);
                self.upstream_ended = true;
                // Call process_next to potentially trigger completion if buffer is empty and not processing.
                self.process_next(ctx);
            }
        }
    }
}

impl<Out, NewOut, Fut, F> Handler<EvalMapCompleted<NewOut>> for EvalMapActor<Out, NewOut, Fut, F>
where
    Out: Streamable,
    NewOut: CloneableStreamable,
    Fut: Future<Output = NewOut> + Send + 'static,
    F: FnMut(Out) -> Fut + Send + 'static + Unpin + Clone,
{
    type Result = ();

    fn handle(&mut self, msg: EvalMapCompleted<NewOut>, ctx: &mut Context<Self>) {
        log::debug!("[EvalMapActor-{}] Handling EvalMapCompleted with result: {:?}", self.actor_id, msg.result);
        self.is_processing = false;
        log::trace!("[EvalMapActor-{}] Set is_processing to false.", self.actor_id);

        if self.downstream.try_send(StreamMessage::Element(msg.result)).is_err() {
            log::warn!("[EvalMapActor-{}] Downstream recipient closed while sending element. Stopping.", self.actor_id);
            ctx.stop();
            return;
        }
        log::trace!("[EvalMapActor-{}] Sent element downstream successfully.", self.actor_id);
        self.process_next(ctx);
    }
}

