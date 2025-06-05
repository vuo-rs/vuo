use crate::stream::{CloneableStreamable, StreamMessage};
use actix::prelude::*;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::collections::VecDeque;
use std::sync::Arc;

// --- Messages to distinguish inputs for ZipActor ---

#[derive(Debug)]
pub struct InputStreamMessageA<A: CloneableStreamable>(pub StreamMessage<A>, PhantomData<fn() -> A>);
impl<A: CloneableStreamable> Message for InputStreamMessageA<A> {
    type Result = ();
}
// Clone is needed if ZipInputAdapterActor's recipient.try_send needs to clone the message.
// Actix messages usually need to be 'static + Send.
// If StreamMessage<A> is Clone, then this can be Clone.
impl<A: CloneableStreamable> Clone for InputStreamMessageA<A> where StreamMessage<A>: Clone {
    fn clone(&self) -> Self {
        InputStreamMessageA(self.0.clone(), PhantomData)
    }
}

#[derive(Debug)]
pub struct InputStreamMessageB<B: CloneableStreamable>(pub StreamMessage<B>, PhantomData<fn() -> B>);
impl<B: CloneableStreamable> Message for InputStreamMessageB<B> {
    type Result = ();
}
impl<B: CloneableStreamable> Clone for InputStreamMessageB<B> where StreamMessage<B>: Clone {
    fn clone(&self) -> Self {
        InputStreamMessageB(self.0.clone(), PhantomData)
    }
}


// --- ZipInputAdapterActor: Adapts a Stream<InType> to send WrappedMsgType to ZipActor ---

pub(crate) struct ZipInputAdapterActor<InType, WrappedMsgType>
where
    InType: CloneableStreamable + 'static,
    WrappedMsgType: Message<Result = ()> + From<StreamMessage<InType>> + Send + Debug + 'static,
    StreamMessage<InType>: Clone, // Ensure StreamMessage<InType> is Cloneable for logging/sending
{
    actor_id: usize,
    zip_actor_recipient: Recipient<WrappedMsgType>,
    stream_name: String, // For logging (e.g., "A" or "B")
    _phantom_in: PhantomData<InType>,
}

impl<InType, WrappedMsgType> ZipInputAdapterActor<InType, WrappedMsgType>
where
    InType: CloneableStreamable + 'static,
    WrappedMsgType: Message<Result = ()> + From<StreamMessage<InType>> + Send + Debug + 'static,
    StreamMessage<InType>: Clone,
{
    pub(crate) fn new(zip_actor_recipient: Recipient<WrappedMsgType>, stream_name: String) -> Self {
        static ADAPTER_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let actor_id = ADAPTER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log::trace!("[ZipInputAdapterActor-{}] Creating for stream '{}'", actor_id, stream_name);
        Self {
            actor_id,
            zip_actor_recipient,
            stream_name,
            _phantom_in: PhantomData,
        }
    }
}

impl<InType, WrappedMsgType> Actor for ZipInputAdapterActor<InType, WrappedMsgType>
where
    InType: CloneableStreamable + 'static,
    WrappedMsgType: Message<Result = ()> + From<StreamMessage<InType>> + Send + Debug + 'static,
    StreamMessage<InType>: Clone,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        log::trace!("[ZipInputAdapterActor-{}] Started for stream '{}'", self.actor_id, self.stream_name);
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::trace!("[ZipInputAdapterActor-{}] Stopped for stream '{}'", self.actor_id, self.stream_name);
    }
}

impl<InType, WrappedMsgType> Handler<StreamMessage<InType>> for ZipInputAdapterActor<InType, WrappedMsgType>
where
    InType: CloneableStreamable + 'static,
    WrappedMsgType: Message<Result = ()> + From<StreamMessage<InType>> + Send + Debug + 'static,
    StreamMessage<InType>: Clone,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<InType>, ctx: &mut Context<Self>) {
        println!("[AdapterActor-{}] handle: Received msg: {:?} for stream '{}'", self.actor_id, msg, self.stream_name);
        log::trace!("[ZipInputAdapterActor-{}] Received from upstream '{}': {:?}", self.actor_id, self.stream_name, msg);
        
        // We need to clone `msg` if `From` consumes it, and we also need `msg` for `StreamMessage::End` check.
        // Or, ensure `From` takes a reference if `msg` is not `Clone` and that's feasible.
        // Given StreamMessage<T> is usually CloneableStreamable, `msg.clone()` is idiomatic.
        let wrapped_msg = WrappedMsgType::from(msg.clone()); 
        
        if self.zip_actor_recipient.try_send(wrapped_msg).is_err() {
            println!("[AdapterActor-{}] handle: Failed to send to ZipActor from stream '{}'. Stopping adapter.", self.actor_id, self.stream_name);
            log::warn!("[ZipInputAdapterActor-{}] Failed to send to ZipActor from stream '{}'. ZipActor might have stopped. Stopping adapter.", self.actor_id, self.stream_name);
            ctx.stop();
            return; // Important to return after stopping
        }

        if let StreamMessage::End = msg {
            println!("[AdapterActor-{}] handle: Upstream '{}' ended. Forwarded End. Stopping adapter itself.", self.actor_id, self.stream_name);
            log::trace!("[ZipInputAdapterActor-{}] Upstream '{}' ended. Stopping adapter.", self.actor_id, self.stream_name);
            ctx.stop();
        }
    }
}

// Required for From<StreamMessage<InType>> for WrappedMsgType
impl<A: CloneableStreamable> From<StreamMessage<A>> for InputStreamMessageA<A> {
    fn from(msg: StreamMessage<A>) -> Self {
        InputStreamMessageA(msg, PhantomData)
    }
}
impl<B: CloneableStreamable> From<StreamMessage<B>> for InputStreamMessageB<B> {
    fn from(msg: StreamMessage<B>) -> Self {
        InputStreamMessageB(msg, PhantomData)
    }
}


// --- ZipActor ---
pub(crate) struct ZipActor<A, B, C, F>
where
    A: CloneableStreamable + 'static,
    B: CloneableStreamable + 'static,
    C: CloneableStreamable + 'static,
    F: Fn(A, B) -> C + Send + Sync + 'static, // Fn needs to be Send + Sync if actor runs in different thread context potentially
{
    actor_id: usize,
    downstream: Recipient<StreamMessage<C>>,
    zip_function: Arc<F>, // Arc for shared ownership if F is Fn (not FnOnce)
    buffer_a: VecDeque<A>, // Changed to VecDeque
    buffer_b: VecDeque<B>, // Changed to VecDeque
    ended_a: bool, // True if Stream A has signaled End
    ended_b: bool, // True if Stream B has signaled End
    downstream_signaled_end: bool,
    ctx_stop_requested: bool, // To prevent multiple stop calls if logic error
}

impl<A, B, C, F> ZipActor<A, B, C, F>
where
    A: CloneableStreamable + 'static,
    B: CloneableStreamable + 'static,
    C: CloneableStreamable + 'static,
    F: Fn(A, B) -> C + Send + Sync + 'static,
{
    pub(crate) fn new(downstream: Recipient<StreamMessage<C>>, zip_function: F) -> Self {
        static ACTOR_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let actor_id = ACTOR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log::debug!("[ZipActor-{}] Creating new instance.", actor_id);
        ZipActor {
            actor_id,
            downstream,
            zip_function: Arc::new(zip_function),
            buffer_a: VecDeque::new(), // Initialize as empty VecDeque
            buffer_b: VecDeque::new(), // Initialize as empty VecDeque
            ended_a: false,
            ended_b: false,
            downstream_signaled_end: false,
            ctx_stop_requested: false,
        }
    }

    // Processes elements from queues, zips them, and checks for termination.
    fn process_queues(&mut self, ctx: &mut Context<Self>) {
        println!("[ZipActor-{}] process_queues: Start. ended_a: {}, ended_b: {}, buf_a: {}, buf_b: {}", self.actor_id, self.ended_a, self.ended_b, self.buffer_a.len(), self.buffer_b.len());
        while self.buffer_a.front().is_some() && self.buffer_b.front().is_some() {
            // Check if actor is stopping before processing more items.
            if self.downstream_signaled_end || self.ctx_stop_requested {
                log::trace!("[ZipActor-{}] process_queues: Stop condition (downstream_signaled_end: {}, ctx_stop_requested: {}) met at loop start. Breaking.", self.actor_id, self.downstream_signaled_end, self.ctx_stop_requested);
                break; // Exit loop, proceed to final termination checks.
            }

            // Safe to unwrap due to the loop condition.
            let a_val = self.buffer_a.pop_front().unwrap();
            let b_val = self.buffer_b.pop_front().unwrap();
            
            println!("[ZipActor-{}] process_queues: Popped A={:?}, B={:?}", self.actor_id, a_val, b_val);

            log::trace!("[ZipActor-{}] Zipping elements A={:?} and B={:?}. Remaining A: {}, B: {}", self.actor_id, a_val, b_val, self.buffer_a.len(), self.buffer_b.len());
            // Clone a_val and b_val for the zip_function, so original values are kept for potential re-queueing.
            // This is crucial because F: Fn(A,B) -> C typically consumes A and B.
            let c_val = (self.zip_function)(a_val.clone(), b_val.clone());

            if self.downstream.try_send(StreamMessage::Element(c_val.clone())).is_err() { // c_val itself is cloned for the message
                log::warn!("[ZipActor-{}] Downstream recipient closed while sending zipped element. Re-queuing A={:?}, B={:?} and stopping.", self.actor_id, a_val, b_val);
                // Re-queue the original popped elements as they were not processed.
                self.buffer_a.push_front(a_val); // a_val is the original, consumed by pop_front earlier, not the clone passed to zip_function
                self.buffer_b.push_front(b_val); // b_val is the original
                
                self.ensure_downstream_end_and_stop(ctx);
                // The message in println should reflect that elements were re-queued.
                println!("[ZipActor-{}] process_queues: Failed to send Element({:?}). Downstream closed. Re-queued inputs. Returning.", self.actor_id, c_val);
                return; // Stop processing and exit function; actor stop handled by ensure_downstream_end_and_stop.
            }
            println!("[ZipActor-{}] process_queues: Sent Element({:?})", self.actor_id, c_val);
        }

        // After processing all possible pairs, check if termination conditions are met
        // Terminate if stream A is exhausted (ended and its buffer is empty)
        // OR if stream B is exhausted (ended and its buffer is empty).
        let a_exhausted = self.ended_a && self.buffer_a.is_empty();
        let b_exhausted = self.ended_b && self.buffer_b.is_empty();

        if a_exhausted || b_exhausted {
            if !(self.downstream_signaled_end || self.ctx_stop_requested) { // Only log/stop if not already doing so
                println!("[ZipActor-{}] process_queues: Termination condition met. A_exhausted ({}), B_exhausted ({}). Stopping.", self.actor_id, a_exhausted, b_exhausted);
                log::debug!("[ZipActor-{}] process_queues: Termination condition: A_exhausted ({}) OR B_exhausted ({}). Stopping.", self.actor_id, a_exhausted, b_exhausted);
                self.ensure_downstream_end_and_stop(ctx);
            }
        }
        println!("[ZipActor-{}] process_queues: End. ended_a: {}, ended_b: {}, buf_a: {}, buf_b: {}", self.actor_id, self.ended_a, self.ended_b, self.buffer_a.len(), self.buffer_b.len());
    }

    // Ensures End is sent to downstream (if not already) and requests actor stop.
    fn ensure_downstream_end_and_stop(&mut self, ctx: &mut Context<Self>) {
        println!("[ZipActor-{}] ensure_downstream_end_and_stop: Called. downstream_signaled_end: {}, ctx_stop_requested: {}", self.actor_id, self.downstream_signaled_end, self.ctx_stop_requested);
        if !self.downstream_signaled_end {
            println!("[ZipActor-{}] ensure_downstream_end_and_stop: Signaling End to downstream.", self.actor_id);
            if self.downstream.try_send(StreamMessage::End).is_err() {
                log::warn!("[ZipActor-{}] Failed to send End to downstream (already closed).", self.actor_id);
            }
            self.downstream_signaled_end = true;
        }
        if !self.ctx_stop_requested {
            if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                println!("[ZipActor-{}] ensure_downstream_end_and_stop: Requesting actor stop.", self.actor_id);
                self.ctx_stop_requested = true; // Mark that stop has been requested
                ctx.stop();
            } else {
                 log::trace!("[ZipActor-{}] Actor already stopping/stopped. No action for stop request.", self.actor_id);
            }
        } else {
            log::trace!("[ZipActor-{}] Actor stop already requested. No action.", self.actor_id);
        }
    }
}

impl<A, B, C, F> Actor for ZipActor<A, B, C, F>
where
    A: CloneableStreamable + 'static,
    B: CloneableStreamable + 'static,
    C: CloneableStreamable + 'static,
    F: Fn(A, B) -> C + Send + Sync + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        log::debug!("[ZipActor-{}] Actor started.", self.actor_id);
    }
    
    fn stopping(&mut self, ctx: &mut Context<Self>) -> Running {
        log::debug!("[ZipActor-{}] Actor stopping.", self.actor_id);
        self.ensure_downstream_end_and_stop(ctx); // Ensure end is signaled on explicit stop too
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::debug!("[ZipActor-{}] Actor stopped.", self.actor_id);
    }
}

impl<A, B, C, F> Handler<InputStreamMessageA<A>> for ZipActor<A, B, C, F>
where
    A: CloneableStreamable + 'static,
    B: CloneableStreamable + 'static,
    C: CloneableStreamable + 'static,
    F: Fn(A, B) -> C + Send + Sync + 'static,
{
    type Result = ();

    fn handle(&mut self, wrapped_msg: InputStreamMessageA<A>, ctx: &mut Context<Self>) {
        if self.downstream_signaled_end || self.ctx_stop_requested {
            log::trace!("[ZipActor-{}] Downstream ended or stop requested. Ignoring message from Stream A: {:?}", self.actor_id, wrapped_msg.0);
            return;
        }
        println!("[ZipActor-{}] handle A: Received wrapped_msg: {:?}", self.actor_id, wrapped_msg);
        log::trace!("[ZipActor-{}] Received from Stream A: {:?}", self.actor_id, wrapped_msg.0);

        match wrapped_msg.0 {
            StreamMessage::Element(a_val) => {
                log::trace!("[ZipActor-{}] Received Element from A, adding to queue. Queue A size: {}", self.actor_id, self.buffer_a.len() + 1);
                self.buffer_a.push_back(a_val);
                self.process_queues(ctx);
            }
            StreamMessage::End => {
                log::debug!("[ZipActor-{}] Stream A signaled End.", self.actor_id);
                self.ended_a = true;
                self.process_queues(ctx); // Process queues will check for termination
            }
        }
    }
}

impl<A, B, C, F> Handler<InputStreamMessageB<B>> for ZipActor<A, B, C, F>
where
    A: CloneableStreamable + 'static,
    B: CloneableStreamable + 'static,
    C: CloneableStreamable + 'static,
    F: Fn(A, B) -> C + Send + Sync + 'static,
{
    type Result = ();

    fn handle(&mut self, wrapped_msg: InputStreamMessageB<B>, ctx: &mut Context<Self>) {
        if self.downstream_signaled_end || self.ctx_stop_requested {
            log::trace!("[ZipActor-{}] Downstream ended or stop requested. Ignoring message from Stream B: {:?}", self.actor_id, wrapped_msg.0);
            return;
        }
        println!("[ZipActor-{}] handle B: Received wrapped_msg: {:?}", self.actor_id, wrapped_msg);
        log::trace!("[ZipActor-{}] Received from Stream B: {:?}", self.actor_id, wrapped_msg.0);
        match wrapped_msg.0 {
            StreamMessage::Element(b_val) => {
                log::trace!("[ZipActor-{}] Received Element from B, adding to queue. Queue B size: {}", self.actor_id, self.buffer_b.len() + 1);
                self.buffer_b.push_back(b_val);
                self.process_queues(ctx);
            }
            StreamMessage::End => {
                log::debug!("[ZipActor-{}] Stream B signaled End.", self.actor_id);
                self.ended_b = true;
                self.process_queues(ctx); // Process queues will check for termination
            }
        }
    }
}