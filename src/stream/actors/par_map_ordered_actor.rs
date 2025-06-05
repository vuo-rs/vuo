use crate::stream::{CloneableStreamable, StreamMessage};
use actix::prelude::*;
use futures::future::BoxFuture;
// FuturesUnordered is not directly used if futures message self on completion.
// use futures::stream::{FuturesUnordered, StreamExt}; 
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Message sent from the spawned mapping future back to the ParMapOrderedActor,
// carrying the original index, the processed item, and the semaphore permit to be released.
#[derive(Message, Debug)]
#[rtype(result = "()")]
struct InternalProcessedItem<Out: CloneableStreamable + 'static> {
    index: usize,
    item: Out,
    permit: OwnedSemaphorePermit, // Permit is carried to be dropped when this message is processed
}

#[derive(Debug)]
pub(crate) struct ParMapOrderedActor<In, Out, F>
where
    In: CloneableStreamable + 'static,
    Out: CloneableStreamable + 'static,
    F: Fn(In) -> BoxFuture<'static, Out> + Send + Sync + 'static + Clone + Unpin,
{
    actor_id: usize, // For logging
    map_fn: F,
    downstream: Recipient<StreamMessage<Out>>,
    semaphore: Arc<Semaphore>, // Limits active futures/tasks
    
    input_buffer: VecDeque<(usize, In)>, // Stores (original_index, item) for items waiting for a permit
    output_buffer: BTreeMap<usize, Out>, // Stores processed items by original_index, ready for ordered emission

    current_input_index: usize,      // Assigns an original index to incoming items
    next_expected_output_index: usize, // Next index to emit downstream to maintain order
    
    in_flight_count: usize,       // Number of map_fn futures currently being processed
    upstream_ended: bool,         // Flag: true if StreamMessage::End received from upstream
    downstream_signaled_end: bool, // Flag: true if StreamMessage::End has been sent downstream
    
    _phantom_in: PhantomData<In>, // To mark usage of In
}

impl<In, Out, F> ParMapOrderedActor<In, Out, F>
where
    In: CloneableStreamable + 'static,
    Out: CloneableStreamable + 'static,
    F: Fn(In) -> BoxFuture<'static, Out> + Send + Sync + 'static + Clone + Unpin,
{
    pub(crate) fn new(
        map_fn: F,
        downstream: Recipient<StreamMessage<Out>>,
        parallelism: usize,
    ) -> Self {
        let effective_parallelism = parallelism.max(1); // Ensure at least 1
        static ACTOR_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let actor_id = ACTOR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log::debug!(
            "[ParMapOrderedActor-{}] Creating new instance with parallelism: {}",
            actor_id,
            effective_parallelism
        );
        ParMapOrderedActor {
            actor_id,
            map_fn,
            downstream,
            semaphore: Arc::new(Semaphore::new(effective_parallelism)),
            input_buffer: VecDeque::new(),
            output_buffer: BTreeMap::new(),
            current_input_index: 0,
            next_expected_output_index: 0,
            in_flight_count: 0,
            upstream_ended: false,
            downstream_signaled_end: false,
            _phantom_in: PhantomData,
        }
    }

    // Tries to spawn new processing tasks from the input_buffer if permits are available.
    fn try_spawn_from_buffer(&mut self, ctx: &mut Context<Self>) {
        if self.downstream_signaled_end {
            log::trace!("[ParMapOrderedActor-{}] try_spawn_from_buffer: Downstream already signaled end. Skipping.", self.actor_id);
            return;
        }
        log::trace!("[ParMapOrderedActor-{}] try_spawn_from_buffer: Attempting to spawn. Input buffer size: {}, In-flight: {}", self.actor_id, self.input_buffer.len(), self.in_flight_count);

        while !self.input_buffer.is_empty() {
            log::trace!("[ParMapOrderedActor-{}] try_spawn_from_buffer: Loop iteration. Input buffer not empty.", self.actor_id);
            // Try to acquire a permit non-blockingly
            match self.semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    log::trace!("[ParMapOrderedActor-{}] try_spawn_from_buffer: Permit acquired.", self.actor_id);
                    if let Some((index, item)) = self.input_buffer.pop_front() {
                        self.in_flight_count += 1;
                        log::debug!(
                            "[ParMapOrderedActor-{}] try_spawn_from_buffer: Spawning task for index {}. In-flight: {}. Input buffer size: {}. Item: {:?}",
                            self.actor_id, index, self.in_flight_count, self.input_buffer.len(), item
                        );
                        let map_fn_clone = self.map_fn.clone();
                        let self_addr = ctx.address();
                        
                        // Spawn an Actix future. This future will run the user's map_fn.
                        // The permit is moved into this future and then into the InternalProcessedItem message.
                        // It will be dropped (releasing the semaphore slot) when the InternalProcessedItem message
                        // is dropped after being handled by the actor.
                        actix_rt::spawn(async move {
                            let processed_item = map_fn_clone(item).await;
                            // Send result back to self actor
                            self_addr.do_send(InternalProcessedItem {
                                index,
                                item: processed_item,
                                permit, 
                            });
                        });
                    } else {
                        // Should not happen if input_buffer wasn't empty, but if it does, drop the permit.
                        drop(permit);
                        log::warn!("[ParMapOrderedActor-{}] try_spawn_from_buffer: Permit acquired but input_buffer was empty. This should not happen.", self.actor_id);
                    }
                }
                Err(_no_permit_available) => {
                    log::trace!("[ParMapOrderedActor-{}] try_spawn_from_buffer: No permit available. Stopping spawn attempts for now. In-flight: {}", self.actor_id, self.in_flight_count);
                    // No permits available right now, stop trying to spawn.
                    break; 
                }
            }
        }
        if self.input_buffer.is_empty() {
            log::trace!("[ParMapOrderedActor-{}] try_spawn_from_buffer: Exited loop, input buffer is empty.", self.actor_id);
        }
    }

    // Tries to emit items from the output_buffer to downstream in the correct order.
    fn try_emit_ordered(&mut self, ctx: &mut Context<Self>) {
        if self.downstream_signaled_end {
            log::trace!("[ParMapOrderedActor-{}] try_emit_ordered: Downstream already signaled end. Skipping.", self.actor_id);
            return;
        }
        log::trace!(
            "[ParMapOrderedActor-{}] try_emit_ordered: Attempting to emit. Next expected: {}, Output buffer size: {}",
            self.actor_id, self.next_expected_output_index, self.output_buffer.len()
        );

        while let Some(item) = self.output_buffer.remove(&self.next_expected_output_index) {
            log::debug!(
                "[ParMapOrderedActor-{}] try_emit_ordered: Emitting item for index {}. Item: {:?}",
                self.actor_id, self.next_expected_output_index, item
            );
            if self.downstream.try_send(StreamMessage::Element(item)).is_err() {
                log::warn!("[ParMapOrderedActor-{}] try_emit_ordered: Downstream recipient closed or errored while sending element for index {}. Stopping.", self.actor_id, self.next_expected_output_index);
                // Downstream closed or errored.
                self.try_send_end_to_downstream_and_stop(ctx);
                return;
            }
            self.next_expected_output_index += 1;
        }
        log::trace!(
            "[ParMapOrderedActor-{}] try_emit_ordered: Finished emitting available items. Next expected: {}, Output buffer size: {}",
            self.actor_id, self.next_expected_output_index, self.output_buffer.len()
        );
        // After emitting, check if the entire stream operation is complete.
        self.check_completion(ctx);
    }
    
    // Checks if all work is done (upstream ended, no buffered input, no tasks in flight, output buffer empty)
    // and if so, signals End to downstream and stops the actor.
    fn check_completion(&mut self, ctx: &mut Context<Self>) {
        log::trace!(
            "[ParMapOrderedActor-{}] check_completion: upstream_ended: {}, input_buffer_empty: {}, in_flight_count: {}, output_buffer_empty: {}",
            self.actor_id,
            self.upstream_ended,
            self.input_buffer.is_empty(),
            self.in_flight_count,
            self.output_buffer.is_empty()
        );
        if self.upstream_ended && 
           self.input_buffer.is_empty() && 
           self.in_flight_count == 0 && 
           self.output_buffer.is_empty() {
            log::debug!("[ParMapOrderedActor-{}] check_completion: All conditions met. Signaling End and stopping.", self.actor_id);
            self.try_send_end_to_downstream_and_stop(ctx);
        } else {
            log::trace!("[ParMapOrderedActor-{}] check_completion: Conditions not met. Continuing.", self.actor_id);
        }
    }

    // Helper to ensure End is sent to downstream at most once and then stop the actor.
    fn try_send_end_to_downstream_and_stop(&mut self, ctx: &mut Context<Self>) {
        if !self.downstream_signaled_end {
            log::debug!("[ParMapOrderedActor-{}] try_send_end_to_downstream_and_stop: Signaling End to downstream.", self.actor_id);
            if self.downstream.try_send(StreamMessage::End).is_err() {
                log::warn!("[ParMapOrderedActor-{}] try_send_end_to_downstream_and_stop: Failed to send End to downstream (already closed).", self.actor_id);
            }
            self.downstream_signaled_end = true;
        } else {
            log::trace!("[ParMapOrderedActor-{}] try_send_end_to_downstream_and_stop: Downstream already signaled end.", self.actor_id);
        }
        if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
            log::debug!("[ParMapOrderedActor-{}] try_send_end_to_downstream_and_stop: Stopping actor.", self.actor_id);
            ctx.stop();
        }
    }
}

impl<In, Out, F> Actor for ParMapOrderedActor<In, Out, F>
where
    In: CloneableStreamable + 'static,
    Out: CloneableStreamable + 'static,
    F: Fn(In) -> BoxFuture<'static, Out> + Send + Sync + 'static + Clone + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        log::debug!("[ParMapOrderedActor-{}] Actor started. Current state: input_buffer_size: {}, in_flight_count: {}, output_buffer_size: {}, next_expected_output_index: {}",
            self.actor_id, self.input_buffer.len(), self.in_flight_count, self.output_buffer.len(), self.next_expected_output_index);
        // Initial attempt to spawn tasks if any items were buffered before starting (not typical).
        self.try_spawn_from_buffer(ctx);
    }
    
    fn stopping(&mut self, ctx: &mut Context<Self>) -> Running {
        log::debug!("[ParMapOrderedActor-{}] Actor stopping. Current state: input_buffer_size: {}, in_flight_count: {}, output_buffer_size: {}, next_expected_output_index: {}, upstream_ended: {}",
            self.actor_id, self.input_buffer.len(), self.in_flight_count, self.output_buffer.len(), self.next_expected_output_index, self.upstream_ended);
        // Ensure downstream is properly signaled that this stream part is ending.
        // Note: The semaphore permits held by in-flight futures will be released
        // when those futures complete and their InternalProcessedItem messages are dropped.
        // If the actor stops abruptly, those permits might not be released immediately
        // through the normal message flow. However, Arc<Semaphore> dropping will release them.
        self.try_send_end_to_downstream_and_stop(ctx); // Call the unified stop logic.
        Running::Stop
    }
}

// Handler for results from the concurrently processed mapping futures
impl<In, Out, F> Handler<InternalProcessedItem<Out>> for ParMapOrderedActor<In, Out, F>
where
    In: CloneableStreamable + 'static,
    Out: CloneableStreamable + 'static,
    F: Fn(In) -> BoxFuture<'static, Out> + Send + Sync + 'static + Clone + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: InternalProcessedItem<Out>, ctx: &mut Context<Self>) {
        let InternalProcessedItem { index, item, permit } = msg;

        log::trace!("[ParMapOrderedActor-{}] Handling InternalProcessedItem for index {}. Item: {:?}", self.actor_id, index, item);
        if self.downstream_signaled_end {
            log::trace!("[ParMapOrderedActor-{}] InternalProcessedItem: Downstream already signaled end. Dropping item for index {}. Permit will be dropped implicitly.", self.actor_id, index);
            // Destructured `permit` is dropped when it goes out of scope here.
            return;
        }
        
        self.in_flight_count -= 1;
        log::debug!(
            "[ParMapOrderedActor-{}] InternalProcessedItem: Processed item for index {}. In-flight: {}. Output buffer size before insert: {}.",
            self.actor_id, index, self.in_flight_count, self.output_buffer.len()
        );
        
        self.output_buffer.insert(index, item);
        log::trace!("[ParMapOrderedActor-{}] InternalProcessedItem: Inserted item for index {} into output_buffer. Output buffer size: {}", self.actor_id, index, self.output_buffer.len());

        // Explicitly drop the permit here to release the semaphore slot *before* trying to spawn more.
        drop(permit);
        log::trace!("[ParMapOrderedActor-{}] InternalProcessedItem: Permit for index {} dropped. Semaphore slot released.", self.actor_id, index);

        self.try_emit_ordered(ctx);     // Try to emit any in-order items
        self.try_spawn_from_buffer(ctx); // Now this call should be able to acquire the released permit
        self.check_completion(ctx);     // Check if all work is done
    }
}

// Handler for incoming stream items from upstream
impl<In, Out, F> Handler<StreamMessage<In>> for ParMapOrderedActor<In, Out, F>
where
    In: CloneableStreamable + 'static,
    Out: CloneableStreamable + 'static,
    F: Fn(In) -> BoxFuture<'static, Out> + Send + Sync + 'static + Clone + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<In>, ctx: &mut Context<Self>) {
        if self.downstream_signaled_end {
            log::trace!("[ParMapOrderedActor-{}] StreamMessage: Downstream already signaled end. Ignoring message: {:?}", self.actor_id, msg);
            return;
        }

        match msg {
            StreamMessage::Element(item) => {
                log::debug!(
                    "[ParMapOrderedActor-{}] StreamMessage::Element: Received item for new index {}. Item: {:?}. Input buffer size before push: {}",
                    self.actor_id, self.current_input_index, item, self.input_buffer.len()
                );
                self.input_buffer.push_back((self.current_input_index, item));
                self.current_input_index += 1;
                log::trace!("[ParMapOrderedActor-{}] StreamMessage::Element: Added to input_buffer. New size: {}. Next input index: {}.", self.actor_id, self.input_buffer.len(), self.current_input_index);
                self.try_spawn_from_buffer(ctx);
            }
            StreamMessage::End => {
                log::debug!("[ParMapOrderedActor-{}] StreamMessage::End: Received End from upstream. In-flight: {}, Input buffer: {}, Output buffer: {}",
                    self.actor_id, self.in_flight_count, self.input_buffer.len(), self.output_buffer.len());
                self.upstream_ended = true;
                self.check_completion(ctx); // Check if we can end now or wait for in-flight tasks
            }
        }
    }
}