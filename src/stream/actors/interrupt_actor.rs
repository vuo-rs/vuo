use crate::stream::{CloneableStreamable, StreamMessage};
use actix::prelude::*;
use actix_rt::task::JoinHandle as SpawnHandle; // Renamed for clarity from tokio::task::JoinHandle
use futures::future::{Fuse, FutureExt}; // Fuse is important for select! loop or handling completion
use std::fmt::Debug;
use std::pin::Pin;
use futures::Future;

// Message sent by the spawned task to InterruptActor when the interrupt future completes.
#[derive(Message, Debug)]
#[rtype(result = "()")]
struct InterruptSignalCompletedMsg;

#[derive(Debug)]
pub(crate) struct InterruptActor<Out, FUT>
where
    Out: CloneableStreamable + 'static,
    FUT: Future<Output = ()> + Send + 'static, // Interrupt future completes with ()
{
    actor_id: usize,
    downstream: Recipient<StreamMessage<Out>>,
    interrupt_future_opt: Option<Fuse<Pin<Box<FUT>>>>, // Holds the future before it's spawned
    interrupt_monitor_handle: Option<SpawnHandle<()>>,   // Handle for the task monitoring the interrupt future
    
    source_ended: bool,                   // True if the main data source has signaled End
    interrupt_triggered_by_signal: bool,  // True if the interrupt future has completed
    downstream_signaled_end: bool,        // True if End has been sent to downstream
    ctx_stop_requested: bool,             // To ensure ctx.stop() is called only once if needed
}

impl<Out, FUT> InterruptActor<Out, FUT>
where
    Out: CloneableStreamable + 'static,
    FUT: Future<Output = ()> + Send + 'static,
{
    pub(crate) fn new(downstream: Recipient<StreamMessage<Out>>, interrupt_future: FUT) -> Self {
        static ACTOR_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let actor_id = ACTOR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        log::debug!("[InterruptActor-{}] Creating new instance.", actor_id);
        InterruptActor {
            actor_id,
            downstream,
            interrupt_future_opt: Some(Box::pin(interrupt_future).fuse()),
            interrupt_monitor_handle: None,
            source_ended: false,
            interrupt_triggered_by_signal: false,
            downstream_signaled_end: false,
            ctx_stop_requested: false,
        }
    }

    fn ensure_downstream_end_and_stop(&mut self, ctx: &mut Context<Self>) {
        if !self.downstream_signaled_end {
            log::debug!("[InterruptActor-{}] Signaling End to downstream.", self.actor_id);
            if self.downstream.try_send(StreamMessage::End).is_err() {
                log::warn!("[InterruptActor-{}] Failed to send End to downstream (already closed).", self.actor_id);
            }
            self.downstream_signaled_end = true;
        }

        // Abort the interrupt monitoring task if it's running
        if let Some(handle) = self.interrupt_monitor_handle.take() {
            log::trace!("[InterruptActor-{}] Aborting interrupt monitor task.", self.actor_id);
            handle.abort();
        }

        if !self.ctx_stop_requested {
            if ctx.state() != ActorState::Stopping && ctx.state() != ActorState::Stopped {
                log::debug!("[InterruptActor-{}] Requesting actor stop.", self.actor_id);
                self.ctx_stop_requested = true;
                ctx.stop();
            }
        }
    }
}

impl<Out, FUT> Actor for InterruptActor<Out, FUT>
where
    Out: CloneableStreamable + 'static,
    FUT: Future<Output = ()> + Send + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        log::debug!("[InterruptActor-{}] Actor started.", self.actor_id);
        
        if let Some(interrupt_fut) = self.interrupt_future_opt.take() {
            let actor_addr = ctx.address();
            let actor_id_clone = self.actor_id;

            let monitor_task = actix_rt::spawn(async move {
                log::trace!("[InterruptActor-{}] Interrupt monitor task started.", actor_id_clone);
                interrupt_fut.await; // Await the user-provided future
                log::debug!("[InterruptActor-{}] Monitored interrupt future completed.", actor_id_clone);
                
                // Send a message to self to trigger interrupt logic
                if actor_addr.try_send(InterruptSignalCompletedMsg).is_err() {
                    log::warn!("[InterruptActor-{}] Failed to send InterruptSignalCompletedMsg to self. Actor might be stopping or stopped.", actor_id_clone);
                }
                log::trace!("[InterruptActor-{}] Interrupt monitor task finished.", actor_id_clone);
            });
            self.interrupt_monitor_handle = Some(monitor_task);
        } else {
            // This case should ideally not happen if new() always sets the future.
            log::error!("[InterruptActor-{}] Interrupt future was not available in started(). This is a bug.", self.actor_id);
            // Potentially stop the actor if the interrupt signal is critical and missing.
            self.ensure_downstream_end_and_stop(ctx); 
        }
    }

    fn stopping(&mut self, ctx: &mut Context<Self>) -> Running {
        log::debug!("[InterruptActor-{}] Actor stopping. Finalizing downstream and tasks.", self.actor_id);
        self.ensure_downstream_end_and_stop(ctx); // Ensure cleanup happens
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::debug!("[InterruptActor-{}] Actor stopped.", self.actor_id);
    }
}

// Handler for messages from the main data source stream
impl<Out, FUT> Handler<StreamMessage<Out>> for InterruptActor<Out, FUT>
where
    Out: CloneableStreamable + 'static,
    FUT: Future<Output = ()> + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<Out>, ctx: &mut Context<Self>) {
        if self.interrupt_triggered_by_signal || self.downstream_signaled_end || self.ctx_stop_requested {
            log::trace!("[InterruptActor-{}] Interrupt triggered or downstream ended/stop requested. Ignoring source message: {:?}", self.actor_id, msg);
            return;
        }

        match msg {
            StreamMessage::Element(item) => {
                log::trace!("[InterruptActor-{}] Received element: {:?}. Forwarding.", self.actor_id, item);
                // Item is cloned for the log, then passed to try_send.
                // If try_send fails, item is dropped.
                if self.downstream.try_send(StreamMessage::Element(item.clone())).is_err() {
                    log::warn!("[InterruptActor-{}] Downstream recipient closed while forwarding element {:?}. Stopping.", self.actor_id, item);
                    self.ensure_downstream_end_and_stop(ctx);
                }
            }
            StreamMessage::End => {
                log::debug!("[InterruptActor-{}] Source stream ended.", self.actor_id);
                self.source_ended = true;
                self.ensure_downstream_end_and_stop(ctx);
            }
        }
    }
}

// Handler for the internal message indicating the interrupt future has completed
impl<Out, FUT> Handler<InterruptSignalCompletedMsg> for InterruptActor<Out, FUT>
where
    Out: CloneableStreamable + 'static,
    FUT: Future<Output = ()> + Send + 'static,
{
    type Result = ();

    fn handle(&mut self, _msg: InterruptSignalCompletedMsg, ctx: &mut Context<Self>) {
        log::debug!("[InterruptActor-{}] Received InterruptSignalCompletedMsg.", self.actor_id);
        
        if self.ctx_stop_requested || self.downstream_signaled_end {
             log::trace!("[InterruptActor-{}] Interrupt signal processed but actor already stopping or stream ended.", self.actor_id);
            return;
        }

        if !self.interrupt_triggered_by_signal { // Process only if this is the first time interrupt is triggered
            self.interrupt_triggered_by_signal = true;
            log::info!("[InterruptActor-{}] Interrupt future completed! Signaling End downstream and stopping.", self.actor_id);
            self.ensure_downstream_end_and_stop(ctx);
        } else {
            log::trace!("[InterruptActor-{}] Interrupt signal already processed.", self.actor_id);
        }
    }
}