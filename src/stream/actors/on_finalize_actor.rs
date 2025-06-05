use crate::stream::{CloneableStreamable, StreamMessage};
use actix::prelude::*;
use actix::Arbiter; // Added for Arbiter::spawn
use std::future::Future;

/// Actor responsible for the `on_finalize` stream operation.
///
/// It forwards all elements from an upstream source to a downstream recipient.
/// When the upstream stream completes (either successfully with `End` or with an `Error`),
/// or when this actor itself is stopped for any reason (e.g., downstream subscriber disappears,
/// or an upstream error causes a stop), it executes a provided side-effecting future (`effect_fn`).
///
/// The effect is guaranteed to be triggered at most once.
/// After triggering the effect, the original terminal message (if any from upstream)
/// is propagated to the downstream recipient.
pub(crate) struct OnFinalizeActor<O, Fut, F>
where
    O: CloneableStreamable,
    Fut: Future<Output = ()> + Send + 'static,
    F: FnOnce() -> Fut + Send + 'static + Unpin,
{
    downstream_recipient: Recipient<StreamMessage<O>>,
    effect_fn: Option<F>,
    effect_triggered: bool,
    /// True if a terminal message (`End`) has been received from upstream.
    /// This helps prevent processing further messages after termination and in `stopping` logic.
    upstream_has_terminated: bool,
}

impl<O, Fut, F> OnFinalizeActor<O, Fut, F>
where
    O: CloneableStreamable,
    Fut: Future<Output = ()> + Send + 'static,
    F: FnOnce() -> Fut + Send + 'static + Unpin,
{
    pub fn new(downstream_recipient: Recipient<StreamMessage<O>>, effect_fn: F) -> Self {
        Self {
            downstream_recipient,
            effect_fn: Some(effect_fn),
            effect_triggered: false,
            upstream_has_terminated: false,
        }
    }

    /// Triggers the effect if it hasn't been triggered yet.
    /// The effect future is spawned onto the current Actix Arbiter.
    /// This means the actor does not wait for the effect to complete, and the effect's
    /// execution is not tied to this actor's context lifecycle after spawning.
    fn trigger_effect_if_needed(&mut self, _ctx: &mut Context<Self>) { // ctx no longer strictly needed here
        if !self.effect_triggered {
            if let Some(f) = self.effect_fn.take() {
                let effect_future = f();
                // Spawn the future on the current arbiter's thread pool.
                // It runs independently of this actor's lifecycle once spawned.
                Arbiter::current().spawn(effect_future);
                // log::trace!("[OnFinalizeActor] Effect triggered via Arbiter::spawn.");
            }
            self.effect_triggered = true;
        }
    }

    /// Sends a terminal message to the downstream recipient and stops the actor.
    /// This should be called after ensuring the effect is triggered (usually via `stopping`).
    fn send_terminal_to_downstream_and_stop(&mut self, msg: StreamMessage<O>, ctx: &mut Context<Self>) {
        self.downstream_recipient.do_send(msg.clone());
        // log::trace!("[OnFinalizeActor] Propagated terminal message: {:?}, stopping actor.", msg);
        ctx.stop();
    }
}

impl<O, Fut, F> Actor for OnFinalizeActor<O, Fut, F>
where
    O: CloneableStreamable,
    Fut: Future<Output = ()> + Send + 'static,
    F: FnOnce() -> Fut + Send + 'static + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // log::trace!("[OnFinalizeActor] Started.");
        // This actor is now passive; its recipient for StreamMessage<O>
        // will be provided to the upstream stream's setup_fn.
    }

    fn stopping(&mut self, ctx: &mut Context<Self>) -> Running {
        // log::trace!("[OnFinalizeActor] Stopping. Ensuring effect is triggered.");
        // This is a crucial point: `stopping` is called when the actor is about to stop for ANY reason,
        // including the downstream recipient disappearing, an explicit ctx.stop(), or supervisor stopping it.
        // This ensures the effect is run even if the stream is prematurely terminated or an error occurs.
        self.trigger_effect_if_needed(ctx); // Pass ctx, though it's marked unused in the method for now
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        // log::trace!("[OnFinalizeActor] Stopped.");
        // Effect is ensured by the `stopping` handler.
    }
}

// Handler for messages from the UPSTREAM source (StreamMessage<O>)
impl<O, Fut, F> Handler<StreamMessage<O>> for OnFinalizeActor<O, Fut, F>
where
    O: CloneableStreamable,
    Fut: Future<Output = ()> + Send + 'static,
    F: FnOnce() -> Fut + Send + 'static + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<O>, ctx: &mut Context<Self>) {
        // log::trace!("[OnFinalizeActor] Received from upstream: {:?}", msg);

        if self.upstream_has_terminated {
            // If upstream already sent a terminal message, ignore further messages.
            // log::warn!("[OnFinalizeActor] Received message after upstream termination: {:?}", msg);
            return;
        }

        match msg {
            StreamMessage::Element(o) => {
                // Forward the element. If sending fails, it means the downstream recipient
                // is no longer available (e.g., a 'take' operator has completed and stopped listening).
                // In such cases, we should trigger the finalization effect and stop this actor.
                if self.downstream_recipient.try_send(StreamMessage::Element(o.clone())).is_err() {
                    // log::debug!("[OnFinalizeActor] Downstream recipient is gone (e.g., 'take' completed or downstream error). Triggering effect and stopping.");
                    // Trigger effect: stopping() will be called by ctx.stop(), which handles it.
                    // We call trigger_effect_if_needed here to ensure it's initiated if stopping() pathway is delayed or complex.
                    // The effect_triggered flag prevents double execution.
                    self.trigger_effect_if_needed(ctx);
                    ctx.stop(); // This will invoke self.stopping()
                }
            }
            StreamMessage::End => {
                self.upstream_has_terminated = true;
                // The effect is triggered when the actor stops.
                // self.stopping() will be called as part of ctx.stop().
                self.send_terminal_to_downstream_and_stop(StreamMessage::End, ctx);
            }
            // Note: StreamMessage does not have an Error variant.
            // Errors from an upstream SetupFn or unexpected upstream actor termination
            // should lead to this actor being stopped by its supervisor or the Actix runtime,
            // which in turn calls self.stopping(), triggering the finalize effect.
        }
    }
}