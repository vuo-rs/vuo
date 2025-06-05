use crate::stream::{CloneableStreamable, StreamMessage, Stream};
use actix::prelude::*;

// --- Messages for HandleErrorWithActor ---

/// Message to inform the HandleErrorWithActor about the result of the primary stream's setup.
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub(crate) enum PrimaryStreamSetupResult {
    Success,
    Failure(String), // Carries the error message from the primary stream's setup
}

// --- HandleErrorWithActor ---

/// Actor responsible for the `handle_error_with` stream operation.
///
/// This actor attempts to run a primary stream. If the primary stream's setup fails,
/// or if the primary stream (once started) terminates unexpectedly (e.g., its actor stops
/// without sending a graceful `End` message), this actor will invoke a handler function.
/// The handler function receives the error (as a String) and returns a new "fallback" stream.
/// This fallback stream is then run in place of the primary one.
///
/// If the primary stream completes successfully (sends `End`), the handler is not called,
/// and the `End` message is propagated.
pub(crate) struct HandleErrorWithActor<O, FHandler>
where
    O: CloneableStreamable,
    FHandler: FnOnce(String) -> Stream<O> + Send + 'static + Unpin,
{
    /// Recipient for the final output stream (the one returned by `handle_error_with`).
    final_downstream_recipient: Recipient<StreamMessage<O>>,
    /// Function to generate the fallback stream. Option because it's FnOnce.
    error_handler_fn: Option<FHandler>,
    // The recipient for messages from the primary stream will be this actor's own address.
    // It's obtained via ctx.address().recipient() within the Stream::handle_error_with setup_fn.
    /// Current state of the actor.
    state: ActorInternalState,
    /// Tracks if the primary stream sent an `End` message, to prevent fallback on graceful termination.
    primary_stream_ended_gracefully: bool,
}

/// Internal state of the HandleErrorWithActor.
#[derive(Debug, Clone, PartialEq)]
enum ActorInternalState {
    Initializing,      // Waiting for the primary stream's setup_fn to complete.
    RunningPrimary,    // Primary stream is active and sending elements to this actor.
    RunningFallback,   // Fallback stream is active and sending elements to final_downstream_recipient.
    AwaitingFallbackSetup, // Fallback handler invoked, waiting for fallback stream's setup_fn.
    Finished,          // Terminal state, either End or an unrecoverable error has occurred.
}

impl<O, FHandler> HandleErrorWithActor<O, FHandler>
where
    O: CloneableStreamable,
    FHandler: FnOnce(String) -> Stream<O> + Send + 'static + Unpin,
{
    pub fn new(
        final_downstream_recipient: Recipient<StreamMessage<O>>,
        error_handler_fn: FHandler,
    ) -> Self {
        Self {
            final_downstream_recipient,
            error_handler_fn: Some(error_handler_fn),
            state: ActorInternalState::Initializing,
            primary_stream_ended_gracefully: false,
        }
    }

    /// Initiates the fallback stream.
    /// This is called when the primary stream's setup fails or when the primary stream
    /// (after setup) terminates unexpectedly.
    fn run_fallback_stream(&mut self, error_msg: String, ctx: &mut Context<Self>) {
        if self.state == ActorInternalState::RunningFallback
            || self.state == ActorInternalState::AwaitingFallbackSetup
            || self.state == ActorInternalState::Finished
        {
            // log::warn!("[HandleErrorWithActor] Attempted to run fallback stream when already in fallback, awaiting setup, or finished state.");
            return;
        }

        if let Some(handler) = self.error_handler_fn.take() {
            // log::debug!("[HandleErrorWithActor] Running fallback stream due to error: {}", error_msg);
            self.state = ActorInternalState::AwaitingFallbackSetup;
            let fallback_stream = handler(error_msg);

            // The fallback stream should send its output directly to the final downstream recipient.
            let setup_fn_future = (fallback_stream.setup_fn)(self.final_downstream_recipient.clone());

            ctx.spawn(
                setup_fn_future
                    .into_actor(self)
                    .map(|res, act, inner_ctx| {
                        match res {
                            Ok(()) => {
                                // log::trace!("[HandleErrorWithActor] Fallback stream setup completed successfully.");
                                // Fallback stream is now running. This actor's job for routing elements is done.
                                // It will stop once the fallback stream completes (by final_downstream_recipient being dropped or End being sent by fallback).
                                act.state = ActorInternalState::RunningFallback;
                            }
                            Err(fallback_setup_err) => {
                                log::error!("[HandleErrorWithActor] Fallback stream setup failed: {}. Propagating End and stopping.", fallback_setup_err);
                                // If the fallback stream itself fails to set up, we consider the whole operation failed.
                                // Propagate an End to the final downstream, as we can't produce more elements or a specific error type here directly.
                                act.final_downstream_recipient.do_send(StreamMessage::End);
                                act.state = ActorInternalState::Finished;
                                inner_ctx.stop();
                            }
                        }
                        // If setup was Ok, this actor now waits. If downstream drops, this actor stops.
                        // If fallback stream sends End, downstream may drop this actor's original recipient, causing stop.
                    }),
            );
        } else {
            // log::warn!("[HandleErrorWithActor] Error handler function already consumed. Cannot run fallback stream for error: {}. Propagating End.", error_msg);
            // If handler is gone, we can't recover. Propagate End and stop.
            self.final_downstream_recipient.do_send(StreamMessage::End);
            self.state = ActorInternalState::Finished;
            ctx.stop();
        }
    }
}

impl<O, FHandler> Actor for HandleErrorWithActor<O, FHandler>
where
    O: CloneableStreamable,
    FHandler: FnOnce(String) -> Stream<O> + Send + 'static + Unpin,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // log::trace!("[HandleErrorWithActor] Started. State: {:?}", self.state);
        // The primary stream's setup_fn is called by Stream::handle_error_with,
        // which will send a PrimaryStreamSetupResult message to this actor.
    }

    fn stopping(&mut self, ctx: &mut Context<Self>) -> Running {
        // log::debug!("[HandleErrorWithActor] Stopping. State: {:?}, Primary Gracefully Ended: {}", self.state, self.primary_stream_ended_gracefully);

        // This `stopping` method is crucial. It's called if this actor is stopped for *any* reason
        // not covered by graceful End from primary or successful handoff to fallback.
        // This includes:
        // 1. The primary stream's actor stopping unexpectedly (connection to this actor drops).
        // 2. The final_downstream_recipient being dropped by the ultimate consumer.
        match self.state {
            ActorInternalState::Initializing | ActorInternalState::RunningPrimary => {
                if !self.primary_stream_ended_gracefully {
                    // If we were initializing or running primary, and it didn't end gracefully,
                    // this means an error occurred or an unexpected stop.
                    // log::debug!("[HandleErrorWithActor] stopping: Primary stream did not end gracefully. Attempting fallback.");
                    self.run_fallback_stream(
                        "Primary stream terminated unexpectedly or setup was interrupted.".to_string(),
                        ctx,
                    );
                }
            }
            ActorInternalState::AwaitingFallbackSetup => {
                // If we are stopping while awaiting fallback setup, it means the fallback setup future
                // might be orphaned or the downstream disappeared. The fallback logic tries to handle this.
                // No explicit action here other than letting the existing logic complete/timeout.
                // log::debug!("[HandleErrorWithActor] stopping: Was awaiting fallback setup.");
            }
            ActorInternalState::RunningFallback | ActorInternalState::Finished => {
                // If fallback is running or we are already finished, do nothing more.
                // log::debug!("[HandleErrorWithActor] stopping: Fallback running or already finished.");
            }
        }
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Context<Self>) {
        // log::trace!("[HandleErrorWithActor] Stopped. Final state: {:?}", self.state);
    }
}

/// Handles the result of the primary stream's setup process.
impl<O, FHandler> Handler<PrimaryStreamSetupResult> for HandleErrorWithActor<O, FHandler>
where
    O: CloneableStreamable,
    FHandler: FnOnce(String) -> Stream<O> + Send + 'static + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: PrimaryStreamSetupResult, ctx: &mut Context<Self>) {
        // log::debug!("[HandleErrorWithActor] Received PrimaryStreamSetupResult: {:?}", msg);
        if self.state != ActorInternalState::Initializing {
            // log::warn!("[HandleErrorWithActor] Received PrimaryStreamSetupResult in unexpected state: {:?}", self.state);
            return;
        }

        match msg {
            PrimaryStreamSetupResult::Success => {
                self.state = ActorInternalState::RunningPrimary;
                // log::trace!("[HandleErrorWithActor] Primary stream setup success. Now in RunningPrimary state. Waiting for elements.");
                // At this point, this actor's `recipient_for_primary_stream` should start receiving messages.
            }
            PrimaryStreamSetupResult::Failure(err_msg) => {
                // log::debug!("[HandleErrorWithActor] Primary stream setup failed. Error: {}. Initiating fallback.", err_msg);
                self.run_fallback_stream(err_msg, ctx);
            }
        }
    }
}

/// Handles messages (Element or End) from the PRIMARY UPSTREAM stream.
impl<O, FHandler> Handler<StreamMessage<O>> for HandleErrorWithActor<O, FHandler>
where
    O: CloneableStreamable,
    FHandler: FnOnce(String) -> Stream<O> + Send + 'static + Unpin,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<O>, ctx: &mut Context<Self>) {
        if self.state != ActorInternalState::RunningPrimary {
            // If we are not in RunningPrimary state (e.g., already switched to fallback or finished),
            // messages from the primary stream should be ignored.
            // log::warn!("[HandleErrorWithActor] Received StreamMessage from primary upstream in unexpected state: {:?}. Message: {:?}", self.state, msg);
            return;
        }

        // log::trace!("[HandleErrorWithActor] Received from primary stream: {:?}", msg);
        match msg {
            StreamMessage::Element(o) => {
                // Forward element to the final downstream recipient.
                self.final_downstream_recipient.do_send(StreamMessage::Element(o));
            }
            StreamMessage::End => {
                // Primary stream completed successfully.
                self.primary_stream_ended_gracefully = true;
                self.final_downstream_recipient.do_send(StreamMessage::End);
                self.state = ActorInternalState::Finished;
                // log::trace!("[HandleErrorWithActor] Primary stream ended gracefully. Propagated End. Stopping actor.");
                ctx.stop();
            }
            // Note: StreamMessage does not have an Error variant.
            // Errors from the primary stream (after setup) manifest as its actor stopping,
            // which will cause this HandleErrorWithActor to be stopped by Actix (if the primary
            // was its only supervisor or if the MailboxError occurs).
            // The `stopping()` method of this actor is designed to catch such ungraceful terminations.
        }
    }
}