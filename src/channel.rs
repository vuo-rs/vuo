use crate::stream::streamable::CloneableStreamable;
use actix::prelude::*;
use futures::channel::oneshot;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;

// --- Channel Errors ---

#[derive(Debug, PartialEq, Eq)]
pub enum ChannelSendError<A: CloneableStreamable> {
    Full(A),
    Closed(A),
}

#[derive(Debug, PartialEq, Eq)]
pub enum ChannelReceiveError {
    Empty,
    Closed,
}

// --- Actor Messages ---

#[derive(Message)]
#[rtype(result = "()")] // Actual result sent via oneshot::Sender
struct SendMessage<A: CloneableStreamable> {
    item: A,
    tx: oneshot::Sender<Result<(), ChannelSendError<A>>>,
}

#[derive(Message)]
#[rtype(result = "()")] // Actual result sent via oneshot::Sender
struct ReceiveMessage<A: CloneableStreamable> {
    tx: oneshot::Sender<Result<A, ChannelReceiveError>>,
}

#[derive(Message)]
#[rtype(result = "()")]
struct CloseMessage;

#[derive(Message)]
#[rtype(result = "usize")]
struct SizeMessage;

#[derive(Message)]
#[rtype(result = "bool")]
struct IsClosedMessage;

// --- ChannelActor ---

#[derive(Debug)]
struct ChannelActor<A: CloneableStreamable + 'static> {
    buffer: VecDeque<A>,
    capacity: usize,
    closed: bool,
    pending_receives: VecDeque<oneshot::Sender<Result<A, ChannelReceiveError>>>,
    // pending_sends: VecDeque<(A, oneshot::Sender<Result<(), ChannelSendError<A>>>)>, // For backpressure on send if desired
}

impl<A: CloneableStreamable + 'static> ChannelActor<A> {
    fn new(capacity: usize) -> Self {
        // Ensure capacity is at least 1 for bounded channels.
        // For unbounded, capacity could be usize::MAX, but we'll assume bounded for now.
        let effective_capacity = if capacity == 0 { 1 } else { capacity };
        ChannelActor {
            buffer: VecDeque::with_capacity(effective_capacity),
            capacity: effective_capacity,
            closed: false,
            pending_receives: VecDeque::new(),
        }
    }

    // Helper to satisfy pending receives if items are available
    fn try_satisfy_pending_receive(&mut self) {
        while !self.buffer.is_empty() {
            if let Some(waiting_tx) = self.pending_receives.pop_front() {
                if let Some(item) = self.buffer.pop_front() {
                    if waiting_tx.send(Ok(item)).is_err() {
                        // Receiver might have been dropped, log or handle as needed
                        // For now, we'll just continue trying to satisfy others
                    }
                } else {
                    // Should not happen if buffer.is_empty() is false, but good to be defensive
                    break; 
                }
            } else {
                // No more pending receives
                break;
            }
        }
    }
}

impl<A: CloneableStreamable + 'static> Actor for ChannelActor<A> {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::trace!("ChannelActor started");
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        if !self.closed {
            self.closed = true;
        }
        // Notify all pending receivers that the channel is closed
        while let Some(tx) = self.pending_receives.pop_front() {
            let _ = tx.send(Err(ChannelReceiveError::Closed));
        }
        // log::trace!("ChannelActor stopped");
        Running::Stop
    }
}

// --- Message Handlers for ChannelActor ---

impl<A: CloneableStreamable + 'static> Handler<SendMessage<A>> for ChannelActor<A> {
    type Result = (); // Result is sent via oneshot

    fn handle(&mut self, msg: SendMessage<A>, _ctx: &mut Context<Self>) {
        if self.closed {
            let _ = msg.tx.send(Err(ChannelSendError::Closed(msg.item)));
            return;
        }

        if self.buffer.len() >= self.capacity {
            let _ = msg.tx.send(Err(ChannelSendError::Full(msg.item)));
            return;
        }

        self.buffer.push_back(msg.item);
        self.try_satisfy_pending_receive(); // Check if anyone was waiting
        let _ = msg.tx.send(Ok(()));
    }
}

impl<A: CloneableStreamable + 'static> Handler<ReceiveMessage<A>> for ChannelActor<A> {
    type Result = (); // Result is sent via oneshot

    fn handle(&mut self, msg: ReceiveMessage<A>, _ctx: &mut Context<Self>) {
        if !self.buffer.is_empty() {
            let item = self.buffer.pop_front().unwrap(); // Safe due to is_empty check
            let _ = msg.tx.send(Ok(item));
            return;
        }

        if self.closed {
            let _ = msg.tx.send(Err(ChannelReceiveError::Closed));
            return;
        }

        // Buffer is empty, channel is open, so queue the receiver
        self.pending_receives.push_back(msg.tx);
    }
}

impl<A: CloneableStreamable + 'static> Handler<CloseMessage> for ChannelActor<A> {
    type Result = ();

    fn handle(&mut self, _msg: CloseMessage, _ctx: &mut Context<Self>) {
        if !self.closed {
            self.closed = true;
            // Notify all pending receivers
            while let Some(tx) = self.pending_receives.pop_front() {
                let _ = tx.send(Err(ChannelReceiveError::Closed));
            }
            // Potentially notify pending senders if we implement backpressure for send
        }
        // In some designs, close might stop the actor if no items and no pending.
        // Here, we keep it alive to respond to IsClosed, Size etc.
        // It will stop if its Addr is dropped and no more messages are processed.
    }
}

impl<A: CloneableStreamable + 'static> Handler<SizeMessage> for ChannelActor<A> {
    type Result = usize;

    fn handle(&mut self, _msg: SizeMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.buffer.len()
    }
}

impl<A: CloneableStreamable + 'static> Handler<IsClosedMessage> for ChannelActor<A> {
    type Result = bool;

    fn handle(&mut self, _msg: IsClosedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.closed
    }
}

// --- Channel Public API ---

#[derive(Debug)]
pub struct Channel<A: CloneableStreamable> {
    actor_addr: Addr<ChannelActor<A>>,
    _phantom_a: PhantomData<A>,
}

impl<A: CloneableStreamable> Clone for Channel<A> {
    fn clone(&self) -> Self {
        Channel {
            actor_addr: self.actor_addr.clone(),
            _phantom_a: PhantomData,
        }
    }
}

impl<A: CloneableStreamable + 'static> Channel<A> {
    pub fn new(capacity: usize) -> Self {
        let actor_addr = ChannelActor::new(capacity).start();
        Channel { actor_addr, _phantom_a: PhantomData }
    }

    /// Sends an item to the channel.
    ///
    /// If the channel is full, this method will return `Err(ChannelSendError::Full)`.
    /// If the channel is closed, this method will return `Err(ChannelSendError::Closed)`.
    /// If the send operation is successful, it returns `Ok(())`.
    pub async fn send(&self, item: A) -> Result<(), ChannelSendError<A>> {
        let (tx, rx) = oneshot::channel();
        // Clone item for the error case, as SendMessage takes ownership if send is successful
        // but if oneshot itself fails, we need to return the original item.
        let item_clone_for_error = item.clone();
        self.actor_addr.do_send(SendMessage { item, tx });
        match rx.await {
            Ok(res) => res,
            Err(_) => Err(ChannelSendError::Closed(item_clone_for_error)), // Actor likely died, treat as closed
        }
    }

    /// Receives an item from the channel.
    ///
    /// If the channel is empty and not closed, this method will wait until an item is available.
    /// If the channel is empty and closed, this method will return `Err(ChannelReceiveError::Closed)`.
    /// If an item is received, it returns `Ok(item)`.
    pub async fn receive(&self) -> Result<A, ChannelReceiveError> {
        let (tx, rx) = oneshot::channel();
        self.actor_addr.do_send(ReceiveMessage { tx });
        match rx.await {
            Ok(res) => res,
            Err(_) => Err(ChannelReceiveError::Closed), // Actor likely died, treat as closed
        }
    }

    /// Closes the channel.
    ///
    /// After a channel is closed, no more items can be sent.
    /// Pending receivers will be notified with `ChannelReceiveError::Closed`.
    /// Subsequent calls to `receive` on an empty closed channel will also return `ChannelReceiveError::Closed`.
    pub fn close(&self) {
        self.actor_addr.do_send(CloseMessage);
    }

    /// Returns the current number of items in the channel.
    pub async fn size(&self) -> usize {
        self.actor_addr.send(SizeMessage).await.unwrap_or(0) // Default to 0 if actor is dead
    }

    /// Returns `true` if the channel is closed.
    pub async fn is_closed(&self) -> bool {
        self.actor_addr.send(IsClosedMessage).await.unwrap_or(true) // Default to true if actor is dead
    }
}