use crate::stream::streamable::CloneableStreamable;
use crate::stream::{Stream, StreamMessage};
use actix::prelude::*;
use futures::channel::oneshot;
use futures::future::BoxFuture; // This is Pin<Box<dyn Future + Send>>
use futures::FutureExt; // For .boxed()
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::sync::mpsc; // For MPSC channel
use tokio::task; // For tokio::task::spawn_local

#[derive(Debug)]
pub enum QueueOfferError<A: CloneableStreamable> {
    Full(A),
    Closed(A),
}

#[derive(Debug)]
pub enum QueuePollError {
    Empty,
    Closed,
}

// --- Actor Messages ---

#[derive(Message)]
#[rtype(result = "()")] // Actual result sent via oneshot::Sender
struct OfferMessage<A: CloneableStreamable> {
    item: A,
    tx: oneshot::Sender<Result<(), QueueOfferError<A>>>,
}

#[derive(Message)]
#[rtype(result = "()")] // Actual result sent via oneshot::Sender
struct PollMessage<A: CloneableStreamable> {
    tx: oneshot::Sender<Result<A, QueuePollError>>,
}

#[derive(Message)]
#[rtype(result = "usize")]
struct SizeMessage;

#[derive(Message)]
#[rtype(result = "bool")]
struct IsEmptyMessage;

#[derive(Message)]
#[rtype(result = "bool")]
struct IsFullMessage;

#[derive(Message)]
#[rtype(result = "bool")]
struct IsClosedMessage;

#[derive(Message)]
#[rtype(result = "()")]
struct CloseMessage;

#[derive(Message)]
#[rtype(result = "()")] // Actual Option<Stream<A>> sent via oneshot
struct ConsumeMessage<A: CloneableStreamable> {
    tx: oneshot::Sender<Option<Stream<A>>>,
    _phantom_a: PhantomData<A>,
}

// --- QueueActor ---

struct QueueActor<A: CloneableStreamable + 'static> {
    buffer: VecDeque<A>,
    capacity: usize,
    closed: bool,
    pending_polls: VecDeque<oneshot::Sender<Result<A, QueuePollError>>>,
}

impl<A: CloneableStreamable + 'static> QueueActor<A> {
    fn new(capacity: usize) -> Self {
        QueueActor {
            buffer: VecDeque::with_capacity(capacity),
            capacity,
            closed: false,
            pending_polls: VecDeque::new(),
        }
    }

    fn try_satisfy_pending_poll(&mut self) {
        if !self.buffer.is_empty() {
            if let Some(waiting_tx) = self.pending_polls.pop_front() {
                if let Some(item) = self.buffer.pop_front() {
                    let _ = waiting_tx.send(Ok(item));
                }
            }
        }
    }
}

impl<A: CloneableStreamable + 'static> Actor for QueueActor<A> {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {}

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        if !self.closed {
            self.closed = true;
        }
        while let Some(tx) = self.pending_polls.pop_front() {
            let _ = tx.send(Err(QueuePollError::Closed));
        }
        Running::Stop
    }
}

// --- Message Handlers for QueueActor ---

impl<A: CloneableStreamable + 'static> Handler<OfferMessage<A>> for QueueActor<A> {
    type Result = ();

    fn handle(&mut self, msg: OfferMessage<A>, _ctx: &mut Context<Self>) {
        if self.closed {
            let _ = msg.tx.send(Err(QueueOfferError::Closed(msg.item)));
            return;
        }
        if self.buffer.len() >= self.capacity {
            let _ = msg.tx.send(Err(QueueOfferError::Full(msg.item)));
            return;
        }
        self.buffer.push_back(msg.item);
        self.try_satisfy_pending_poll();
        let _ = msg.tx.send(Ok(()));
    }
}

impl<A: CloneableStreamable + 'static> Handler<PollMessage<A>> for QueueActor<A> {
    type Result = ();

    fn handle(&mut self, msg: PollMessage<A>, _ctx: &mut Context<Self>) {
        if !self.buffer.is_empty() {
            let item = self.buffer.pop_front().unwrap();
            let _ = msg.tx.send(Ok(item));
            return;
        }
        if self.closed {
            let _ = msg.tx.send(Err(QueuePollError::Closed));
            return;
        }
        self.pending_polls.push_back(msg.tx);
    }
}

impl<A: CloneableStreamable + 'static> Handler<SizeMessage> for QueueActor<A> {
    type Result = usize;
    fn handle(&mut self, _msg: SizeMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.buffer.len()
    }
}

impl<A: CloneableStreamable + 'static> Handler<IsEmptyMessage> for QueueActor<A> {
    type Result = bool;
    fn handle(&mut self, _msg: IsEmptyMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.buffer.is_empty()
    }
}

impl<A: CloneableStreamable + 'static> Handler<IsFullMessage> for QueueActor<A> {
    type Result = bool;
    fn handle(&mut self, _msg: IsFullMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.buffer.len() >= self.capacity
    }
}

impl<A: CloneableStreamable + 'static> Handler<IsClosedMessage> for QueueActor<A> {
    type Result = bool;
    fn handle(&mut self, _msg: IsClosedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        self.closed
    }
}

impl<A: CloneableStreamable + 'static> Handler<CloseMessage> for QueueActor<A> {
    type Result = ();
    fn handle(&mut self, _msg: CloseMessage, _ctx: &mut Context<Self>) {
        if !self.closed {
            self.closed = true;
            while let Some(tx) = self.pending_polls.pop_front() {
                let _ = tx.send(Err(QueuePollError::Closed));
            }
        }
    }
}

impl<A: CloneableStreamable + 'static> Handler<ConsumeMessage<A>> for QueueActor<A> {
    type Result = ();

    fn handle(&mut self, msg: ConsumeMessage<A>, _ctx: &mut Context<Self>) {
        if self.closed && self.buffer.is_empty() {
            let _ = msg.tx.send(None);
            return;
        }

        let items: Vec<A> = self.buffer.drain(..).collect();
        if !self.closed {
            self.closed = true;
        }
        while let Some(poll_tx) = self.pending_polls.pop_front() {
            let _ = poll_tx.send(Err(QueuePollError::Closed));
        }

        // This closure captures `items` (Send if A is Send) and `downstream_recipient` (!Send).
        // If SetupFn requires the closure to be Send, this is problematic.
        // This suggests that the interpretation of Stream's SetupFn Send requirement
        // or how Actix Recipient interacts might have nuances (e.g., specific Actix feature flags).\
        let stream_setup_fn = move |downstream_recipient: Recipient<StreamMessage<A>>| -> BoxFuture<'static, Result<(), String>> {
            async move {
                for item in items {
                    if downstream_recipient.try_send(StreamMessage::Element(item)).is_err() {
                        // If downstream is gone, this setup task should error.
                        return Err("Downstream recipient closed during static queue item processing".to_string());
                    }
                }
                Ok(())
            }
            .boxed()
        };

        let stream = Stream {
            setup_fn: Box::new(stream_setup_fn),
            _phantom: PhantomData,
        };

        let _ = msg.tx.send(Some(stream));
    }
}

// --- VuoQueue Public API ---

#[derive(Debug)]
pub struct VuoQueue<A: CloneableStreamable> {
    actor_addr: Addr<QueueActor<A>>,
    _phantom_a: PhantomData<A>,
}

impl<A: CloneableStreamable> Clone for VuoQueue<A> {
    fn clone(&self) -> Self {
        VuoQueue {
            actor_addr: self.actor_addr.clone(),
            _phantom_a: PhantomData,
        }
    }
}

impl<A: CloneableStreamable + 'static> VuoQueue<A> {
    pub fn new(capacity: usize) -> Self {
        let effective_capacity = if capacity == 0 { 1 } else { capacity };
        let actor_addr = QueueActor::new(effective_capacity).start();
        VuoQueue {
            actor_addr,
            _phantom_a: PhantomData,
        }
    }

    pub async fn offer(&self, item: A) -> Result<(), QueueOfferError<A>> {
        let (tx, rx) = oneshot::channel();
        self.actor_addr.do_send(OfferMessage {
            item: item.clone(),
            tx,
        });
        match rx.await {
            Ok(res) => res,
            Err(_) => Err(QueueOfferError::Closed(item)),
        }
    }

    pub async fn poll(&self) -> Result<A, QueuePollError> {
        let (tx, rx) = oneshot::channel();
        self.actor_addr.do_send(PollMessage { tx });
        match rx.await {
            Ok(res) => res,
            Err(_) => Err(QueuePollError::Closed),
        }
    }

    pub async fn size(&self) -> usize {
        self.actor_addr.send(SizeMessage).await.unwrap_or(0)
    }

    pub async fn is_empty(&self) -> bool {
        self.actor_addr.send(IsEmptyMessage).await.unwrap_or(true)
    }

    pub async fn is_full(&self) -> bool {
        self.actor_addr.send(IsFullMessage).await.unwrap_or(false)
    }

    pub async fn is_closed(&self) -> bool {
        self.actor_addr.send(IsClosedMessage).await.unwrap_or(true)
    }

    pub fn close(&self) {
        self.actor_addr.do_send(CloseMessage);
    }

    pub async fn consume(self) -> Option<Stream<A>> {
        let (tx, rx) = oneshot::channel();
        self.actor_addr.do_send(ConsumeMessage {
            tx,
            _phantom_a: PhantomData,
        });
        rx.await.unwrap_or(None)
    }

    pub fn dequeue_stream(&self) -> Stream<A> {
        let queue_for_polling_task = self.clone();

        // MPSC channel buffer size.
        const MPSC_BUFFER_SIZE: usize = 16; // Arbitrary small buffer
        let (mpsc_sender, mpsc_receiver) = mpsc::channel::<StreamMessage<A>>(MPSC_BUFFER_SIZE);

        // This task polls the VuoQueue (which is !Send due to Addr)
        // and sends results via the MPSC channel.
        // It must run on a LocalSet.
        task::spawn_local(async move {
            loop {
                match queue_for_polling_task.poll().await {
                    Ok(item) => {
                        if mpsc_sender
                            .send(StreamMessage::Element(item))
                            .await
                            .is_err()
                        {
                            // MPSC receiver was dropped, consumer stream is gone.
                            break;
                        }
                    }
                    Err(QueuePollError::Closed) | Err(QueuePollError::Empty) => {
                        // Queue is closed or empty (which implies closed for a blocking poll).
                        // Attempt to signal End to the MPSC channel.
                        let _ = mpsc_sender.send(StreamMessage::End).await;
                        break; // Stop polling.
                    }
                }
            }
            // When this local task ends, mpsc_sender is dropped.
            // This will cause mpsc_receiver.recv() in the setup_fn to eventually return None.
        });

        // This closure captures mpsc_receiver (which is Send).
        // The `downstream_actix_recipient` is a parameter, not a capture here.
        // Therefore, this closure itself should be Send.
        let setup_fn_closure = move |downstream_actix_recipient: Recipient<StreamMessage<A>>| -> BoxFuture<'static, Result<(), String>> {
            // The mpsc_receiver is moved into this closure.
            // To use it in the async block, we need to move it again.
            // Assign to a mutable variable to call .recv() in a loop.
            let mut mutable_mpsc_receiver = mpsc_receiver;

            // This async block should be Send because:
            // 1. It captures `mutable_mpsc_receiver` (which is Send).
            // 2. `mutable_mpsc_receiver.recv().await` is a Send-compatible await.
            // 3. `downstream_actix_recipient.try_send()` is synchronous and does not
            //    hold `downstream_actix_recipient` (a !Send type) across an await point.
            async move {
                loop {
                    match mutable_mpsc_receiver.recv().await {
                        Some(StreamMessage::Element(item)) => {
                            if downstream_actix_recipient.try_send(StreamMessage::Element(item)).is_err() {
                                // Downstream Actix consumer is gone.
                                return Err(String::from("Downstream Actix recipient is gone"));
                            }
                        }
                        Some(StreamMessage::End) => {
                            // Polling task signaled the end.
                            let _ = downstream_actix_recipient.try_send(StreamMessage::End);
                            return Ok(());
                        }
                        None => {
                            // MPSC channel was closed (sender dropped by the polling task ending).
                            // This is the primary way the stream terminates.
                            let _ = downstream_actix_recipient.try_send(StreamMessage::End);
                            return Ok(());
                        }
                    }
                }
            }
            .boxed() // This boxes the Send future into BoxFuture (which is also Send).
        };

        Stream {
            // Box::new(setup_fn_closure) creates a Box<dyn FnOnce + Send> if setup_fn_closure is Send.
            setup_fn: Box::new(setup_fn_closure),
            _phantom: PhantomData,
        }
    }
}
