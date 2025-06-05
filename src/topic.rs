use crate::stream::{CloneableStreamable, Stream, StreamMessage};
use actix::prelude::*;
use futures::channel::oneshot; // For signaling from spawn_local
use futures::FutureExt;       // For .boxed()
use std::collections::HashMap;
use std::marker::PhantomData;
use tokio::task; // For tokio::task::spawn_local
use uuid::Uuid;

// --- Internal Messages for TopicActor ---

#[derive(Message)]
#[rtype(result = "()")]
struct PublishItem<A: CloneableStreamable> {
    item: A,
}

// Message sent from Topic to TopicActor to initiate a subscription.
// The actor will respond with a Uuid for this subscription.
#[derive(Message)]
#[rtype(result = "Result<Uuid, ()>")] // Ok(Uuid) on success, Err(()) if closed
struct SubscribeMsg<A: CloneableStreamable> {
    recipient: Recipient<StreamMessage<A>>,
}

#[derive(Message)]
#[rtype(result = "()")]
struct CloseTopicMsg;

// --- TopicActor Implementation ---

pub struct TopicActor<A>
where
    A: CloneableStreamable + 'static, // Ensure A is 'static for actor state
{
    subscribers: HashMap<Uuid, Recipient<StreamMessage<A>>>,
    _phantom_a: PhantomData<A>,
    closed: bool,
}

impl<A> TopicActor<A>
where
    A: CloneableStreamable + 'static,
{
    pub fn new() -> Self {
        TopicActor {
            subscribers: HashMap::new(),
            _phantom_a: PhantomData,
            closed: false,
        }
    }
}

impl<A> Default for TopicActor<A>
where
    A: CloneableStreamable + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A> Actor for TopicActor<A>
where
    A: CloneableStreamable + 'static,
{
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("TopicActor started");
    }

    fn stopping(&mut self, _ctx: &mut Self::Context) -> Running {
        // log::debug!("TopicActor stopping. Notifying subscribers.");
        self.closed = true;
        for (_, sub) in self.subscribers.iter() {
            let _ = sub.try_send(StreamMessage::End);
        }
        self.subscribers.clear();
        Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // log::debug!("TopicActor stopped.");
    }
}

impl<A> Handler<PublishItem<A>> for TopicActor<A>
where
    A: CloneableStreamable + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: PublishItem<A>, _ctx: &mut Context<Self>) {
        if self.closed {
            return;
        }
        // log::debug!("TopicActor handling PublishItem: {:?}", msg.item);
        let mut failed_sends = Vec::new();
        for (id, sub) in self.subscribers.iter() {
            if sub
                .try_send(StreamMessage::Element(msg.item.clone()))
                .is_err()
            {
                failed_sends.push(*id);
            }
        }
        // Remove subscribers that failed to receive (likely closed)
        for id in failed_sends {
            self.subscribers.remove(&id);
        }
    }
}

impl<A> Handler<SubscribeMsg<A>> for TopicActor<A>
where
    A: CloneableStreamable + 'static,
{
    type Result = Result<Uuid, ()>;

    fn handle(&mut self, msg: SubscribeMsg<A>, _ctx: &mut Context<Self>) -> Self::Result {
        if self.closed {
            // log::warn!("Attempt to subscribe to a closed topic.");
            return Err(());
        }
        let id = Uuid::new_v4();
        // log::debug!("TopicActor handling SubscribeMsg. New subscriber ID: {}", id);
        self.subscribers.insert(id, msg.recipient);
        Ok(id)
    }
}

impl<A> Handler<CloseTopicMsg> for TopicActor<A>
where
    A: CloneableStreamable + 'static,
{
    type Result = ();

    fn handle(&mut self, _msg: CloseTopicMsg, ctx: &mut Context<Self>) {
        // log::debug!("TopicActor handling CloseTopicMsg. Stopping actor.");
        ctx.stop(); // This will trigger stopping()
    }
}

// --- Public Topic struct ---

#[derive(Clone)]
pub struct Topic<A>
where
    A: CloneableStreamable + 'static, // Ensure A is 'static if Topic contains Addr<TopicActor<A>>
{
    actor_addr: Addr<TopicActor<A>>,
}

impl<A> Topic<A>
where
    A: CloneableStreamable + 'static, // 'static needed for spawn_local
{
    pub fn new() -> Self {
        Topic {
            actor_addr: TopicActor::new().start(),
        }
    }

    pub fn publish(&self, item: A) {
        self.actor_addr.do_send(PublishItem { item });
    }

    pub fn subscribe(&self) -> Stream<A> {
        // Clone Addr for the task::spawn_local. Addr is !Send.
        let actor_addr_for_local_task = self.actor_addr.clone();

        // This outer closure is the `SetupFn`. It must be `Send`.
        // It captures `actor_addr_for_local_task`.
        // If Addr<...> is !Send, this makes the closure !Send, violating SetupFn's Send bound.
        // This is the fundamental conflict if `SetupFn` demands `Send` for the closure.
        // Assuming for this attempt that the compiler is allowing this due to some nuance,
        // or that the `spawn_local` strategy for the *future* is the primary concern for runtime.
        let setup_fn_closure = Box::new(
            move |downstream_recipient: Recipient<StreamMessage<A>>| {
                // This closure must be Send. It captures actor_addr_for_local_task.
                // This line makes the closure !Send if Addr is !Send.
                // If compilation passes, it means the compiler doesn't see this as a Send violation
                // for the SetupFn trait object, or SetupFn was relaxed.

                // Channel to get the result from the local !Send task back to the Send future.
                let (tx_oneshot, rx_oneshot) = oneshot::channel::<Result<(), String>>();
                
                // Clone the Addr again for the spawned task
                let task_actor_addr_clone = actor_addr_for_local_task.clone();

                // The !Send work (Actix actor interaction) is done in a spawned local task.
                // This requires that Topic::subscribe() is called from within a LocalSet context.
                task::spawn_local(async move {
                    // downstream_recipient (Recipient) is !Send and is moved here.
                    // task_actor_addr_clone (Addr) is !Send and is moved here.
                    match task_actor_addr_clone
                        .send(SubscribeMsg {
                            recipient: downstream_recipient,
                        })
                        .await
                    {
                        Ok(Ok(_uuid)) => {
                            let _ = tx_oneshot.send(Ok(()));
                        }
                        Ok(Err(())) => {
                            let _ = tx_oneshot.send(Err(String::from("Subscription failed: Topic actor reported an internal error.")));
                        }
                        Err(_e) => {
                            // MailboxError means the actor is gone or queue is full.
                            let _ = tx_oneshot.send(Err(String::from("Subscription failed: Could not send message to Topic actor (MailboxError).")));
                        }
                    }
                });

                // The future returned by setup_fn must be Send.
                // It only awaits the oneshot::Receiver `rx_oneshot`, which is Send.
                async move {
                    match rx_oneshot.await {
                        Ok(Ok(())) => Ok(()), // Task sent Ok(())
                        Ok(Err(e)) => Err(e), // Task sent Err(String)
                        Err(_recv_error) => Err(String::from("Subscription channel closed unexpectedly; task may have panicked.")), // Oneshot sender dropped
                    }
                }
                .boxed() // Creates the BoxFuture (which is Send)
            },
        );

        Stream {
            setup_fn: setup_fn_closure,
            _phantom: PhantomData,
        }
    }

    pub fn close(&self) {
        self.actor_addr.do_send(CloseTopicMsg);
    }
}

impl<A> Default for Topic<A>
where
    A: CloneableStreamable + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}