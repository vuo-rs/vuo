use crate::stream::{Stream, StreamMessage, Streamable};
use actix::prelude::*;
use futures::FutureExt;
use std::fmt; // Added for fmt::Debug
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

// Internal messages for SignalActor
struct GetValue<A>(PhantomData<A>)
where
    A: 'static + Send + Clone + Unpin + fmt::Debug;

impl<A> Message for GetValue<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    type Result = Option<A>;
}

struct SetValue<A>(A)
where
    A: 'static + Send + Clone + Unpin + fmt::Debug;

impl<A> Message for SetValue<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    type Result = ();
}

struct SubscribeDiscrete<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    recipient: Recipient<StreamMessage<A>>,
}

impl<A> Message for SubscribeDiscrete<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    type Result = ();
}

// SignalActor: Holds the current value and manages discrete updates to subscribers.
struct SignalActor<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    current_value: Arc<Mutex<Option<A>>>,
    subscribers: Vec<Recipient<StreamMessage<A>>>,
}

impl<A> SignalActor<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    fn new(initial_value: Option<A>) -> Self {
        SignalActor {
            current_value: Arc::new(Mutex::new(initial_value)),
            subscribers: Vec::new(),
        }
    }
}

impl<A> Actor for SignalActor<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    type Context = Context<Self>;
}

impl<A> Handler<GetValue<A>> for SignalActor<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    type Result = Option<A>;

    fn handle(&mut self, _msg: GetValue<A>, _ctx: &mut Context<Self>) -> Self::Result {
        self.current_value.lock().unwrap().clone()
    }
}

impl<A> Handler<SetValue<A>> for SignalActor<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    type Result = ();

    fn handle(&mut self, msg: SetValue<A>, _ctx: &mut Context<Self>) -> Self::Result {
        let mut value_guard = self.current_value.lock().unwrap();
        *value_guard = Some(msg.0.clone());

        // Notify subscribers
        self.subscribers
            .retain(|sub| sub.try_send(StreamMessage::Element(msg.0.clone())).is_ok());
    }
}

impl<A> Handler<SubscribeDiscrete<A>> for SignalActor<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    type Result = ();

    fn handle(&mut self, msg: SubscribeDiscrete<A>, _ctx: &mut Context<Self>) -> Self::Result {
        // Send current value immediately if available
        if let Some(val) = self.current_value.lock().unwrap().as_ref() {
            let _ = msg.recipient.try_send(StreamMessage::Element(val.clone()));
        }
        self.subscribers.push(msg.recipient);
    }
}

// Public Signal struct
#[derive(Clone)]
pub struct Signal<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug,
{
    actor_addr: Addr<SignalActor<A>>,
}

impl<A> Signal<A>
where
    A: 'static + Send + Clone + Unpin + fmt::Debug + Streamable, // Streamable for discrete()
{
    pub fn new(initial_value: A) -> Self {
        Signal {
            actor_addr: SignalActor::new(Some(initial_value)).start(),
        }
    }

    pub fn constant(value: A) -> Self {
        Signal::new(value)
    }

    pub fn empty() -> Self {
        Signal {
            actor_addr: SignalActor::new(None).start(),
        }
    }

    pub async fn get_value(&self) -> Option<A> {
        self.actor_addr
            .send(GetValue(PhantomData))
            .await
            .unwrap_or(None)
    }

    pub fn set_value(&self, value: A) {
        self.actor_addr.do_send(SetValue(value));
    }

    // Returns a stream that emits the current value and then all subsequent changes.
    pub fn discrete(&self) -> Stream<A> {
        let actor_addr_clone = self.actor_addr.clone();
        let setup_fn = Box::new(move |downstream_recipient: Recipient<StreamMessage<A>>| {
            async move {
                actor_addr_clone.do_send(SubscribeDiscrete {
                    recipient: downstream_recipient,
                });
                Ok(())
            }
            .boxed() // Ensure .boxed() is available via futures::FutureExt
        });
        Stream {
            setup_fn,
            _phantom: PhantomData,
        }
    }
}
