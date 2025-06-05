use crate::stream::StreamMessage;
use actix::*;
use std::fmt::Debug;

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct InnerStreamCompletedMsg;

#[derive(Message)]
#[rtype(result = "()")]
pub(crate) struct InnerStreamItemMsg<NewOut: 'static + Send + Unpin + Debug + Clone> {
    pub(crate) item: NewOut,
}

pub(crate) struct InnerStreamProxyActor<NewOut>
where
    NewOut: 'static + Send + Unpin + Debug + Clone,
{
    concat_map_items_recipient: Recipient<InnerStreamItemMsg<NewOut>>,
    concat_map_completion_recipient: Recipient<InnerStreamCompletedMsg>,
}

impl<NewOut> InnerStreamProxyActor<NewOut>
where
    NewOut: 'static + Send + Unpin + Debug + Clone,
{
    pub(crate) fn new(
        items_recipient: Recipient<InnerStreamItemMsg<NewOut>>,
        completion_recipient: Recipient<InnerStreamCompletedMsg>,
    ) -> Self {
        Self {
            concat_map_items_recipient: items_recipient,
            concat_map_completion_recipient: completion_recipient,
        }
    }
}

impl<NewOut> Actor for InnerStreamProxyActor<NewOut>
where
    NewOut: 'static + Send + Unpin + Debug + Clone,
{
    type Context = Context<Self>;

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        // Notify the ConcatMapActor that this inner stream's proxy has stopped.
        // This is crucial for ConcatMapActor to know when an inner stream has finished.
        let _ = self
            .concat_map_completion_recipient
            .do_send(InnerStreamCompletedMsg);
    }
}

impl<NewOut> Handler<StreamMessage<NewOut>> for InnerStreamProxyActor<NewOut>
where
    NewOut: 'static + Send + Unpin + Debug + Clone,
{
    type Result = ();

    fn handle(&mut self, msg: StreamMessage<NewOut>, ctx: &mut Context<Self>) {
        match msg {
            StreamMessage::Element(item) => {
                // Forward the item to the ConcatMapActor
                if self
                    .concat_map_items_recipient
                    .try_send(InnerStreamItemMsg { item })
                    .is_err()
                {
                    // If the ConcatMapActor is no longer receiving, this proxy can stop.
                    // This might happen if the main stream is dropped or encounters an error.
                    ctx.stop();
                }
            }
            StreamMessage::End => {
                // The inner stream has ended. The proxy's job for this stream is done.
                // It will stop, and its `stopped` method will notify ConcatMapActor.
                ctx.stop();
            }
        }
    }
}
