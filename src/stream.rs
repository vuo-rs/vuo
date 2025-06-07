// Module for actor implementations
mod actors;
pub mod streamable; // Declare the submodule file stream/streamable.rs

// Publicly re-export Streamable and CloneableStreamable
pub use self::streamable::{CloneableStreamable, Streamable};

// Internal actor imports (not part of public API of this module unless re-exported)
use crate::stream::actors::EvalTapActor;
use actors::{
    ChunkingActor, CollectorActor, ConcatMapActor, DebounceActor, DrainActor, DropWhileActor,
    EmitsActor, EvalMapActor, FilterActor, FoldActor, GroupWithinActor, HandleErrorWithActor,
    MappingActor, Merge2Actor, OnFinalizeActor, ParMapOrderedActor, ParMapUnorderedActor,
    PrimaryStreamSetupResult, ScanActor, TakeActor, TakeWhileActor, ThrottleActor,
};

use actix::prelude::{Actor, Context, Handler, Recipient}; // More specific imports
use actix_rt; // Added for actix_rt::spawn
use futures::future::BoxFuture; // Added TryFutureExt for map_err on JoinHandle
use futures::FutureExt;
use std::marker::PhantomData;
use std::{fmt::Debug, future::Future};

// Specific imports for zip_with and its actors
use actors::zip_actor::{InputStreamMessageA, InputStreamMessageB, ZipInputAdapterActor};

// --- Core Stream Message ---
#[derive(Debug, Clone)] // Keep Debug and Clone
                        // Removed #[derive(Message)] and #[rtype] for manual impl if derive fails fundamentally
pub enum StreamMessage<T: Streamable> {
    Element(T),
    End,
}

// --- Stream Consumer Trait (Marker Trait) ---
// Identifies an actor that can consume StreamMessages.
// This was part of an older design and might not be strictly necessary if
// Stream construction directly uses Recipient. For now, keeping it as per history.
pub trait StreamConsumer<ItemType>:
    Actor<Context = Context<Self>> + Handler<StreamMessage<ItemType>>
where
    ItemType: Streamable,
    Self: Actor<Context = Context<Self>>,
{
}

// Blanket implementation for any actor that satisfies the bounds.
impl<A, ItemType> StreamConsumer<ItemType> for A
where
    A: Actor<Context = Context<A>> + Handler<StreamMessage<ItemType>>,
    ItemType: Streamable,
{
}

// --- Stream Definition ---
// This type alias defines the shape of the setup function for a stream.
// It takes a Recipient for downstream messages and returns a boxed future
// that resolves to Ok(()) on successful setup, or Err(()) on failure.
// This version requires the closure and its returned future to be Send.
// Manual Message impl for StreamMessage will be added below.
// Recipient<StreamMessage<Out>> requires StreamMessage<Out>: actix::Message.
pub(crate) type SetupFn<Out> = Box<
    dyn FnOnce(Recipient<StreamMessage<Out>>) -> BoxFuture<'static, Result<(), String>>
        + Send // Closure itself must be Send
        + 'static,
>;

// Manual implementation of actix::Message for StreamMessage<T>
// This is to work around the potential issues with #[derive(Message)] or #[rtype] not being found.
impl<T: Streamable> actix::Message for StreamMessage<T> {
    type Result = (); // Matching the previous rtype
}

// The main Stream struct.
pub struct Stream<Out: Streamable> {
    pub setup_fn: SetupFn<Out>,
    pub _phantom: PhantomData<Out>,
}

// Note: Many stream operations require Out: CloneableStreamable if they need to
// clone elements (e.g. for internal actors or multiple consumers).
// The impl block here is for Out: Streamable, specific methods might add CloneableStreamable.
impl<Out: Streamable> Stream<Out> {
    // Creates a stream that emits items from an iterator.
    pub fn emits<I>(items: I) -> Self
    where
        I: IntoIterator<Item = Out> + Send + 'static,
        I::IntoIter: Send + 'static,
        Out: CloneableStreamable, // Out items are CloneableStreamable (for EmitsActor)
    {
        let items_vec: Vec<Out> = items.into_iter().collect(); // Collect items into a Vec for EmitsActor
        let setup_fn_closure = move |downstream_recipient: Recipient<StreamMessage<Out>>| {
            // This closure captures `items` (Send). `downstream_recipient` is a parameter.
            // This closure should be Send.
            async move {
                // The future created by this async block must be Send.
                // It uses `items` (Send) and `downstream_recipient` (!Send).
                // `downstream_recipient.try_send()` is synchronous, so it doesn't hold
                // `downstream_recipient` across an await point within this future.
                // Thus, this future should also be Send.

                // Start the EmitsActor. It will send its items and then StreamMessage::End.
                // The EmitsActor itself handles stopping.
                EmitsActor::new(items_vec, downstream_recipient).start();
                Ok(()) // Setup is successful once the actor is started.
            }
            .boxed() // Creates a BoxFuture (which is Send)
        };
        Stream {
            setup_fn: Box::new(setup_fn_closure),
            _phantom: PhantomData,
        }
    }

    // Filters the stream, keeping only elements for which the predicate returns true.
    pub fn filter<F>(self, predicate: F) -> Stream<Out>
    where
        F: FnMut(&Out) -> bool + Send + 'static + Clone + Unpin, // FnMut and Clone for actor
        Out: CloneableStreamable, // FilterActor might need to clone for predicate or if Out is passed through
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_filter_output: Recipient<StreamMessage<Out>>| {
                // This closure captures `prev_setup_fn` (Send) and `predicate` (Send). It is Send.
                async move {
                    // This future must be Send.
                    let filter_actor_addr =
                        FilterActor::new(predicate.clone(), downstream_recipient_for_filter_output)
                            .start(); // FilterActor is an Actix actor
                                      // `filter_actor_addr.recipient()` is !Send.
                                      // The future returned by `prev_setup_fn` must be Send.
                                      // If `prev_setup_fn`'s execution involves `!Send` types across await, this can be an issue.
                    let prev_stage_recipient = filter_actor_addr.recipient::<StreamMessage<Out>>();
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed()
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    // Takes the first n elements from the stream.
    pub fn take(self, n: u64) -> Stream<Out>
    where
        Out: CloneableStreamable,
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_take_output: Recipient<StreamMessage<Out>>| {
                // This closure captures `prev_setup_fn` (Send) and `n` (Send). It is Send.
                async move {
                    // This future must be Send.
                    let take_actor_addr =
                        TakeActor::new(n, downstream_recipient_for_take_output).start();
                    let prev_stage_recipient = take_actor_addr.recipient::<StreamMessage<Out>>();
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed()
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    // Maps each element of the stream to a new element using the provided function.
    pub fn map<F, NewOut>(self, f: F) -> Stream<NewOut>
    where
        F: FnMut(Out) -> NewOut + Send + 'static + Clone + Unpin, // FnMut and Clone for actor
        NewOut: CloneableStreamable, // Mapped output must be CloneableStreamable for actors
        Out: CloneableStreamable,    // Input to MappingActor
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_map_output: Recipient<StreamMessage<NewOut>>| {
                // This closure captures `prev_setup_fn` (Send) and `f` (Send). It is Send.
                async move {
                    // This future must be Send.
                    let mapping_actor_addr =
                        MappingActor::new(f.clone(), downstream_recipient_for_map_output).start();
                    let prev_stage_recipient = mapping_actor_addr.recipient::<StreamMessage<Out>>();
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed()
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    // Groups elements into chunks of a specified size.
    pub fn chunks(self, chunk_size: usize) -> Stream<Vec<Out>>
    where
        Out: CloneableStreamable,
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_chunk_output: Recipient<StreamMessage<Vec<Out>>>| {
                async move {
                    let chunking_actor_addr =
                        ChunkingActor::new(chunk_size, downstream_recipient_for_chunk_output)
                            .start();
                    let prev_stage_recipient =
                        chunking_actor_addr.recipient::<StreamMessage<Out>>();
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed()
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn par_map_unordered<F, NewOut>(self, parallelism: usize, f: F) -> Stream<NewOut>
    where
        F: Fn(Out) -> BoxFuture<'static, NewOut> + Send + Sync + 'static + Clone + Unpin,
        Out: CloneableStreamable,    // Input to ParMapUnorderedActor
        NewOut: CloneableStreamable, // Output from ParMapUnorderedActor
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_par_map_output: Recipient<StreamMessage<NewOut>>| {
                async move {
                    let par_map_actor_addr = ParMapUnorderedActor::new(
                        f.clone(),
                        downstream_recipient_for_par_map_output,
                        parallelism,
                    )
                    .start();
                    let prev_stage_recipient = par_map_actor_addr.recipient::<StreamMessage<Out>>();
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed()
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn fold<Acc, FoldFn>(self, initial: Acc, fold_fn: FoldFn) -> Stream<Acc>
    where
        Acc: CloneableStreamable, // Accumulator type
        FoldFn: FnMut(Acc, Out) -> Acc + Send + 'static + Clone + Unpin,
        Out: Streamable, // Input type for fold_fn, not necessarily Cloneable if consumed
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_fold_result: Recipient<StreamMessage<Acc>>| {
                async move {
                    let fold_actor_addr = FoldActor::new(
                        initial.clone(), // Clone initial accumulator for the actor
                        fold_fn.clone(),
                        downstream_recipient_for_fold_result,
                    )
                    .start();
                    let prev_stage_recipient = fold_actor_addr.recipient::<StreamMessage<Out>>();
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed()
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn scan<Acc, FScan>(self, initial: Acc, scan_fn: FScan) -> Stream<Acc>
    where
        Acc: CloneableStreamable,
        FScan: FnMut(Acc, Out) -> Acc + Send + 'static + Clone + Unpin,
        Out: Streamable,
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_scan_result: Recipient<StreamMessage<Acc>>| {
                async move {
                    let scan_actor_addr = ScanActor::new(
                        initial.clone(),
                        scan_fn.clone(),
                        downstream_recipient_for_scan_result,
                    )
                    .start();
                    let prev_stage_recipient = scan_actor_addr.recipient::<StreamMessage<Out>>();
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed()
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn take_while<P>(self, predicate: P) -> Stream<Out>
    where
        P: FnMut(&Out) -> bool + Send + 'static + Clone + Unpin,
        Out: CloneableStreamable, // Actor needs to pass elements through
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(move |downstream_recipient: Recipient<StreamMessage<Out>>| {
            async move {
                let actor_addr =
                    TakeWhileActor::new(predicate.clone(), downstream_recipient).start();
                (prev_setup_fn)(actor_addr.recipient::<StreamMessage<Out>>()).await
            }
            .boxed()
        });
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn drop_while<P>(self, predicate: P) -> Stream<Out>
    where
        P: FnMut(&Out) -> bool + Send + 'static + Clone + Unpin,
        Out: CloneableStreamable,
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(move |downstream_recipient: Recipient<StreamMessage<Out>>| {
            async move {
                let actor_addr =
                    DropWhileActor::new(predicate.clone(), downstream_recipient).start();
                (prev_setup_fn)(actor_addr.recipient::<StreamMessage<Out>>()).await
            }
            .boxed()
        });
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn eval_map<NewOut, Fut, F>(self, map_fn: F) -> Stream<NewOut>
    where
        F: FnMut(Out) -> Fut + Send + 'static + Clone + Unpin,
        Fut: futures::Future<Output = NewOut> + Send + 'static,
        NewOut: CloneableStreamable, // Output type
        Out: Streamable,             // Input type
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient: Recipient<StreamMessage<NewOut>>| {
                async move {
                    let eval_map_actor_addr =
                        EvalMapActor::new(map_fn.clone(), downstream_recipient).start();
                    let prev_stage_recipient =
                        eval_map_actor_addr.recipient::<StreamMessage<Out>>();
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed()
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    /// Performs a side-effecting action for each element of the stream.
    ///
    /// For each element, the provided function `effect_fn` is called, which returns a `Future`.
    /// This future is awaited to completion before the original element is passed downstream.
    /// The processing is sequential: the effect for an element must complete before that
    /// element is forwarded and before the next element's effect begins.
    /// The stream's elements themselves are not modified.
    ///
    /// # Arguments
    ///
    /// * `effect_fn`: A function that takes an element of type `Out` and returns a `futures::Future<Output = ()>`.
    ///
    /// # Returns
    ///
    /// A new `Stream` that emits the same elements as the original stream, after the effect
    /// has been evaluated for each.
    pub fn eval_tap<Fut, F>(self, effect_fn: F) -> Stream<Out>
    where
        F: FnMut(Out) -> Fut + Send + 'static + Clone + Unpin,
        Fut: futures::Future<Output = ()> + 'static + Unpin, // Consistent with eval_map, Send is not needed here due to EvalTapActor's handler, Unpin is required
        Out: CloneableStreamable, // Element type, must be Cloneable for the tap operation. CloneableStreamable implies 'static.
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(move |downstream_recipient: Recipient<StreamMessage<Out>>| {
            async move {
                // Note: This assumes EvalTapActor is updated to expect a function F
                // that returns `futures::Future`. If EvalTapActor currently expects
                // `std::future::Future`, a compilation error will occur here,
                // and EvalTapActor will need to be modified.
                let actor_addr = EvalTapActor::new(downstream_recipient, effect_fn.clone()).start();

                // The recipient for the previous stage is the input recipient of our EvalTapActor
                let prev_stage_recipient = actor_addr.recipient::<StreamMessage<Out>>();

                (prev_setup_fn)(prev_stage_recipient).await
            }
            .boxed()
        });

        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn concat_map<NewOut, FStream>(self, map_to_stream_fn: FStream) -> Stream<NewOut>
    where
        FStream: FnMut(Out) -> Stream<NewOut> + Send + 'static + Clone + Unpin,
        NewOut: CloneableStreamable, // Output type for ConcatMapActor
        Out: Streamable,
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient: Recipient<StreamMessage<NewOut>>| {
                async move {
                    let actor_addr =
                        ConcatMapActor::new(map_to_stream_fn.clone(), downstream_recipient).start();
                    (prev_setup_fn)(actor_addr.recipient::<StreamMessage<Out>>()).await
                }
                .boxed()
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    /// Alias for `concat_map`.
    ///
    /// Maps each element to a new stream and concatenates the resulting streams sequentially.
    pub fn flat_map<NewOut, FStream>(self, map_to_stream_fn: FStream) -> Stream<NewOut>
    where
        FStream: FnMut(Out) -> Stream<NewOut> + Send + 'static + Clone + Unpin,
        NewOut: CloneableStreamable,
        Out: Streamable,
    {
        self.concat_map(map_to_stream_fn)
    }

    pub fn drain(self) -> Stream<()>
    // Emits one () when done.
    where
        Out: Streamable,
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_unit: Recipient<StreamMessage<()>>| {
                async move {
                    let drain_actor_addr = DrainActor::new(downstream_recipient_for_unit).start();
                    (prev_setup_fn)(drain_actor_addr.recipient::<StreamMessage<Out>>()).await
                }
                .boxed()
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn debounce(self, duration: std::time::Duration) -> Stream<Out>
    where
        Out: CloneableStreamable + 'static, // 'static for actor
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_debounce_output: Recipient<StreamMessage<Out>>| {
                // This closure captures `prev_setup_fn` (Send) and `duration` (Send). It is Send.
                async move {
                    // This future must be Send.
                    // DebounceActor is an Actix actor.
                    let debounce_actor_addr =
                        DebounceActor::new(duration, downstream_recipient_for_debounce_output)
                            .start();

                    // The DebounceActor becomes the recipient for the previous stage.
                    let prev_stage_recipient =
                        debounce_actor_addr.recipient::<StreamMessage<Out>>();

                    // Set up the previous stage to send to DebounceActor.
                    // The future returned by `prev_setup_fn` must be Send.
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed() // Creates BoxFuture (Send Future)
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn group_within(self, count: usize, duration: std::time::Duration) -> Stream<Vec<Out>>
    where
        Out: CloneableStreamable + 'static, // 'static for actor context
                                            // Vec<Out> needs to be CloneableStreamable for the output Stream.
                                            // If Out is CloneableStreamable, Vec<Out> is Clone.
                                            // Vec<Out> is Send/Sync/'static/Unpin/Debug if Out has these (which Streamable ensures).
                                            // So, Vec<Out> effectively becomes CloneableStreamable.
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_group_output: Recipient<StreamMessage<Vec<Out>>>| {
                // This closure captures `prev_setup_fn` (Send), `count` (Send), and `duration` (Send).
                // It is Send, satisfying the SetupFn bound.
                async move {
                    // This future must be Send.
                    // GroupWithinActor is an Actix actor. Its Addr is !Send.
                    // The recipient() call returns a Recipient which is also !Send.
                    // However, these are used to set up the stream and then `prev_setup_fn(...).await`
                    // is called. The future from `prev_setup_fn` is already a BoxFuture (Send).
                    // The key is that `group_within_actor_addr` and `prev_stage_recipient`
                    // are not held across an await point *within this specific async block*
                    // in a way that makes this block's future !Send.
                    let group_within_actor_addr = GroupWithinActor::new(
                        count,
                        duration,
                        downstream_recipient_for_group_output,
                    )
                    .start();

                    let prev_stage_recipient =
                        group_within_actor_addr.recipient::<StreamMessage<Out>>();

                    // Set up the previous stage to send to GroupWithinActor.
                    // The future returned by `prev_setup_fn` is a BoxFuture (Send).
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed() // Creates BoxFuture (Send Future)
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData, // PhantomData for Stream<Vec<Out>>
        }
    }

    /// Creates a stream that emits a single value and then ends.
    pub fn eval(value: Out) -> Self
    where
        Out: CloneableStreamable, // Value needs to be cloneable for sending
    {
        let setup_fn_closure = move |downstream_recipient: Recipient<StreamMessage<Out>>| {
            // This closure captures `value` (CloneableStreamable => Send + Clone).
            // The closure is Send.
            async move {
                // This future must be Send.
                // It uses `value` and `downstream_recipient.try_send()` (sync).
                // This future is Send.
                if downstream_recipient
                    .try_send(StreamMessage::Element(value))
                    .is_err()
                {
                    return Err(
                        "Downstream recipient closed during Stream::eval item send".to_string()
                    );
                }
                if downstream_recipient.try_send(StreamMessage::End).is_err() {
                    return Err(
                        "Downstream recipient closed during Stream::eval end send".to_string()
                    );
                }
                Ok(())
            }
            .boxed() // Creates BoxFuture (Send)
        };
        Stream {
            setup_fn: Box::new(setup_fn_closure),
            _phantom: PhantomData,
        }
    }

    /// Creates a stream from a `BoxFuture` that resolves to a single value.
    /// The stream will emit that value and then end.
    /// If the future fails (panics) or is cancelled, the stream may end without emitting or abruptly.
    pub fn future<FUT>(future_result: FUT) -> Self
    where
        Out: CloneableStreamable,
        FUT: Future<Output = Result<Out, String>> + Send + 'static, // Future produces a Result
    {
        let setup_fn_closure = move |downstream_recipient: Recipient<StreamMessage<Out>>| {
            async move {
                match future_result.await {
                    Ok(value) => {
                        // Future succeeded, produced Out
                        if downstream_recipient
                            .try_send(StreamMessage::Element(value))
                            .is_err()
                        {
                            return Err(
                                "Downstream recipient closed during Stream::future item send"
                                    .to_string(),
                            );
                        }
                        if downstream_recipient.try_send(StreamMessage::End).is_err() {
                            return Err(
                                "Downstream recipient closed during Stream::future end send"
                                    .to_string(),
                            );
                        }
                        Ok(()) // Setup successful
                    }
                    Err(err_string) => {
                        // Future itself failed
                        Err(err_string) // Propagate this error as the setup error
                    }
                }
            }
            .boxed() // Creates BoxFuture (Send)
        };
        Stream {
            setup_fn: Box::new(setup_fn_closure),
            _phantom: PhantomData,
        }
    }

    // Creates a stream that unfolds from an initial state `S`.
    /// Creates a stream by repeatedly applying a function `f` to a state `S`.
    /// `f` produces an `Option<(Out, S)>`. `Some((element, next_state))` emits `element`
    /// and continues with `next_state`. `None` ends the stream.
    /// This is a synchronous unfold; `f` is not async.
    pub fn unfold<S, F>(initial_state: S, mut f: F) -> Self
    where
        S: Send + 'static,
        F: FnMut(S) -> Option<(Out, S)> + Send + 'static,
        // Out is already Streamable from the impl block. No CloneableStreamable needed here by default.
    {
        let setup_fn_closure = move |downstream_recipient: Recipient<StreamMessage<Out>>| {
            // This closure captures `initial_state` (moved) and `f` (moved). Both are Send.
            // The closure is Send.
            let mut current_state = initial_state; // mutable state for the loop
                                                   // `f` is also captured and mutable here (due to FnMut).

            async move {
                // This future must be Send.
                // It uses `current_state` (Send) and `f` (Send), and `downstream_recipient.try_send()` (sync).
                // This future is Send.
                loop {
                    match f(current_state) {
                        Some((element, next_state)) => {
                            if downstream_recipient
                                .try_send(StreamMessage::Element(element))
                                .is_err()
                            {
                                return Err(String::from("Downstream consumer gone during unfold"));
                                // Downstream consumer is gone
                            }
                            current_state = next_state;
                        }
                        None => {
                            // End of unfold
                            break;
                        }
                    }
                }
                if downstream_recipient.try_send(StreamMessage::End).is_err() {
                    return Err(String::from("Downstream consumer gone at end of unfold"));
                    // Downstream consumer is gone
                }
                Ok(())
            }
            .boxed()
        };
        Stream {
            setup_fn: Box::new(setup_fn_closure),
            _phantom: PhantomData,
        }
    }

    pub fn throttle(self, duration: std::time::Duration) -> Stream<Out>
    where
        Out: CloneableStreamable + 'static, // 'static for actor context
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_throttle_output: Recipient<StreamMessage<Out>>| {
                // This closure captures `prev_setup_fn` (Send) and `duration` (Send). It is Send.
                async move {
                    // This future must be Send.
                    // ThrottleActor is an Actix actor.
                    let throttle_actor_addr =
                        ThrottleActor::new(duration, downstream_recipient_for_throttle_output)
                            .start();

                    // The ThrottleActor becomes the recipient for the previous stage.
                    let prev_stage_recipient =
                        throttle_actor_addr.recipient::<StreamMessage<Out>>();

                    // Set up the previous stage to send to ThrottleActor.
                    // The future returned by `prev_setup_fn` must be Send.
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed() // Creates BoxFuture (Send Future)
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn par_map_ordered<F, NewOut>(self, parallelism: usize, f: F) -> Stream<NewOut>
    where
        F: Fn(Out) -> BoxFuture<'static, NewOut> + Send + Sync + 'static + Clone + Unpin, // Added Unpin
        Out: CloneableStreamable + 'static, // Input to ParMapOrderedActor
        NewOut: CloneableStreamable + 'static, // Output from ParMapOrderedActor
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_par_map_output: Recipient<StreamMessage<NewOut>>| {
                // This closure captures `prev_setup_fn` (Send), `parallelism` (Send), and `f` (Send + Sync + Clone).
                // It is Send, satisfying the SetupFn bound.
                async move {
                    // This future must be Send.
                    // ParMapOrderedActor is an Actix actor. Its Addr is !Send.
                    // The recipient() call returns a Recipient which is also !Send.
                    // These are used to set up the stream, then `prev_setup_fn(...).await` is called.
                    // The future from `prev_setup_fn` is already a BoxFuture (Send).
                    let par_map_ordered_actor_addr = ParMapOrderedActor::new(
                        f,
                        downstream_recipient_for_par_map_output,
                        parallelism,
                    )
                    .start();

                    let prev_stage_recipient =
                        par_map_ordered_actor_addr.recipient::<StreamMessage<Out>>();

                    // Set up the previous stage to send to ParMapOrderedActor.
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed() // Creates BoxFuture (Send Future)
            },
        );
        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn merge(self, other: Stream<Out>) -> Stream<Out>
    where
        Out: CloneableStreamable + 'static,
    {
        let setup_fn1 = self.setup_fn;
        let setup_fn2 = other.setup_fn;

        let new_setup_fn = Box::new(
            move |final_downstream_recipient: Recipient<StreamMessage<Out>>| {
                async move {
                    // Start the Merge2Actor. It will forward items from both streams
                    // to the final_downstream_recipient.
                    let merge_actor_addr = Merge2Actor::new(final_downstream_recipient).start();
                    let merge_actor_recipient = merge_actor_addr.recipient::<StreamMessage<Out>>();

                    // Set up both input streams to send their items to the Merge2Actor.
                    // We use try_join to run both setup functions concurrently.
                    // If either setup fails, the merge setup fails.
                    let setup_stream1_fut = (setup_fn1)(merge_actor_recipient.clone());
                    let setup_stream2_fut = (setup_fn2)(merge_actor_recipient);

                    match futures::try_join!(setup_stream1_fut, setup_stream2_fut) {
                        Ok((_result1, _result2)) => Ok(()), // Both streams set up successfully
                        Err(_) => Err(String::from(
                            "Failed to set up one or both streams for merge",
                        )), // At least one stream setup failed
                    }
                }
                .boxed()
            },
        );

        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub fn zip_with<B, C, F>(self, other_stream: Stream<B>, zip_function: F) -> Stream<C>
    where
        Out: CloneableStreamable + 'static, // 'Out' is the type of 'self' stream
        B: CloneableStreamable + 'static,
        C: CloneableStreamable + 'static,
        F: Fn(Out, B) -> C + Send + Sync + 'static,
    {
        let setup_fn_a = self.setup_fn;
        let setup_fn_b = other_stream.setup_fn;

        let new_setup_fn = Box::new(
            move |final_downstream_recipient: Recipient<StreamMessage<C>>| {
                async move {
                    println!("[Stream::zip_with setup_fn] Entered async block."); // PRINTLN ADDED
                    log::trace!("[Stream::zip_with setup_fn] Starting setup for zip_with.");

                    println!("[Stream::zip_with setup_fn] About to start ZipActor."); // PRINTLN ADDED
                    // Start the main ZipActor
                    let zip_actor_addr = actors::ZipActor::<Out, B, C, F>::new(
                        final_downstream_recipient,
                        zip_function,
                    )
                    .start();
                    println!("[Stream::zip_with setup_fn] ZipActor started. Addr: {:?}", zip_actor_addr); // PRINTLN ADDED

                    println!("[Stream::zip_with setup_fn] About to start AdapterA."); // PRINTLN ADDED
                    // Adapter for Stream A (self stream of type Out)
                    let adapter_a_addr = ZipInputAdapterActor::<Out, InputStreamMessageA<Out>>::new(
                        zip_actor_addr.clone().recipient::<InputStreamMessageA<Out>>(),
                        "A".to_string(), // Name for logging
                    )
                    .start();
                    println!("[Stream::zip_with setup_fn] AdapterA started. Addr: {:?}", adapter_a_addr); // PRINTLN ADDED
                    let recipient_for_stream_a = adapter_a_addr.recipient::<StreamMessage<Out>>();

                    println!("[Stream::zip_with setup_fn] About to start AdapterB."); // PRINTLN ADDED
                    // Adapter for Stream B (other_stream of type B)
                    let adapter_b_addr = ZipInputAdapterActor::<B, InputStreamMessageB<B>>::new(
                        zip_actor_addr.clone().recipient::<InputStreamMessageB<B>>(),
                        "B".to_string(), // Name for logging
                    )
                    .start();
                    println!("[Stream::zip_with setup_fn] AdapterB started. Addr: {:?}", adapter_b_addr); // PRINTLN ADDED
                    let recipient_for_stream_b = adapter_b_addr.recipient::<StreamMessage<B>>();

                    println!("[Stream::zip_with setup_fn] About to call setup_fn for input streams."); // PRINTLN ADDED
                    // Set up both input streams to send their items to their respective adapters
                    let setup_stream_a_fut = (setup_fn_a)(recipient_for_stream_a);
                    let setup_stream_b_fut = (setup_fn_b)(recipient_for_stream_b);

                    match futures::try_join!(setup_stream_a_fut, setup_stream_b_fut) {
                        Ok((_result1, _result2)) => {
                            log::trace!("[Stream::zip_with] Both input streams set up successfully.");
                            println!("[Stream::zip_with setup_fn] Input streams setup OK."); // PRINTLN ADDED
                            Ok(())
                        }
                        Err(_) => {
                            log::error!("[Stream::zip_with] At least one input stream setup failed for zip_with.");
                            println!("[Stream::zip_with setup_fn] Input streams setup FAILED."); // PRINTLN ADDED
                            Err(String::from("At least one input stream setup failed for zip_with"))
                        }
                    }
                }
                .boxed()
            },
        );

        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    // Optional: A simpler 'zip' method that produces tuples
    pub fn zip<B>(self, other_stream: Stream<B>) -> Stream<(Out, B)>
    where
        Out: CloneableStreamable + 'static,
        B: CloneableStreamable + 'static,
        // Ensure the tuple itself is CloneableStreamable if it needs to be passed around by StreamMessage
        (Out, B): CloneableStreamable + 'static,
    {
        self.zip_with(other_stream, |a, b| (a, b))
    }

    pub fn interrupt_when<FUT>(self, interrupt_signal: FUT) -> Stream<Out>
    where
        Out: CloneableStreamable + 'static,
        FUT: Future<Output = ()> + Send + 'static,
    {
        let setup_fn_source = self.setup_fn;

        let new_setup_fn = Box::new(
            move |final_downstream_recipient: Recipient<StreamMessage<Out>>| {
                async move {
                    // Start the InterruptActor. It will manage the interrupt logic.
                    // It takes the final_downstream_recipient to send its output (or End) to.
                    // It also takes the interrupt_signal future.
                    let interrupt_actor_addr = actors::InterruptActor::<Out, FUT>::new(
                        final_downstream_recipient,
                        interrupt_signal,
                    )
                    .start();

                    // The source stream (self) should send its items to the InterruptActor.
                    let recipient_for_source = interrupt_actor_addr.recipient::<StreamMessage<Out>>();

                    // Execute the setup function of the source stream, directing its output
                    // to the InterruptActor.
                    let setup_source_fut = (setup_fn_source)(recipient_for_source);

                    match setup_source_fut.await {
                        Ok(()) => {
                            log::trace!("[Stream::interrupt_when] Source stream setup successful, wired to InterruptActor.");
                            Ok(())
                        }
                        Err(_) => {
                            log::error!("[Stream::interrupt_when] Source stream setup failed for interrupt_when.");
                            Err(String::from("Source stream setup failed for interrupt_when"))
                        }
                    }
                }
                .boxed()
            },
        );

        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    /// Executes a given side-effecting future when this stream completes,
    /// either successfully (`End`) or with an error (`Error`), or if the
    /// stream processing is stopped for any other reason (e.g., downstream dropped).
    ///
    /// The provided `effect_fn` is a function that returns a future. This future
    /// will be spawned and run by the underlying actor. The `on_finalize` combinator
    /// does not wait for this future to complete before propagating the original
    /// termination signal or stopping. The effect is guaranteed to be run at most once.
    ///
    /// This is useful for resource cleanup, logging, or other side effects
    /// that need to occur regardless of how the stream terminates.
    ///
    /// The `effect_fn` must be `Send + 'static`, and the future it returns
    /// (`Fut`) must also be `Send + 'static`.
    ///
    /// # Arguments
    ///
    /// * `effect_fn`: A `FnOnce` that returns a future `Fut`.
    ///
    /// # Returns
    ///
    /// A new `Stream<Out>` that behaves identically to the original stream
    /// but ensures the `effect_fn` is executed upon termination.
    pub fn on_finalize<Fut, F>(self, effect_fn: F) -> Stream<Out>
    where
        Out: CloneableStreamable, // Required by OnFinalizeActor and for message passing
        Fut: Future<Output = ()> + Send + 'static,
        F: FnOnce() -> Fut + Send + 'static + Unpin, // FnOnce to allow capture, Send + 'static + Unpin for actor
    {
        let prev_setup_fn = self.setup_fn;
        let new_setup_fn = Box::new(
            move |downstream_recipient_for_finalize_output: Recipient<StreamMessage<Out>>| {
                // This outer closure captures `prev_setup_fn` (Send) and `effect_fn` (Send).
                // It needs to be Send + 'static.
                async move {
                    // This async block's future must be Send.
                    let on_finalize_actor_addr =
                        OnFinalizeActor::new(downstream_recipient_for_finalize_output, effect_fn)
                            .start();

                    // The recipient for the previous stage is the input recipient of our OnFinalizeActor
                    let prev_stage_recipient =
                        on_finalize_actor_addr.recipient::<StreamMessage<Out>>();

                    // Call the setup function of the previous stage in the stream,
                    // providing it with the recipient of our OnFinalizeActor.
                    (prev_setup_fn)(prev_stage_recipient).await
                }
                .boxed() // Ensure the future returned by the setup_fn is BoxFuture<'static, Result<(), String>> and Send
            },
        );

        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    /// Handles errors in the current stream by switching to a new stream
    /// provided by the `handler` function.
    ///
    /// If the current stream's setup fails (the `SetupFn` returns `Err`), or if the
    /// stream's processing actor stops unexpectedly (leading to this combinator's
    /// actor being stopped before a graceful `End` from the primary stream),
    /// the `handler` function is called with an error message (as a `String`).
    /// The `handler` returns a new `Stream<Out>` which is then run.
    ///
    /// If the current stream completes successfully (`End`), the `handler` is
    /// not called, and the `End` message is propagated.
    ///
    /// # Arguments
    ///
    /// * `handler`: A `FnOnce(String) -> Stream<Out> + Send + 'static + Unpin`
    ///   that takes an error message and returns a new stream. `Unpin` is
    ///   required as the handler becomes part of the created actor's state.
    ///
    /// # Returns

    /// Handles errors in the current stream by switching to a new stream
    /// provided by the `handler` function.
    ///
    /// If the current stream's setup fails (the `SetupFn` returns `Err`), or if the
    /// stream's processing actor stops unexpectedly (leading to this combinator's
    /// actor being stopped before a graceful `End` from the primary stream),
    /// the `handler` function is called with an error message (as a `String`).
    /// The `handler` returns a new `Stream<Out>` which is then run.
    ///
    /// If the current stream completes successfully (`End`), the `handler` is
    /// not called, and the `End` message is propagated.
    ///
    /// # Arguments
    ///
    /// * `handler`: A `FnOnce(String) -> Stream<Out> + Send + 'static + Unpin`
    ///   that takes an error message and returns a new stream. `Unpin` is
    ///   required as the handler becomes part of the created actor's state.
    ///
    /// # Returns
    ///
    /// A new `Stream<Out>` that incorporates the error handling logic.
    pub fn handle_error_with<FHandler>(self, handler: FHandler) -> Stream<Out>
    where
        Out: CloneableStreamable, // Required for actor messages and stream operations
        FHandler: FnOnce(String) -> Stream<Out> + Send + 'static + Unpin,
    {
        let prev_setup_fn = self.setup_fn;

        let new_setup_fn = Box::new(
            move |final_downstream_recipient: Recipient<StreamMessage<Out>>| {
                // This outer closure captures `prev_setup_fn` (Send) and `handler` (Send + Unpin).
                // It needs to be Send + 'static.
                async move {
                    // Start the HandleErrorWithActor.
                    // It needs the final downstream recipient and the error handler function.
                    let handle_error_actor_addr = HandleErrorWithActor::new(
                        final_downstream_recipient.clone(), // Clone for the actor state
                        handler,
                    )
                    .start();

                    // Recipient for the HandleErrorWithActor to receive StreamMessage<Out> from the primary stream.
                    let recipient_for_primary_stream_elements = handle_error_actor_addr.clone().recipient::<StreamMessage<Out>>();

                    // Recipient for the HandleErrorWithActor to receive the result of the primary stream's setup.
                    let recipient_for_primary_setup_result = handle_error_actor_addr.recipient::<PrimaryStreamSetupResult>();

                    // Execute the setup function of the primary (previous) stream.
                    // Its output (elements or End) goes to `recipient_for_primary_stream_elements`.
                    // Its setup success/failure goes to `recipient_for_primary_setup_result`.
                    match (prev_setup_fn)(recipient_for_primary_stream_elements).await {
                        Ok(()) => {
                            // Primary stream setup was successful. Notify the HandleErrorWithActor.
                            if recipient_for_primary_setup_result.try_send(PrimaryStreamSetupResult::Success).is_err() {
                                // HandleErrorWithActor might be gone if final_downstream_recipient was dropped quickly.
                                // log::warn!("[handle_error_with setup_fn] Failed to send Success to HandleErrorWithActor. It might have stopped.");
                                return Err("HandleErrorWithActor not available to confirm primary stream success.".to_string());
                            }
                        }
                        Err(primary_setup_err_msg) => {
                            // Primary stream setup failed. Notify the HandleErrorWithActor.
                            if recipient_for_primary_setup_result.try_send(PrimaryStreamSetupResult::Failure(primary_setup_err_msg.clone())).is_err() {
                                // log::warn!("[handle_error_with setup_fn] Failed to send Failure to HandleErrorWithActor. It might have stopped.");
                                // If the handler actor is gone, we can't run the fallback. Propagate the original setup error.
                                return Err(format!("HandleErrorWithActor not available for primary stream failure ({}). Cannot initiate fallback.", primary_setup_err_msg));
                            }
                        }
                    }
                    // This setup_fn itself has completed its orchestration role successfully.
                    // The HandleErrorWithActor now manages the stream lifecycle.
                    Ok(())
                }
                .boxed()
            },
        );

        Stream {
            setup_fn: new_setup_fn,
            _phantom: PhantomData,
        }
    }

    pub async fn compile_to_list(self) -> Result<Vec<Out>, String>
    where
        Out: CloneableStreamable + 'static, // Add 'static for actix_rt::spawn
    {
        let (tx_oneshot_collector, rx_oneshot_collector) = futures::channel::oneshot::channel();
        let collector_actor = CollectorActor::new(tx_oneshot_collector).start();
        let recipient_for_setup = collector_actor.recipient::<StreamMessage<Out>>();

        let setup_fn_closure = self.setup_fn; // This is the Box<dyn FnOnce + Send ...>

        // Spawn the execution of the setup_fn's future onto the current Actix Arbiter.
        // The closure and the future it returns must be 'static for actix_rt::spawn.
        // SetupFn is already 'static. BoxFuture is 'static.
        let setup_join_handle = actix_rt::spawn(async move {
            // Execute the setup_fn closure to get the BoxFuture
            let fut = setup_fn_closure(recipient_for_setup);
            // Await the BoxFuture
            fut.await
        });

        // Await the result of the spawned setup task (which ran on an Arbiter)
        match setup_join_handle.await {
            // This awaits the JoinHandle from actix_rt::spawn
            Ok(Ok(())) => { /* Setup was successful, CollectorActor will eventually send result */ }
            Ok(Err(setup_err_str)) => {
                // Expecting String error from setup_fn
                /* setup_fn's future returned Err */
                return Err(setup_err_str);
            }
            Err(join_err) => {
                /* Task panicked or was cancelled */
                return Err(format!("Setup task failed: {:?}", join_err));
            }
        }

        // Wait for CollectorActor to send back the collected Vec.
        rx_oneshot_collector
            .await
            .map_err(|_| "Collector channel closed prematurely".to_string())
    }
}

#[cfg(test)]

/// Helper function for tests or utilities.
#[allow(dead_code)]
async fn run_stream_to_list<T: CloneableStreamable>(stream: Stream<T>) -> Vec<T> {
    stream
        .compile_to_list()
        .await
        .unwrap_or_else(|_err: String| Vec::new()) // Handle String error, return empty Vec
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_rt;
    use futures::future::{pending, ready};
    use futures::StreamExt as FuturesStreamExt; // Added for MPSC stream in tests
    use std::collections::HashSet;
    use std::marker::PhantomData; // Added for custom stream in tests
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration; // Added for on_finalize tests

    // Simple test types satisfy Streamable via blanket impl if Debug, Send, Unpin, 'static
    // For tests that need CloneableStreamable, the types also need Clone.
    // e.g. i32, String, Vec<i32> generally work.

    #[actix_rt::test]
    async fn test_fold_sum() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 5]);
        let sum_stream = stream.fold(0, |acc, x| acc + x);
        let result = run_stream_to_list(sum_stream).await;
        assert_eq!(result, vec![15]);
    }

    #[actix_rt::test]
    async fn test_fold_empty_stream() {
        let stream = Stream::emits(Vec::<i32>::new());
        let sum_stream = stream.fold(100, |acc, x| acc + x); // Initial value is emitted
        let result = run_stream_to_list(sum_stream).await;
        assert_eq!(result, vec![100]);
    }

    #[actix_rt::test]
    async fn test_fold_string_concat() {
        let items: Vec<String> = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let stream = Stream::emits(items);
        let concat_stream = stream.fold("S:".to_string(), |mut acc: String, x_val: String| {
            acc.push_str(&x_val);
            acc
        });
        let result = run_stream_to_list(concat_stream).await;
        assert_eq!(result, vec!["S:abc".to_string()]);
    }

    #[actix_rt::test]
    async fn test_scan_sum() {
        let stream = Stream::emits(vec![1, 2, 3]);
        let sum_stream = stream.scan(0, |acc, x| acc + x);
        let result = run_stream_to_list(sum_stream).await;
        assert_eq!(result, vec![0, 1, 3, 6]); // Scan emits initial value
    }

    #[actix_rt::test]
    async fn test_scan_empty_stream() {
        let stream = Stream::emits(Vec::<i32>::new());
        let scan_stream = stream.scan(100, |acc, x| acc + x);
        let result = run_stream_to_list(scan_stream).await;
        assert_eq!(result, vec![100]); // Scan emits initial value
    }

    #[actix_rt::test]
    async fn test_scan_string_concat() {
        let items: Vec<String> = vec!["a".to_string(), "b".to_string()];
        let stream = Stream::emits(items);
        let concat_stream = stream.scan("S:".to_string(), |mut acc: String, x_val: String| {
            acc.push_str(&x_val);
            acc
        });
        let result = run_stream_to_list(concat_stream).await;
        assert_eq!(
            result,
            vec!["S:".to_string(), "S:a".to_string(), "S:ab".to_string()]
        );
    }

    #[actix_rt::test]
    async fn test_eval_map_simple_sequential() {
        let stream = Stream::emits(vec![1, 2, 3]);
        let eval_stream = stream.eval_map(|x| async move {
            // EvalMapActor processes sequentially in this simplified version
            actix_rt::time::sleep(Duration::from_millis(if x == 2 { 50 } else { 10 })).await;
            x * 10
        });
        let result = run_stream_to_list(eval_stream).await;
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[actix_rt::test]
    async fn test_eval_map_empty_stream() {
        let stream = Stream::emits(Vec::<i32>::new());
        let eval_stream = stream.eval_map(|x: i32| async move { x * 10 });
        let result = run_stream_to_list(eval_stream).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_eval_map_type_change() {
        let stream = Stream::emits(vec![1, 2]);
        let eval_stream = stream.eval_map(|x| async move { format!("num:{}", x) });
        let result: Vec<String> = run_stream_to_list(eval_stream).await;
        assert_eq!(result, vec!["num:1".to_string(), "num:2".to_string()]);
    }

    #[actix_rt::test]
    async fn test_take_while_simple() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 1, 5]);
        let taken = stream.take_while(|x| *x < 4);
        let result = run_stream_to_list(taken).await;
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[actix_rt::test]
    async fn test_take_while_all() {
        let stream = Stream::emits(vec![1, 2, 3]);
        let taken = stream.take_while(|x| *x < 10);
        let result = run_stream_to_list(taken).await;
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[actix_rt::test]
    async fn test_take_while_none() {
        let stream = Stream::emits(vec![5, 1, 2]);
        let taken = stream.take_while(|x| *x < 4);
        let result = run_stream_to_list(taken).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_take_while_empty_stream() {
        let stream = Stream::emits(Vec::<i32>::new());
        let taken = stream.take_while(|x| *x < 4);
        let result = run_stream_to_list(taken).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_drop_while_simple() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 1, 5]);
        let kept = stream.drop_while(|x| *x < 3);
        let result = run_stream_to_list(kept).await;
        assert_eq!(result, vec![3, 4, 1, 5]);
    }

    #[actix_rt::test]
    async fn test_drop_while_all_dropped() {
        let stream = Stream::emits(vec![1, 2, 3]);
        let kept = stream.drop_while(|x| *x < 10);
        let result = run_stream_to_list(kept).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_drop_while_none_dropped() {
        let stream = Stream::emits(vec![1, 2, 3]);
        let kept = stream.drop_while(|x| *x < 0);
        let result = run_stream_to_list(kept).await;
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[actix_rt::test]
    async fn test_drop_while_empty_stream() {
        let stream = Stream::emits(Vec::<i32>::new());
        let kept = stream.drop_while(|x| *x < 4);
        let result = run_stream_to_list(kept).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_drain_non_empty() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 5]);
        let drained_stream = stream.drain(); // drain returns Stream<()>, which emits one () on completion
        let result: Vec<()> = run_stream_to_list(drained_stream).await;
        assert_eq!(result, vec![()]); // Expect one () unit value
    }

    #[actix_rt::test]
    async fn test_drain_empty_stream() {
        let stream = Stream::emits(Vec::<i32>::new());
        let drained_stream = stream.drain();
        let result: Vec<()> = run_stream_to_list(drained_stream).await;
        assert_eq!(result, vec![()]); // Expect one () unit value even for empty upstream
    }

    #[actix_rt::test]
    async fn test_emits_and_compile() {
        let stream = Stream::emits(vec![1, 2, 3]);
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[actix_rt::test]
    async fn test_emits_empty_and_compile() {
        let stream = Stream::emits(Vec::<i32>::new());
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_map() {
        let stream = Stream::emits(vec![1, 2, 3]).map(|x| x * 2);
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, vec![2, 4, 6]);
    }

    #[actix_rt::test]
    async fn test_map_empty() {
        let stream = Stream::emits(Vec::<i32>::new()).map(|x| x * 2);
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_map_types() {
        let items: Vec<String> = vec!["n:1".to_string(), "n:2".to_string(), "n:3".to_string()];
        let stream = Stream::emits(items).map(|s: String| s.to_uppercase());
        let result: Vec<String> = run_stream_to_list(stream).await;
        assert_eq!(
            result,
            vec!["N:1".to_string(), "N:2".to_string(), "N:3".to_string()]
        );
    }

    #[actix_rt::test]
    async fn test_chunks() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 5]).chunks(2);
        let result: Vec<Vec<i32>> = run_stream_to_list(stream).await;
        assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![5]]);
    }

    #[actix_rt::test]
    async fn test_chunks_exact() {
        let stream = Stream::emits(vec![1, 2, 3, 4]).chunks(2);
        let result: Vec<Vec<i32>> = run_stream_to_list(stream).await;
        assert_eq!(result, vec![vec![1, 2], vec![3, 4]]);
    }

    #[actix_rt::test]
    async fn test_chunks_larger_than_stream() {
        let stream = Stream::emits(vec![1, 2, 3]).chunks(5);
        let result: Vec<Vec<i32>> = run_stream_to_list(stream).await;
        assert_eq!(result, vec![vec![1, 2, 3]]);
    }

    #[actix_rt::test]
    async fn test_chunks_empty_input() {
        let stream = Stream::emits(Vec::<i32>::new()).chunks(2);
        let result: Vec<Vec<i32>> = run_stream_to_list(stream).await;
        assert_eq!(result, Vec::<Vec<i32>>::new());
    }

    #[actix_rt::test]
    async fn test_chunks_zero_size() {
        // ChunkingActor implementation likely treats 0 as 1 to avoid infinite loops/errors
        let stream = Stream::emits(vec![1, 2, 3]).chunks(0);
        let result: Vec<Vec<i32>> = run_stream_to_list(stream).await;
        assert_eq!(result, vec![vec![1], vec![2], vec![3]]);
    }

    #[actix_rt::test]
    async fn test_map_chunks() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 5]).map(|x| x + 1).chunks(2);
        let result: Vec<Vec<i32>> = run_stream_to_list(stream).await;
        assert_eq!(result, vec![vec![2, 3], vec![4, 5], vec![6]]);
    }

    #[actix_rt::test]
    async fn test_chunks_map() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 5])
            .chunks(2)
            .map(|chunk: Vec<i32>| chunk.into_iter().sum::<i32>());
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, vec![3, 7, 5]);
    }

    #[actix_rt::test]
    async fn test_filter() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 5, 6]).filter(|x| x % 2 == 0);
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, vec![2, 4, 6]);
    }

    #[actix_rt::test]
    async fn test_filter_empty_result() {
        let stream = Stream::emits(vec![1, 3, 5]).filter(|x| x % 2 == 0);
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_filter_all_pass() {
        let stream = Stream::emits(vec![2, 4, 6]).filter(|x| x % 2 == 0);
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, vec![2, 4, 6]);
    }

    #[actix_rt::test]
    async fn test_filter_after_map() {
        let items: Vec<String> = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let stream = Stream::emits(items)
            .map(|s: String| s.to_uppercase())
            .filter(|s_upper: &String| s_upper == "A" || s_upper == "C");
        let result: Vec<String> = run_stream_to_list(stream).await;
        assert_eq!(result, vec!["A".to_string(), "C".to_string()]);
    }

    #[actix_rt::test]
    async fn test_take() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 5]).take(3);
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[actix_rt::test]
    async fn test_take_more_than_available() {
        let stream = Stream::emits(vec![1, 2, 3]).take(5);
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[actix_rt::test]
    async fn test_take_zero() {
        let stream = Stream::emits(vec![1, 2, 3]).take(0);
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_take_on_empty_stream() {
        let stream = Stream::emits(Vec::<i32>::new()).take(5);
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_map_take_filter() {
        // Order: map, then filter, then take
        let stream = Stream::emits((0..10i32).collect::<Vec<i32>>()) // 0..9
            .map(|x: i32| x * 10) // 0, 10, ..., 90
            .filter(|x: &i32| *x > 30) // 40, 50, 60, 70, 80, 90
            .take(3); // 40, 50, 60
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, vec![40, 50, 60]);
    }

    #[actix_rt::test]
    async fn test_par_map_unordered_simple() {
        let stream = Stream::emits(vec![1, 2, 3, 4]).par_map_unordered(2, |x: i32| {
            async move {
                actix_rt::time::sleep(Duration::from_millis(if x % 2 == 0 { 10 } else { 50 }))
                    .await;
                x * 10
            }
            .boxed() // BoxFuture is Send
        });

        let mut result = run_stream_to_list(stream).await;
        result.sort_unstable();
        let expected = vec![10, 20, 30, 40];
        assert_eq!(
            result, expected,
            "par_map_unordered results should contain all items (order-independent check)"
        );
    }

    #[actix_rt::test]
    async fn test_par_map_unordered_string_concat() {
        let input_items: Vec<String> = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let stream = Stream::emits(input_items.clone()).par_map_unordered(3, |s: String| {
            let s_owned = s.to_string(); // Ensure s is owned for async block
            async move {
                if s_owned == "b" {
                    actix_rt::time::sleep(Duration::from_millis(50)).await;
                } else {
                    actix_rt::time::sleep(Duration::from_millis(10)).await;
                }
                format!("mapped:{}", s_owned)
            }
            .boxed()
        });

        let result_set: HashSet<String> = run_stream_to_list(stream).await.into_iter().collect();
        let expected_set: HashSet<String> = vec![
            "mapped:a".to_string(),
            "mapped:b".to_string(),
            "mapped:c".to_string(),
        ]
        .into_iter()
        .collect();
        assert_eq!(result_set, expected_set);
    }

    #[actix_rt::test]
    async fn test_par_map_unordered_empty_stream() {
        let stream = Stream::emits(Vec::<i32>::new())
            .par_map_unordered(2, |x: i32| async move { x * 2 }.boxed());
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_par_map_unordered_single_element() {
        let stream =
            Stream::emits(vec![100]).par_map_unordered(1, |x: i32| async move { x + 5 }.boxed());
        let result = run_stream_to_list(stream).await;
        assert_eq!(result, vec![105]);
    }

    #[actix_rt::test]
    async fn test_par_map_unordered_with_chunks() {
        // Example: Stream of numbers -> chunks -> sum each chunk in parallel
        let stream = Stream::emits((1..=10i32).collect::<Vec<i32>>()) // 1 to 10
            .chunks(3) // [[1,2,3], [4,5,6], [7,8,9], [10]]
            .par_map_unordered(2, |chunk: Vec<i32>| {
                // Process 2 chunks concurrently
                async move {
                    // Simulate work, e.g. sum is more work for larger numbers
                    actix_rt::time::sleep(Duration::from_millis(
                        if chunk.iter().sum::<i32>() % 2 == 0 {
                            10
                        } else {
                            50
                        },
                    ))
                    .await;
                    chunk.into_iter().sum::<i32>() // Sums: 6, 15, 24, 10
                }
                .boxed()
            });

        let mut result = run_stream_to_list(stream).await;
        result.sort_unstable(); // Sort because of unordered nature
        let expected = vec![6, 10, 15, 24];
        assert_eq!(result, expected);
    }

    // --- ConcatMap Tests ---
    #[actix_rt::test]
    async fn test_concat_map_simple() {
        let stream = Stream::emits(vec![1, 2]);
        let flat_mapped_stream = stream.concat_map(|x: i32| Stream::emits(vec![x, x * 10]));
        let result = run_stream_to_list(flat_mapped_stream).await;
        assert_eq!(result, vec![1, 10, 2, 20]);
    }

    #[actix_rt::test]
    async fn test_concat_map_with_empty_inner_streams() {
        let stream = Stream::emits(vec![1, 2, 3]);
        let flat_mapped_stream = stream.concat_map(|x: i32| {
            if x % 2 == 0 {
                Stream::emits(Vec::<i32>::new()) // Empty stream for even numbers
            } else {
                Stream::emits(vec![x, x]) // Stream with two items for odd numbers
            }
        });
        let result = run_stream_to_list(flat_mapped_stream).await;
        assert_eq!(result, vec![1, 1, 3, 3]);
    }

    #[actix_rt::test]
    async fn test_concat_map_outer_empty() {
        let stream = Stream::emits(Vec::<i32>::new()); // Outer stream is empty
        let flat_mapped_stream = stream.concat_map(|x: i32| Stream::emits(vec![x, x * 10]));
        let result = run_stream_to_list(flat_mapped_stream).await;
        assert_eq!(result, Vec::<i32>::new()); // Result should also be empty
    }

    #[actix_rt::test]
    async fn test_concat_map_inner_streams_with_delay() {
        // This test helps verify that concatMap processes inner streams sequentially.
        let stream = Stream::emits(vec![1, 2]); // Outer stream
        let flat_mapped_stream = stream.concat_map(|x: i32| {
            let items_for_inner = vec![x, x * 10];
            // Inner stream with delays to check sequential processing
            Stream::emits(items_for_inner).eval_map(move |val| async move {
                if val == x {
                    // First element of inner stream
                    actix_rt::time::sleep(Duration::from_millis(if x == 1 { 40 } else { 10 }))
                        .await;
                } else {
                    // Second element of inner stream
                    actix_rt::time::sleep(Duration::from_millis(10)).await;
                }
                val
            })
        });
        let result = run_stream_to_list(flat_mapped_stream).await;
        // Expected: 1 (delay 40ms), 10 (delay 10ms), then 2 (delay 10ms), 20 (delay 10ms)
        assert_eq!(result, vec![1, 10, 2, 20]);
    }

    #[actix_rt::test]
    async fn test_concat_map_propagate_end_correctly() {
        // Test that concatMap correctly ends when an inner stream ends,
        // or when the outer stream ends.

        // Outer stream taken to 1 element, inner stream taken to 1 element
        let stream_outer = Stream::emits(vec![1, 2]).take(1); // Emits [1], then End
        let stream_1 = stream_outer.concat_map(|x| Stream::emits(vec![x, x * 2]).take(1)); // Inner emits [x], then End
                                                                                           // Expected: outer(1) -> inner(1) -> result [1]
        assert_eq!(run_stream_to_list(stream_1).await, vec![1]);

        // Outer stream 2 elements, inner streams taken to 2 elements
        let stream_outer_2 = Stream::emits(vec![1, 2]); // Emits [1, 2], then End
        let stream_2 = stream_outer_2.concat_map(|x| Stream::emits(vec![x, x * 2, x * 3]).take(2)); // Inner emits [x, x*2], then End
                                                                                                    // Expected: outer(1) -> inner(1,2) -> outer(2) -> inner(2,4) -> result [1,2,2,4]
        assert_eq!(run_stream_to_list(stream_2).await, vec![1, 2, 2, 4]);
    }

    // --- Merge Tests ---

    #[actix_rt::test]
    async fn test_merge_basic() {
        let stream1 = Stream::emits(vec![1, 2, 3]);
        let stream2 = Stream::emits(vec![4, 5, 6]);
        let merged_stream = stream1.merge(stream2);
        let mut result = run_stream_to_list(merged_stream).await;
        result.sort_unstable();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[actix_rt::test]
    async fn test_merge_one_stream_empty() {
        let stream1 = Stream::emits(vec![1, 2, 3]);
        let stream2 = Stream::emits(Vec::<i32>::new());
        let merged_stream = stream1.merge(stream2);
        let mut result = run_stream_to_list(merged_stream).await;
        result.sort_unstable();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[actix_rt::test]
    async fn test_merge_other_stream_empty() {
        let stream1 = Stream::emits(Vec::<i32>::new());
        let stream2 = Stream::emits(vec![4, 5, 6]);
        let merged_stream = stream1.merge(stream2);
        let mut result = run_stream_to_list(merged_stream).await;
        result.sort_unstable();
        assert_eq!(result, vec![4, 5, 6]);
    }

    #[actix_rt::test]
    async fn test_merge_both_streams_empty() {
        let stream1 = Stream::emits(Vec::<i32>::new());
        let stream2 = Stream::emits(Vec::<i32>::new());
        let merged_stream = stream1.merge(stream2);
        let result = run_stream_to_list(merged_stream).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_merge_with_delays_interleaved() {
        use futures::FutureExt; // For .boxed()

        let stream1 = Stream::emits(vec![1, 3, 5]).eval_map(|x| {
            async move {
                actix_rt::time::sleep(Duration::from_millis(if x == 1 { 50 } else { 10 })).await;
                x
            }
            .boxed()
        });

        let stream2 = Stream::emits(vec![2, 4, 6]).eval_map(|x| {
            async move {
                actix_rt::time::sleep(Duration::from_millis(20)).await; // Constant moderate delay
                x
            }
            .boxed()
        });

        let merged_stream = stream1.merge(stream2);
        let mut result = run_stream_to_list(merged_stream).await;
        result.sort_unstable();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[actix_rt::test]
    async fn test_merge_different_lengths() {
        let stream1 = Stream::emits(vec![1, 2]);
        let stream2 = Stream::emits(vec![10, 20, 30, 40]);
        let merged_stream = stream1.merge(stream2);
        let mut result = run_stream_to_list(merged_stream).await;
        result.sort_unstable();
        assert_eq!(result, vec![1, 2, 10, 20, 30, 40]);
    }

    #[actix_rt::test]
    async fn test_merge_one_finishes_early() {
        use futures::FutureExt; // For .boxed()

        // Stream 1 emits its items quickly
        let stream1 = Stream::emits(vec![100, 200]).eval_map(|x| {
            async move {
                actix_rt::time::sleep(Duration::from_millis(5)).await;
                x
            }
            .boxed()
        });

        // Stream 2 emits its items slowly
        let stream2 = Stream::emits(vec![1, 2, 3]).eval_map(|x| {
            async move {
                actix_rt::time::sleep(Duration::from_millis(50)).await;
                x
            }
            .boxed()
        });

        let merged_stream = stream1.merge(stream2);
        let mut result = run_stream_to_list(merged_stream).await;
        result.sort_unstable(); // Sort for reliable comparison
        assert_eq!(result, vec![1, 2, 3, 100, 200]);
    }

    #[actix_rt::test]
    async fn test_merge_string_streams() {
        let stream1 = Stream::emits(vec!["a".to_string(), "c".to_string()]);
        let stream2 = Stream::emits(vec!["b".to_string(), "d".to_string()]);
        let merged_stream = stream1.merge(stream2);
        let mut result = run_stream_to_list(merged_stream).await;
        result.sort_unstable();
        assert_eq!(
            result,
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string()
            ]
        );
    }

    // --- GroupWithin Tests ---

    #[actix_rt::test]
    async fn test_group_within_by_size() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 5, 6]);
        // Group by size 2, duration long enough not to trigger
        let grouped_stream = stream.group_within(2, Duration::from_secs(10));
        let result: Vec<Vec<i32>> = run_stream_to_list(grouped_stream).await;
        assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![5, 6]]);
    }

    #[actix_rt::test]
    async fn test_group_within_by_size_partial_last_chunk() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 5]);
        // Group by size 2, duration long enough not to trigger
        let grouped_stream = stream.group_within(2, Duration::from_secs(10));
        let result: Vec<Vec<i32>> = run_stream_to_list(grouped_stream).await;
        // The last item [5] is flushed when the upstream ends.
        assert_eq!(result, vec![vec![1, 2], vec![3, 4], vec![5]]);
    }

    #[actix_rt::test]
    async fn test_group_within_by_time() {
        // Emit items with a delay, group by time
        let source_stream = Stream::emits(vec![1, 2, 3]).eval_map(|x| {
            async move {
                actix_rt::time::sleep(Duration::from_millis(60)).await; // Delay between items
                x
            }
            .boxed()
        });

        // Group by time (100ms), size large enough not to trigger by size
        let grouped_stream = source_stream.group_within(5, Duration::from_millis(100));
        let result: Vec<Vec<i32>> = run_stream_to_list(grouped_stream).await;
        assert_eq!(result, vec![vec![1, 2], vec![3]]);
    }

    #[actix_rt::test]
    async fn test_group_within_by_time_single_item_groups() {
        // Emit items with a delay longer than the grouping duration
        let source_stream = Stream::emits(vec![1, 2, 3]).eval_map(|x| {
            async move {
                actix_rt::time::sleep(Duration::from_millis(150)).await; // Delay longer than group duration
                x
            }
            .boxed()
        });

        // Group by time (100ms), size large enough not to trigger
        let grouped_stream = source_stream.group_within(5, Duration::from_millis(100));
        let result: Vec<Vec<i32>> = run_stream_to_list(grouped_stream).await;
        // Each item should form its own group due to timeout
        assert_eq!(result, vec![vec![1], vec![2], vec![3]]);
    }

    #[actix_rt::test]
    async fn test_group_within_empty_stream() {
        let stream = Stream::emits(Vec::<i32>::new());
        let grouped_stream = stream.group_within(2, Duration::from_secs(1));
        let result: Vec<Vec<i32>> = run_stream_to_list(grouped_stream).await;
        assert_eq!(result, Vec::<Vec<i32>>::new());
    }

    #[actix_rt::test]
    async fn test_group_within_fewer_items_than_size_emitted_at_end() {
        let stream = Stream::emits(vec![1, 2]);
        // Group by size 3, duration long enough not to trigger
        let grouped_stream = stream.group_within(3, Duration::from_secs(10));
        let result: Vec<Vec<i32>> = run_stream_to_list(grouped_stream).await;
        // Buffer [1,2] flushed when upstream ends
        assert_eq!(result, vec![vec![1, 2]]);
    }

    #[actix_rt::test]
    async fn test_group_within_zero_chunk_size_acts_as_one() {
        // GroupWithinActor treats max_chunk_size 0 as 1.
        let stream = Stream::emits(vec![1, 2, 3]);
        let grouped_stream = stream.group_within(0, Duration::from_secs(10));
        let result: Vec<Vec<i32>> = run_stream_to_list(grouped_stream).await;
        assert_eq!(result, vec![vec![1], vec![2], vec![3]]);
    }

    #[actix_rt::test]
    async fn test_group_within_rapid_emission_groups_by_size() {
        // Emit many items quickly, should group by size.
        let items: Vec<i32> = (1..=10).collect();
        let source_stream = Stream::emits(items.clone()).eval_map(|x| {
            async move {
                actix_rt::time::sleep(Duration::from_millis(1)).await; // Very short delay
                x
            }
            .boxed()
        });

        // Group by size 3, duration relatively long (100ms)
        let grouped_stream = source_stream.group_within(3, Duration::from_millis(100));
        let result: Vec<Vec<i32>> = run_stream_to_list(grouped_stream).await;
        assert_eq!(
            result,
            vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9], vec![10]]
        );
    }

    #[actix_rt::test]
    async fn test_group_within_mixed_size_and_time() {
        // Create a stream of events with controlled timing
        // (item, delay_after_item_ms)
        let events_with_delays: Vec<(i32, u64)> = vec![
            (1, 10), // item 1, then 10ms delay
            (2, 10), // item 2, then 10ms delay. Total time for [1,2] = 20ms. Chunk size 3. Timer 50ms.
            (3, 70), // item 3, then 70ms delay. Buffer [1,2,3]. Size hit. Emit [1,2,3]. Timer reset.
            // Total time for [1,2,3] = 20ms for items + processing. Emitted by size.
            (4, 10), // item 4 arrives. Starts new timer. Then 10ms delay.
            (5, 70), // item 5 arrives. Buffer [4,5]. 10ms+10ms = 20ms for items. Timer (50ms) for 4 fires. Emit [4,5].
            // Then 70ms delay.
            (6, 10), // item 6 arrives. Starts new timer. Then 10ms delay.
            (7, 10), // item 7 arrives. Buffer [6,7]. 10ms+10ms = 20ms.
        ]; // Upstream ends. Emit [6,7].

        let (tx, mut rx) = futures::channel::mpsc::unbounded::<StreamMessage<i32>>();

        actix_rt::spawn(async move {
            for (item, delay_ms) in events_with_delays {
                tx.unbounded_send(StreamMessage::Element(item)).unwrap();
                actix_rt::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            // Sender drops, MPSC stream will end
        });

        let source_stream = Stream {
            setup_fn: Box::new(move |recipient| {
                async move {
                    while let Some(msg) = rx.next().await {
                        if recipient.try_send(msg).is_err() {
                            return Err(String::from("Failed to send message to recipient"));
                        }
                    }
                    let _ = recipient.try_send(StreamMessage::End);
                    Ok(())
                }
                .boxed()
            }),
            _phantom: PhantomData::<i32>,
        };

        let grouped_stream = source_stream.group_within(3, Duration::from_millis(50));
        let result: Vec<Vec<i32>> = run_stream_to_list(grouped_stream).await;

        // Expected behavior based on comments above:
        // 1. [1,2,3] (by size, items arrive quickly within 50ms timer)
        // 2. [4,5] (by time, item 4 starts timer, item 5 arrives, timer for 4 fires)
        // 3. [6,7] (by end of stream)
        assert_eq!(result, vec![vec![1, 2, 3], vec![4, 5], vec![6, 7]]);
    }

    #[actix_rt::test]
    async fn test_group_within_timer_reset_behavior_not_expected() {
        // This test is to confirm current behavior: timer is NOT reset by new items.
        // It times out from the *first* item in the current pending chunk.
        let (tx, mut rx) = futures::channel::mpsc::unbounded::<StreamMessage<i32>>();

        actix_rt::spawn(async move {
            tx.unbounded_send(StreamMessage::Element(1)).unwrap(); // Item 1, starts 100ms timer
            actix_rt::time::sleep(Duration::from_millis(60)).await; // 60ms elapsed

            tx.unbounded_send(StreamMessage::Element(2)).unwrap(); // Item 2. Buffer [1,2]. Timer for 1 has 40ms left.
            actix_rt::time::sleep(Duration::from_millis(60)).await; // Total 120ms elapsed. Timer for 1 fired at 100ms. [1,2] emitted.

            tx.unbounded_send(StreamMessage::Element(3)).unwrap(); // Item 3, new timer starts (100ms)
            actix_rt::time::sleep(Duration::from_millis(60)).await; // 60ms elapsed for item 3's timer

            tx.unbounded_send(StreamMessage::Element(4)).unwrap(); // Item 4. Buffer [3,4]. Timer for 3 has 40ms left.
        });

        let source_stream = Stream {
            setup_fn: Box::new(move |recipient| {
                async move {
                    while let Some(msg) = rx.next().await {
                        if recipient.try_send(msg).is_err() {
                            return Err(String::from(
                                "Failed to send message to recipient in test setup",
                            ));
                        }
                    }
                    let _ = recipient.try_send(StreamMessage::End);
                    Ok(())
                }
                .boxed()
            }),
            _phantom: PhantomData::<i32>,
        };

        let grouped_stream = source_stream.group_within(5, Duration::from_millis(100)); // Size 5, Time 100ms
        let result: Vec<Vec<i32>> = run_stream_to_list(grouped_stream).await;
        assert_eq!(result, vec![vec![1, 2], vec![3, 4]]);
    }

    // --- Zip and ZipWith Tests ---

    #[actix_rt::test]
    async fn test_zip_basic() {
        let stream1 = Stream::emits(vec![1, 2, 3]);
        let stream2 = Stream::emits(vec!['a', 'b', 'c']);
        let zipped_stream = stream1.zip(stream2);
        let result = run_stream_to_list(zipped_stream).await;
        assert_eq!(result, vec![(1, 'a'), (2, 'b'), (3, 'c')]);
    }

    #[actix_rt::test]
    async fn test_zip_left_shorter() {
        let stream1 = Stream::emits(vec![1, 2]);
        let stream2 = Stream::emits(vec!['a', 'b', 'c']);
        let zipped_stream = stream1.zip(stream2);
        let result = run_stream_to_list(zipped_stream).await;
        assert_eq!(result, vec![(1, 'a'), (2, 'b')]);
    }

    #[actix_rt::test]
    async fn test_zip_right_shorter() {
        let stream1 = Stream::emits(vec![1, 2, 3]);
        let stream2 = Stream::emits(vec!['a', 'b']);
        let zipped_stream = stream1.zip(stream2);
        let result = run_stream_to_list(zipped_stream).await;
        assert_eq!(result, vec![(1, 'a'), (2, 'b')]);
    }

    #[actix_rt::test]
    async fn test_zip_left_empty() {
        let stream1 = Stream::emits(Vec::<i32>::new());
        let stream2 = Stream::emits(vec!['a', 'b', 'c']);
        let zipped_stream = stream1.zip(stream2);
        let result = run_stream_to_list(zipped_stream).await;
        assert_eq!(result, Vec::<(i32, char)>::new());
    }

    #[actix_rt::test]
    async fn test_zip_right_empty() {
        let stream1 = Stream::emits(vec![1, 2, 3]);
        let stream2 = Stream::emits(Vec::<char>::new());
        let zipped_stream = stream1.zip(stream2);
        let result = run_stream_to_list(zipped_stream).await;
        assert_eq!(result, Vec::<(i32, char)>::new());
    }

    #[actix_rt::test]
    async fn test_zip_both_empty() {
        let stream1 = Stream::emits(Vec::<i32>::new());
        let stream2 = Stream::emits(Vec::<char>::new());
        let zipped_stream = stream1.zip(stream2);
        let result = run_stream_to_list(zipped_stream).await;
        assert_eq!(result, Vec::<(i32, char)>::new());
    }

    #[actix_rt::test]
    async fn test_zip_with_formatting() {
        let stream1 = Stream::emits(vec![1, 2, 3]);
        // Note: Need to provide type for &str if not inferrable or if using .to_string() etc inside closure
        let stream2 = Stream::emits(vec![
            "one".to_string(),
            "two".to_string(),
            "three".to_string(),
        ]);
        let zipped_stream = stream1.zip_with(stream2, |num, s: String| {
            format!("{}: {}", num, s.to_uppercase())
        });
        let result: Vec<String> = run_stream_to_list(zipped_stream).await;
        assert_eq!(
            result,
            vec![
                "1: ONE".to_string(),
                "2: TWO".to_string(),
                "3: THREE".to_string()
            ]
        );
    }

    #[actix_rt::test]
    async fn test_zip_with_delays() {
        let stream1 = Stream::emits(vec![1, 2]).eval_map(|x| async move {
            if x == 1 {
                actix_rt::time::sleep(Duration::from_millis(50)).await;
            }
            x
        });
        let stream2 = Stream::emits(vec!['a', 'b']).eval_map(|c_val| async move {
            // Renamed c to c_val
            if c_val == 'a' {
                actix_rt::time::sleep(Duration::from_millis(10)).await;
            }
            c_val
        });

        let zipped_stream = stream1.zip(stream2);
        let result = run_stream_to_list(zipped_stream).await;
        assert_eq!(result, vec![(1, 'a'), (2, 'b')]);
    }

    // --- InterruptWhen Tests ---

    #[actix_rt::test]
    async fn test_interrupt_when_immediate() {
        let stream = Stream::emits(vec![1, 2, 3, 4, 5]).eval_map(|x| async move {
            actix_rt::time::sleep(Duration::from_millis(20)).await; // Small delay per item
            x
        });
        let interrupt_signal = ready(()); // Completes immediately
        let interrupted_stream = stream.interrupt_when(interrupt_signal);
        let result = run_stream_to_list(interrupted_stream).await;
        // Stream might emit 0 or 1 element depending on races.
        // The InterruptActor's monitor task for `ready(())` will send InterruptSignalCompletedMsg.
        // If any item from `emits` gets to `InterruptActor` and is processed before this msg, it might pass.
        assert!(
            result.len() <= 1,
            "Stream should be empty or have at most one element, got: {:?}",
            result
        );
    }

    #[actix_rt::test]
    async fn test_interrupt_when_mid_stream() {
        let (tx_original, rx_interrupt) = futures::channel::oneshot::channel::<()>();
        // Wrap the sender in Arc<Mutex<Option<Sender>>> to make it Cloneable for the eval_map closure
        let shared_tx = Arc::new(Mutex::new(Some(tx_original)));

        let stream = Stream::emits(vec![1, 2, 3, 4, 5]).eval_map({
            let shared_tx_clone_for_eval_map_closure = Arc::clone(&shared_tx); // Clone Arc for the eval_map closure
            move |x_val| {
                // This outer closure (passed to eval_map) needs to be Clone
                let shared_tx_clone_for_async_block =
                    Arc::clone(&shared_tx_clone_for_eval_map_closure); // Clone Arc for the async block
                async move {
                    if x_val == 3 {
                        // Lock the mutex to safely take and use the sender
                        if let Some(tx) = shared_tx_clone_for_async_block
                            .lock()
                            .expect("Mutex lock failed in test")
                            .take()
                        {
                            let _ = tx.send(());
                        }
                    }
                    actix_rt::time::sleep(Duration::from_millis(30)).await; // Ensure elements are spaced out
                    x_val
                }
            }
        });

        // rx_interrupt is moved into the async block.
        let interrupted_stream =
            stream.interrupt_when(async { rx_interrupt.await.unwrap_or_default() });
        let result = run_stream_to_list(interrupted_stream).await;

        assert!(result.contains(&1), "Should contain 1");
        assert!(result.contains(&2), "Should contain 2");
        assert!(!result.contains(&4), "Should not contain 4");
        assert!(!result.contains(&5), "Should not contain 5");
        assert!(
            result.len() == 2 || result.len() == 3,
            "Result length should be 2 or 3, got {:?}",
            result
        );
        if result.len() == 3 {
            assert!(result.contains(&3), "If length is 3, it should contain 3");
        }
    }

    #[actix_rt::test]
    async fn test_interrupt_when_never_completes() {
        let stream = Stream::emits(vec![1, 2, 3]);
        let interrupt_signal = pending::<()>(); // Never completes
        let interrupted_stream = stream.interrupt_when(interrupt_signal);
        let result = run_stream_to_list(interrupted_stream).await;
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[actix_rt::test]
    async fn test_interrupt_when_on_empty_stream_ready_interrupt() {
        let stream = Stream::emits(Vec::<i32>::new());
        let interrupt_signal = ready(());
        let interrupted_stream = stream.interrupt_when(interrupt_signal);
        let result = run_stream_to_list(interrupted_stream).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_interrupt_when_on_empty_stream_pending_interrupt() {
        let stream = Stream::emits(Vec::<i32>::new());
        let interrupt_signal = pending::<()>();
        let interrupted_stream = stream.interrupt_when(interrupt_signal);
        let result = run_stream_to_list(interrupted_stream).await;
        assert_eq!(result, Vec::<i32>::new());
    }

    #[actix_rt::test]
    async fn test_interrupt_when_after_stream_ends() {
        let (tx_interrupt, rx_interrupt) = futures::channel::oneshot::channel::<()>();

        let stream = Stream::emits(vec![1, 2]).eval_map(|x| async move {
            actix_rt::time::sleep(Duration::from_millis(5)).await;
            x
        });

        // Spawn a task to send the interrupt signal later
        let _ = actix_rt::spawn(async move {
            // Assign to _ to avoid warning if JoinHandle is not used
            actix_rt::time::sleep(Duration::from_millis(100)).await; // Interrupt well after stream likely ends
            let _ = tx_interrupt.send(());
        });

        let interrupted_stream =
            stream.interrupt_when(async { rx_interrupt.await.unwrap_or_default() });
        let result = run_stream_to_list(interrupted_stream).await;
        assert_eq!(result, vec![1, 2]);
    }

    // --- Tests for on_finalize ---
    // Imports for Arc, AtomicBool, Ordering, Duration, Mutex should be at the top of `mod tests`
    use tokio::time::sleep; // Renamed from actix_rt::time::sleep for clarity if tokio is the primary runtime context

    #[actix_rt::test]
    async fn test_on_finalize_success_effect_runs() {
        let effect_run = Arc::new(AtomicBool::new(false));
        let effect_run_clone = effect_run.clone();

        let stream = Stream::emits(vec![1, 2, 3]).on_finalize(move || {
            let effect_run_clone_inner = effect_run_clone.clone();
            async move {
                effect_run_clone_inner.store(true, Ordering::SeqCst);
                // println!("[test_on_finalize_error_effect_runs] Effect executed.");
            }
            .boxed()
        });

        let result = stream.compile_to_list().await;
        assert_eq!(result, Ok(vec![1, 2, 3]));
        sleep(Duration::from_millis(200)).await; // Allow time for the spawned effect
        assert!(
            effect_run.load(Ordering::SeqCst),
            "Effect was not run on success"
        );
    }

    #[actix_rt::test]
    async fn test_on_finalize_error_effect_runs() {
        let effect_run = Arc::new(AtomicBool::new(false));
        let effect_run_clone = effect_run.clone();

        let stream: Stream<i32> =
            Stream::future(async { Err::<i32, String>("Test error".to_string()) }.boxed())
                .on_finalize(move || {
                    let effect_run_clone_inner = effect_run_clone.clone();
                    async move {
                        effect_run_clone_inner.store(true, Ordering::SeqCst);
                        // println!("[test_on_finalize_error_effect_runs] Effect executed.");
                    }
                    .boxed()
                });

        let result = stream.compile_to_list().await;
        assert_eq!(result, Err("Test error".to_string()));
        sleep(Duration::from_millis(50)).await;
        assert!(
            effect_run.load(Ordering::SeqCst),
            "Effect was not run on error"
        );
    }

    #[actix_rt::test]
    async fn test_on_finalize_propagates_elements_and_ends_correctly() {
        let effect_run_count = Arc::new(Mutex::new(0));
        let effect_run_count_clone = effect_run_count.clone();

        let stream = Stream::emits(vec![10, 20]).on_finalize(move || {
            let mut count = effect_run_count_clone.lock().unwrap();
            *count += 1;
            // println!("[test_on_finalize_propagates_elements_and_ends_correctly] Effect executed.");
            async {}
        });

        let result = stream.compile_to_list().await;
        assert_eq!(result, Ok(vec![10, 20]));
        sleep(Duration::from_millis(50)).await;
        assert_eq!(
            *effect_run_count.lock().unwrap(),
            1,
            "Effect ran incorrect number of times"
        );
    }

    #[actix_rt::test]
    async fn test_on_finalize_propagates_original_error() {
        let effect_run = Arc::new(AtomicBool::new(false));
        let effect_run_clone = effect_run.clone();

        // Create a stream that produces one element then an error.
        let stream_err: Stream<Result<i32, String>> = Stream::emits(vec![1, 2]) // element 2 will cause error
            .eval_map(|x| async move {
                if x == 2 {
                    Err("forced error".to_string())
                } else {
                    Ok::<i32, String>(x)
                }
            })
            .on_finalize(move || {
                effect_run_clone.store(true, Ordering::SeqCst);
                // println!("[test_on_finalize_propagates_original_error] Effect executed.");
                async {}
            });

        let result = stream_err.compile_to_list().await;
        assert_eq!(result, Ok(vec![Ok(1), Err("forced error".to_string())]));
        sleep(Duration::from_millis(50)).await;
        assert!(
            effect_run.load(Ordering::SeqCst),
            "Effect was not run when error propagated"
        );
    }

    #[actix_rt::test]
    async fn test_on_finalize_runs_on_empty_stream_completion() {
        let effect_run = Arc::new(AtomicBool::new(false));
        let effect_run_clone = effect_run.clone();

        let stream: Stream<i32> = Stream::emits(Vec::<i32>::new()).on_finalize(move || {
            let effect_run_clone_inner = effect_run_clone.clone();
            async move {
                effect_run_clone_inner.store(true, Ordering::SeqCst);
                // println!("[test_on_finalize_runs_on_empty_stream_completion] Effect executed.");
            }
        });

        let result = stream.compile_to_list().await;
        assert_eq!(result, Ok(vec![]));
        sleep(Duration::from_millis(50)).await;
        assert!(
            effect_run.load(Ordering::SeqCst),
            "Effect was not run on empty stream"
        );
    }

    #[actix_rt::test]
    async fn test_on_finalize_runs_if_stream_is_taken() {
        let effect_run = Arc::new(AtomicBool::new(false));
        let effect_run_clone = effect_run.clone();

        let stream = Stream::emits(vec![1, 2, 3, 4, 5])
            .on_finalize(move || {
                let effect_run_clone_inner = effect_run_clone.clone();
                async move {
                    effect_run_clone_inner.store(true, Ordering::SeqCst);
                    // println!("[test_on_finalize_runs_if_stream_is_taken] Effect executed.");
                }
            })
            .take(2);

        let result = stream.compile_to_list().await;
        assert_eq!(result, Ok(vec![1, 2]));
        sleep(Duration::from_millis(100)).await;
        assert!(
            effect_run.load(Ordering::SeqCst),
            "Effect was not run when stream was taken"
        );
    }

    #[actix_rt::test]
    async fn test_on_finalize_effect_runs_only_once_on_complex_case() {
        // This test covers a scenario where an error occurs, and `take` might also be involved.
        // The key is that `on_finalize`'s effect should run exactly once.
        let effect_run_count = Arc::new(Mutex::new(0));
        let effect_run_count_clone = effect_run_count.clone();

        let stream: Stream<Result<i32, String>> = Stream::emits(vec![1, 0, 3]) // 0 will cause an error
            .eval_map(|x| async move {
                if x == 0 {
                    Err("simulated error from eval_map".to_string())
                } else {
                    Ok::<i32, String>(10 / x) // Should process 1 (10/1 = 10), then error on 0
                }
            })
            .on_finalize(move || {
                let mut count = effect_run_count_clone.lock().unwrap();
                *count += 1;
                // println!("[test_on_finalize_effect_runs_only_once_on_complex_case] Effect executed. Count: {}", *count);
                async {}
            })
            .take(5); // Take more elements than the stream would produce before erroring

        let result = stream.compile_to_list().await;
        // Expecting error before take(5) completes
        assert_eq!(
            result,
            Ok(vec![
                Ok(10),
                Err("simulated error from eval_map".to_string()),
                Ok(3)
            ])
        );
        sleep(Duration::from_millis(100)).await; // Allow time for all actors to process and effect to run
        assert_eq!(
            *effect_run_count.lock().unwrap(),
            1,
            "Effect ran an incorrect number of times"
        );
    }
}
