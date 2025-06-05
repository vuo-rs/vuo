use actix::Recipient; // Correct single import for Recipient
use actix_rt;
use futures::FutureExt; // For .boxed()
use futures::StreamExt as FuturesStreamExt; // For .next() on MPSC receiver
use std::marker::PhantomData;
use std::time::Duration;
use tokio::task::{self, LocalSet};
use vuo::Stream;
use vuo::stream::StreamMessage; // Correct import path for StreamMessage

#[derive(Debug, Clone, PartialEq)]
struct Event {
    id: u32,
    data: String,
}

// No explicit `impl Streamable for Event` needed due to blanket impl in virta::stream::streamable.rs,
// assuming Event satisfies 'static + Send + Unpin + Debug (which it does).

fn main() {
    let system = actix_rt::System::new();
    system.block_on(async {
        let local_set = LocalSet::new();
        local_set
            .run_until(async {
                println!("[Main] Debounce Example: Starting");
                let debounce_duration = Duration::from_millis(200);

                // Create an MPSC channel for the Emitter to send items for the source_stream
                let (tx_emitter_mpsc, rx_emitter_mpsc) =
                    futures::channel::mpsc::unbounded::<StreamMessage<Event>>();

                // This task will emit items via the MPSC sender
                let emitter_handle = task::spawn_local(async move {
                    println!("[Emitter] Sending item 1 (isolated)");
                    tx_emitter_mpsc
                        .unbounded_send(StreamMessage::Element(Event {
                            id: 1,
                            data: "A".to_string(),
                        }))
                        .ok();
                    tokio::time::sleep(Duration::from_millis(300)).await; // Longer than debounce

                    println!("[Emitter] Sending item 2 (start of burst)");
                    tx_emitter_mpsc
                        .unbounded_send(StreamMessage::Element(Event {
                            id: 2,
                            data: "B_first".to_string(),
                        }))
                        .ok();
                    tokio::time::sleep(Duration::from_millis(50)).await; // Shorter than debounce

                    println!("[Emitter] Sending item 3 (middle of burst)");
                    tx_emitter_mpsc
                        .unbounded_send(StreamMessage::Element(Event {
                            id: 3,
                            data: "B_second".to_string(),
                        }))
                        .ok();
                    tokio::time::sleep(Duration::from_millis(50)).await; // Shorter than debounce

                    println!("[Emitter] Sending item 4 (end of burst - this should be debounced)");
                    tx_emitter_mpsc
                        .unbounded_send(StreamMessage::Element(Event {
                            id: 4,
                            data: "B_last".to_string(),
                        }))
                        .ok();
                    tokio::time::sleep(Duration::from_millis(300)).await; // Longer than debounce

                    println!("[Emitter] Sending item 5 (isolated)");
                    tx_emitter_mpsc
                        .unbounded_send(StreamMessage::Element(Event {
                            id: 5,
                            data: "C".to_string(),
                        }))
                        .ok();
                    tokio::time::sleep(Duration::from_millis(300)).await; // Longer than debounce

                    println!("[Emitter] Sending item 6 (penultimate, will be overridden by 7)");
                    tx_emitter_mpsc
                        .unbounded_send(StreamMessage::Element(Event {
                            id: 6,
                            data: "D_almost".to_string(),
                        }))
                        .ok();
                    tokio::time::sleep(Duration::from_millis(50)).await;

                    println!("[Emitter] Sending item 7 (final item to be flushed)");
                    tx_emitter_mpsc
                        .unbounded_send(StreamMessage::Element(Event {
                            id: 7,
                            data: "D_final".to_string(),
                        }))
                        .ok();

                    println!("[Emitter] Sending End (by closing MPSC channel)");
                    // tx_emitter_mpsc is dropped when emitter_handle scope ends, closing the channel (signaling End for the MPSC stream).
                });

                // Define the setup_fn for the source_stream.
                // It polls items from rx_emitter_mpsc and sends them to its downstream (the debounce actor).
                let source_stream_setup_fn = Box::new(
                    move |downstream_recipient: Recipient<StreamMessage<Event>>| {
                        // rx_emitter_mpsc is moved into this closure, then into the async block.
                        async move {
                            // It's important that rx_emitter_mpsc is mutable here for .next()
                            let mut rx_mpsc_for_stream = rx_emitter_mpsc;
                            while let Some(msg) =
                                FuturesStreamExt::next(&mut rx_mpsc_for_stream).await
                            {
                                if downstream_recipient.try_send(msg).is_err() {
                                    // Downstream (debounce actor) closed or errored.
                                    return Err(String::from(
                                        "Downstream consumer closed for debounce example source",
                                    ));
                                }
                            }
                            // MPSC channel closed by sender (emitter_handle finished).
                            // Signal End to the debounce actor.
                            let _ = downstream_recipient.try_send(StreamMessage::End);
                            Ok(())
                        }
                        .boxed() // Returns BoxFuture<'static, Result<(), ()>>
                    },
                );

                let source_stream = Stream {
                    setup_fn: source_stream_setup_fn,
                    _phantom: PhantomData,
                };

                println!(
                    "[Main] Applying debounce with duration: {:?}",
                    debounce_duration
                );
                let debounced_stream = source_stream.debounce(debounce_duration);

                println!("[Main] Collecting results from debounced stream...");
                // Spawn the collection onto a local task to allow the emitter to run concurrently
                // before we await the final results.
                let debounced_results_handle =
                    task::spawn_local(async move { debounced_stream.compile_to_list().await });

                // Wait for emitter to finish its work (dropping tx_emitter_mpsc and closing the MPSC channel)
                emitter_handle.await.expect("Emitter task failed");
                println!("[Main] Emitter task completed.");

                // Now await the results from the debounced stream
                match debounced_results_handle
                    .await
                    .expect("Debounced stream task panicked")
                {
                    Ok(results) => {
                        println!("\n[Main] Debounced results ({} items):", results.len());
                        for (idx, res_event) in results.iter().enumerate() {
                            let current_event: &Event = res_event; // Explicit type for clarity
                            println!(
                                "  Result {}: ID: {}, Data: '{}'",
                                idx, current_event.id, current_event.data
                            );
                        }

                        let expected_ids: Vec<u32> = vec![1, 4, 5, 7];
                        let received_ids: Vec<u32> = results
                            .iter()
                            .map(|event_ref: &Event| event_ref.id)
                            .collect(); // Explicit type
                        assert_eq!(received_ids, expected_ids, "Debounced items mismatch!");
                        if results.len() == 4 {
                            // Further check data if length is correct
                            assert_eq!(results[0].data, "A", "Data mismatch for ID 1");
                            assert_eq!(results[1].data, "B_last", "Data mismatch for ID 4");
                            assert_eq!(results[2].data, "C", "Data mismatch for ID 5");
                            assert_eq!(results[3].data, "D_final", "Data mismatch for ID 7");
                        }
                        println!("\n[Main] Verification successful.");
                    }
                    Err(_) => eprintln!("[Main] Debounced stream processing failed."),
                }

                println!("\n[Main] Debounce Example: Complete.");
            })
            .await; // End of local_set.run_until
    }); // End of system.block_on
}
