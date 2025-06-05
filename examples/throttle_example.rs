use actix::Recipient;
use actix_rt;
use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt as FuturesStreamExt;
use futures::channel::mpsc;
use std::marker::PhantomData;
use std::time::{Duration, Instant};
use tokio::task::{self, LocalSet};
use vuo::{Stream, stream::StreamMessage}; // Ensure StreamMessage is accessible

#[derive(Debug, Clone, PartialEq)]
struct Tick {
    id: u32,
    // Store original emission time for verification, though downstream processing time will differ
    original_timestamp: Instant,
}

fn main() {
    let system = actix_rt::System::new();
    system.block_on(async {
        let local_set = LocalSet::new();
        local_set.run_until(async {
            println!("[Main] Throttle (Trailing Mode) Example: Starting");

            let throttle_duration = Duration::from_millis(500);
            let total_items_to_send: u32 = 10;
            let send_interval = Duration::from_millis(100); // Send items faster than throttle

            // --- Emitter Task & Manual Source Stream ---
            let (mut tx_emitter_mpsc, rx_emitter_mpsc) =
                mpsc::unbounded::<StreamMessage<Tick>>();

            let emitter_handle = task::spawn_local(async move {
                println!(
                    "[Emitter] Starting to emit {} items at ~{}ms intervals.",
                    total_items_to_send,
                    send_interval.as_millis()
                );
                for i in 0..total_items_to_send {
                    let event = Tick {
                        id: i,
                        original_timestamp: Instant::now(),
                    };
                    if tx_emitter_mpsc
                        .send(StreamMessage::Element(event))
                        .await
                        .is_err()
                    {
                        println!("[Emitter] Error sending item {} to MPSC. Receiver likely dropped.", i);
                        return;
                    }
                    if i < total_items_to_send - 1 {
                        tokio::time::sleep(send_interval).await;
                    }
                }
                println!("[Emitter] All items sent. Sending End.");
                if tx_emitter_mpsc.send(StreamMessage::End).await.is_err() {
                    println!("[Emitter] Error sending End to MPSC.");
                }
                println!("[Emitter] Emitter task finished.");
            });

            let source_stream_setup_fn = Box::new(
                move |downstream_recipient: Recipient<StreamMessage<Tick>>| {
                    let mut rx_mpsc_clone = rx_emitter_mpsc;
                    async move {
                        while let Some(msg) = FuturesStreamExt::next(&mut rx_mpsc_clone).await {
                            if downstream_recipient.try_send(msg).is_err() {
                                return Err(String::from("Downstream consumer closed for throttle example source")); // Downstream closed
                            }
                        }
                        // MPSC channel closed by sender, ensure End is propagated.
                        let _ = downstream_recipient.try_send(StreamMessage::End);
                        Ok(())
                    }
                    .boxed()
                },
            );
            let source_stream = Stream {
                setup_fn: source_stream_setup_fn,
                _phantom: PhantomData,
            };
            // --- End of Source Stream Setup ---

            println!("[Main] Applying throttle with duration: {:?}", throttle_duration);
            let throttled_stream = source_stream.throttle(throttle_duration);

            println!("[Main] Collecting results from throttled stream...");
            let collection_start_time = Instant::now();
            let mut received_timestamps = Vec::new();

            match throttled_stream.compile_to_list().await {
                Ok(results) => {
                    println!("\n[Main] Throttled results ({} items):", results.len());
                    for (idx, item) in results.iter().enumerate() {
                        let time_since_collection_start = item.original_timestamp.duration_since(collection_start_time);
                        received_timestamps.push(item.original_timestamp);
                        println!(
                            "  Result {}: ID: {}, Original_Emit_Offset: {:.3}s",
                            idx,
                            item.id,
                            time_since_collection_start.as_secs_f32()
                        );
                    }

                    // Verification
                    // 1. Number of items:
                    // Emitter sends 10 items over ~900ms (9 intervals of 100ms).
                    // Throttle is 500ms.
                    // Expected: Item 0 @ ~0ms. Next possible @ ~500ms (Item 5 if sent at 500ms). Next possible @ ~1000ms (stream ends).
                    // So, 2 items if stream ends just after 900ms.
                    // If the last item processing and End signal take time, maybe 3.
                    // This calculation is tricky due to async scheduling.
                    // The core idea is significantly fewer items than 10.
                    assert!(
                        results.len() >= 2 && results.len() <= 3,
                        "Expected 2 or 3 items for this timing, got {}. Results: {:?}", results.len(), results
                    );

                    // 2. First item should be ID 0.
                    if !results.is_empty() {
                        assert_eq!(results[0].id, 0, "First throttled item should be ID 0.");
                    }

                    // 3. Time difference between consecutive emitted items should be >= throttle_duration
                    if received_timestamps.len() > 1 {
                        for i in 0..(received_timestamps.len() - 1) {
                            let diff = received_timestamps[i+1].duration_since(received_timestamps[i]);
                            println!("[Main] Time diff between emitted item {} and {}: {:.3}s", results[i].id, results[i+1].id, diff.as_secs_f32());
                            assert!(
                                diff >= throttle_duration,
                                "Throttle violation: Time between ID {} and ID {} is {:?}, less than throttle duration {:?}",
                                results[i].id, results[i+1].id, diff, throttle_duration
                            );
                        }
                    }
                    println!("\n[Main] Verification assertions passed.");
                }
                Err(_) => eprintln!("[Main] Throttled stream processing failed."),
            }

            // Wait for emitter to fully complete its sending logic before example ends.
            emitter_handle.await.expect("Emitter task failed");
            println!("[Main] Emitter task confirmed complete (MPSC channel closed).");
            println!("\n[Main] Throttle Example: Complete.");
        }).await;
    });
}
