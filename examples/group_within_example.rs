use actix::Recipient;
use actix_rt;
use futures::FutureExt; // For .boxed()
use futures::SinkExt; // for mpsc::Sender::send
use futures::StreamExt as FuturesStreamExt; // for mpsc::Receiver::next
use futures::channel::mpsc;
use std::marker::PhantomData;
use std::time::Duration;
use tokio::task::{self, LocalSet};
use vuo::Stream;
use vuo::stream::StreamMessage;

#[derive(Debug, Clone, PartialEq)]
struct DataEvent {
    id: u32,
    value: String,
}

fn main() {
    let system = actix_rt::System::new();
    system.block_on(async {
        let local_set = LocalSet::new();
        local_set.run_until(async {
            println!("[Main] group_within Example: Starting");

            let group_max_size = 3;
            let group_max_duration = Duration::from_millis(500);

            // --- Emitter Task & Manual Source Stream ---
            let (mut tx_emitter_mpsc, rx_emitter_mpsc) =
                mpsc::unbounded::<StreamMessage<DataEvent>>();

            let emitter_handle = task::spawn_local(async move {
                println!("[Emitter] Starting emissions...");

                // Batch 1 (emitted by size) - Items 0, 1, 2
                for i in 0..group_max_size {
                    let event = DataEvent { id: i as u32, value: format!("Item-{}", i) };
                    println!("[Emitter] Sending id: {}", event.id);
                    if tx_emitter_mpsc.send(StreamMessage::Element(event)).await.is_err() {
                        println!("[Emitter] Error sending Batch 1 item to MPSC. Receiver likely dropped.");
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                println!("[Emitter] Batch 1 (IDs 0,1,2 for size-trigger) sent.");

                // Batch 2 (should be emitted by time) - Items 3, 4
                let batch2_id_start = group_max_size as u32;
                let event_id_3 = batch2_id_start;
                let event_id_4 = batch2_id_start + 1;

                println!("[Emitter] Sending id: {}", event_id_3);
                let event3 = DataEvent { id: event_id_3, value: format!("Item-{}", event_id_3) };
                if tx_emitter_mpsc.send(StreamMessage::Element(event3)).await.is_err() { return; }
                tokio::time::sleep(Duration::from_millis(50)).await;

                println!("[Emitter] Sending id: {}", event_id_4);
                let event4 = DataEvent { id: event_id_4, value: format!("Item-{}", event_id_4) };
                if tx_emitter_mpsc.send(StreamMessage::Element(event4)).await.is_err() { return; }

                println!("[Emitter] Batch 2 (IDs {},{}) sent. Waiting for their timeout ({}ms + margin)...", event_id_3, event_id_4, group_max_duration.as_millis());
                tokio::time::sleep(group_max_duration + Duration::from_millis(2000)).await; // Wait for timeout + processing margin (increased to 2000ms)

                // Batch 3 (final single item, should be flushed by End) - Item 5
                let final_event_id = batch2_id_start + 2;
                let event_final = DataEvent { id: final_event_id, value: "FinalItem".to_string() };
                println!("[Emitter] Sending final id: {}", event_final.id);
                if tx_emitter_mpsc.send(StreamMessage::Element(event_final)).await.is_err() { return; }

                println!("[Emitter] All test items sent. Sending End via MPSC.");
                if tx_emitter_mpsc.send(StreamMessage::End).await.is_err() {
                     println!("[Emitter] Error sending End to MPSC.");
                }
            });

            let source_stream_setup_fn = Box::new(
                move |downstream_recipient: Recipient<StreamMessage<DataEvent>>| {
                    let mut rx_mpsc_clone = rx_emitter_mpsc;
                    async move {
                        while let Some(msg) = FuturesStreamExt::next(&mut rx_mpsc_clone).await {
                            if downstream_recipient.try_send(msg).is_err() { return Err(String::from("Downstream consumer closed for group_within example source")); }
                        }
                        let _ = downstream_recipient.try_send(StreamMessage::End);
                        Ok(())
                    }.boxed()
                },
            );
            let source_stream = Stream { setup_fn: source_stream_setup_fn, _phantom: PhantomData };

            println!("[Main] Applying group_within(size={}, duration={:?})", group_max_size, group_max_duration);
            let grouped_stream: Stream<Vec<DataEvent>> = source_stream.group_within(group_max_size, group_max_duration);

            println!("[Main] Collecting results from grouped stream...");
            let grouped_results_handle = task::spawn_local(async move {
                grouped_stream.compile_to_list().await
            });

            if emitter_handle.await.is_err() {
                eprintln!("[Main] Emitter task panicked or failed.");
            } else {
                println!("[Main] Emitter task completed.");
            }

            match grouped_results_handle.await.expect("Grouped stream task panicked") {
                Ok(chunks) => {
                    println!("\n[Main] Grouped results ({} chunks):", chunks.len());
                    for (idx, chunk) in chunks.iter().enumerate() {
                        println!("  Chunk {}: ({} items)", idx, chunk.len());
                        for item_in_chunk in chunk { // Renamed to avoid conflict with outer `event`
                            println!("    -> ID: {}, Value: '{}'", item_in_chunk.id, item_in_chunk.value);
                        }
                    }

                    assert_eq!(chunks.len(), 3, "Expected 3 chunks, but got {}. Chunks: {:?}", chunks.len(), chunks);

                    if chunks.len() >= 1 {
                        assert_eq!(chunks[0].len(), group_max_size, "Chunk 0 (IDs 0,1,2 by size) wrong size. Actual: {:?}", chunks[0]);
                        if chunks[0].len() == group_max_size {
                            assert_eq!(chunks[0][0].id, 0);
                            assert_eq!(chunks[0][1].id, 1);
                            assert_eq!(chunks[0][2].id, 2);
                        }
                    }
                    if chunks.len() >= 2 {
                        assert_eq!(chunks[1].len(), 2, "Chunk 1 (IDs 3,4 by time) wrong size. Actual: {:?}", chunks[1]);
                        if chunks[1].len() == 2 {
                             assert_eq!(chunks[1][0].id, 3);
                             assert_eq!(chunks[1][1].id, 4);
                        }
                    }
                     if chunks.len() >= 3 {
                        assert_eq!(chunks[2].len(), 1, "Chunk 2 (ID 5 by end) wrong size. Actual: {:?}", chunks[2]);
                         if chunks[2].len() == 1 {
                            assert_eq!(chunks[2][0].id, 5);
                            assert_eq!(chunks[2][0].value, "FinalItem");
                        }
                    }
                    let total_received_items: usize = chunks.iter().map(|c| c.len()).sum();
                    assert_eq!(total_received_items, group_max_size + 2 + 1, "Total number of items mismatch");

                    println!("\n[Main] Verification successful.");
                }
                Err(_) => eprintln!("[Main] Grouped stream processing failed."),
            }

            println!("\n[Main] group_within Example: Complete.");
        }).await;
    });
}
