use std::time::Duration;
use tokio::task::LocalSet;
use vuo::Queue;
use vuo::queue::QueueOfferError;

// Items in the queue must be CloneableStreamable.
// For virtaus, Streamable implies: Send + Sync + Clone + std::fmt::Debug + 'static.
// i32 satisfies these requirements.

#[tokio::main]
async fn main() {
    let local_set = LocalSet::new();

    local_set.run_until(async {
        println!("[Main] Queue Example: Starting");

        // Create a bounded queue. A small capacity helps demonstrate backpressure.
        let queue_capacity = 3;
        let queue: Queue<i32> = Queue::new(queue_capacity);
        println!("[Main] Created queue with capacity {}.", queue_capacity);

        // Producer task
        let producer_queue_clone = queue.clone();
        let num_items_to_produce = 7;

        let producer_handle = tokio::spawn(async move {
            for i in 0..num_items_to_produce {
                println!("[Producer] Attempting to offer item: {}", i);
                let mut item_to_offer = i; // mutable because offer can return it on Full
                loop { // Loop to retry if queue is full
                    match producer_queue_clone.offer(item_to_offer).await {
                        Ok(_) => {
                            println!("[Producer] Successfully offered item: {}", item_to_offer);
                            break; // Exit retry loop, move to next item
                        }
                        Err(QueueOfferError::Full(returned_item)) => {
                            item_to_offer = returned_item; // Store returned item for retry
                            eprintln!("[Producer] Failed to offer {}: Queue is full. Retrying in 50ms.", item_to_offer);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            // Retry with the same item
                        }
                        Err(QueueOfferError::Closed(returned_item)) => {
                            eprintln!("[Producer] Failed to offer {}: Queue is closed. Producer stopping.", returned_item);
                            return; // Stop producer if queue is closed
                        }
                    }
                }
                // Brief pause to simulate work and allow consumer to potentially fall behind
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            println!("[Producer] All {} items offered. Closing queue.", num_items_to_produce);
            producer_queue_clone.close(); // close() is synchronous
            println!("[Producer] Queue closed by producer.");
        });

        // Consumer stream
        // Use the new dequeue_stream() method from VirtaQueue
        let consumer_queue_clone = queue.clone();
        let consuming_stream = consumer_queue_clone.dequeue_stream();

        // Add some logging to the consuming stream itself for clarity, if possible,
        // or rely on the behavior of dequeue_stream and then log the collected items.
        // For this example, the internal logs in dequeue_stream (if enabled) and the final
        // collection will show the behavior. If direct logging per item processed by
        // the stream before collection is needed, we'd use .eval_map or similar.
        // Here, we will just collect and print.

        println!("[Main] Setting up consumer stream to collect items...");
        // The stream won't start processing until a terminal operation like compile_to_list is awaited.
        let results = consuming_stream.compile_to_list().await;

        // Wait for producer to finish to ensure all logs are captured and it didn't panic.
        if let Err(e) = producer_handle.await {
            eprintln!("[Main] Producer task panicked: {:?}", e);
        } else {
            println!("[Main] Producer task completed.");
        }

        match results {
            Ok(items) => {
                println!("[Main] Collected items from queue ({} items):", items.len());
                if items.is_empty() {
                    println!("No items were collected.");
                } else {
                    for (idx, item) in items.iter().enumerate() {
                        println!("Item {}: {}", idx, item);
                    }
                }
                // Verify all items were received in order
                let expected_items: Vec<i32> = (0..num_items_to_produce).collect();
                assert_eq!(items, expected_items, "Collected items do not match expected items.");
                println!("[Main] Item verification successful.");
            }
            Err(_) => {
                // compile_to_list() for Stream<T> returns Result<Vec<T>, ()>
                println!("[Main] Stream processing failed (compile_to_list returned Err).");
            }
        }
        println!("[Main] Queue Example: Complete");
    }).await;
}
