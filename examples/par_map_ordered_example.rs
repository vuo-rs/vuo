use actix_rt;
use futures::FutureExt; // For .boxed()
use std::time::Duration;
use tokio::task::LocalSet;
use vuo::Stream; // Assuming Vuo::Streamable is re-exported or accessible

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OrderTestItem {
    id: u32,
    description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProcessedOrderTestItem {
    original_id: u32,
    processed_description: String,
    processing_time_ms: u64,
}

// Ensure structs are Streamable. The blanket impl in vuo::stream::streamable.rs should cover this,
// as long as they satisfy 'static + Send + Unpin + Debug.
// If vuo::Streamable is not directly in scope, this might need `use vuo::stream::Streamable;`
// and then explicit impls. For now, relying on blanket impl.
// E.g., if needed:
// impl vuo::stream::Streamable for OrderTestItem {}
// impl vuo::stream::Streamable for ProcessedOrderTestItem {}

// Asynchronous mapping function that simulates work with variable delays
async fn process_item_with_delay(item: OrderTestItem) -> ProcessedOrderTestItem {
    // Assign processing times such that faster items (lower duration) might finish before slower, earlier items
    let duration_ms = match item.id % 4 {
        0 => 600, // ID 0, 4 - Slowest
        1 => 100, // ID 1, 5 - Fastest
        2 => 400, // ID 2, 6 - Medium
        _ => 250, // ID 3, 7 - Average
    };

    println!(
        "[Processor] Start  ID: {}, Desc: '{}'. Will take {}ms.",
        item.id, item.description, duration_ms
    );
    tokio::time::sleep(Duration::from_millis(duration_ms)).await;

    let processed_description = format!(
        "{} (ID: {} processed in {}ms)",
        item.description.to_uppercase(),
        item.id,
        duration_ms
    );
    println!("[Processor] Finish ID: {}.", item.id);

    ProcessedOrderTestItem {
        original_id: item.id,
        processed_description,
        processing_time_ms: duration_ms,
    }
}

fn main() {
    // Configure Actix System to use a multi-threaded Tokio runtime
    // This is important if the async operations in par_map_ordered (like process_item_with_delay)
    // use tokio::task::spawn_blocking for true CPU parallelism.
    // For simple tokio::time::sleep, a current-thread runtime would also show concurrency.
    let system = actix_rt::System::with_tokio_rt(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4) // Example: 4 worker threads
            .enable_all() // Enable IO, time, etc., for the Tokio runtime
            .build()
            .expect("Failed to build Tokio multi-threaded runtime")
    });

    system.block_on(async {
        // Use LocalSet for consistency with other examples and if any vuo internal
        // relies on task::spawn_local from the calling context.
        let local_set = LocalSet::new();
        local_set.run_until(async {
            println!("[Main] par_map_ordered Example: Starting");

            let items_to_process = (0..8_u32).map(|i| OrderTestItem {
                id: i,
                description: format!("OrderEvent-{}", i),
            }).collect::<Vec<_>>();

            let source_stream = Stream::emits(items_to_process.clone());

            let parallelism_level = 3; // Max concurrent async map operations
            println!(
                "[Main] Applying par_map_ordered with parallelism: {}",
                parallelism_level
            );

            // Apply par_map_ordered
            // The closure takes an OrderTestItem and returns a BoxFuture<'static, ProcessedOrderTestItem>
            let processed_stream = source_stream.par_map_ordered(
                parallelism_level,
                move |item: OrderTestItem| {
                    // process_item_with_delay is an async fn, its call returns a Future.
                    // .boxed() converts it to BoxFuture (which is Pin<Box<dyn Future + Send>>)
                    process_item_with_delay(item).boxed()
                },
            );

            println!("[Main] Collecting results from par_map_ordered stream...");
            match processed_stream.compile_to_list().await {
                Ok(results) => {
                    println!(
                        "\n[Main] par_map_ordered results ({} items, should be in original ID order):",
                        results.len()
                    );
                    let mut emitted_ids_in_order = Vec::new();
                    for (idx, res) in results.iter().enumerate() {
                        println!(
                            "  Result {}: Original ID: {}, Payload: '{}', Took: {}ms",
                            idx, res.original_id, res.processed_description, res.processing_time_ms
                        );
                        emitted_ids_in_order.push(res.original_id);
                    }

                    // Verification
                    assert_eq!(
                        results.len(),
                        items_to_process.len(),
                        "Number of processed items does not match number of input items."
                    );

                    // CRITICAL ASSERTION: Check if IDs in the results are in the original sequential order
                    let original_ids: Vec<u32> = items_to_process.iter().map(|i| i.id).collect();
                    assert_eq!(
                        emitted_ids_in_order, original_ids,
                        "Results are not in original ID order! Expected: {:?}, Got: {:?}",
                        original_ids, emitted_ids_in_order
                    );

                    println!(
                        "\n[Main] Verification successful: All items processed and results are in original order."
                    );
                    println!("[Main] Observe [Processor] start/finish logs to see out-of-order execution vs. the in-order final results list.");
                }
                Err(_) => {
                    eprintln!("[Main] par_map_ordered stream processing failed.");
                }
            }

            println!("\n[Main] par_map_ordered Example: Complete.");
        }).await; // End of local_set.run_until
    }); // End of system.block_on
}
