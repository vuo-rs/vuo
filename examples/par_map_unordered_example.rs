use actix_rt;
use futures::FutureExt; // For .boxed()
use std::collections::HashSet;
use std::time::Duration;
use tokio::task::{self, LocalSet};
use vuo::Stream;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct InputItem {
    id: u32,
    payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OutputItem {
    id: u32,
    transformed_payload: String,
    processing_duration_ms: u64,
}

// Asynchronous mapping function that simulates CPU-intensive work using spawn_blocking
async fn process_item_cpu_intensive(item: InputItem) -> OutputItem {
    let duration_ms = match item.id % 4 {
        0 => 500, // Slowest
        1 => 100, // Fastest
        2 => 300, // Medium
        _ => 200, // Average
    };

    println!(
        "[Processor] Starting ID: {}, Data: '{}'. CPU-bound simulation will take {}ms.",
        item.id, item.payload, duration_ms
    );

    let item_id_clone = item.id;
    let item_payload_clone = item.payload.clone();

    // Offload the "CPU-intensive" part to spawn_blocking
    let (transformed_payload_segment, actual_duration_ms) = task::spawn_blocking(move || {
        // This closure runs on a blocking thread from Tokio's pool.
        // Simulate CPU work with a standard thread sleep.
        std::thread::sleep(Duration::from_millis(duration_ms));

        let result_payload = format!(
            "{} (processed in {}ms on blocking thread)",
            item_payload_clone.to_uppercase(),
            duration_ms
        );
        (result_payload, duration_ms)
    })
    .await
    .expect("Spawn_blocking task failed"); // .await the JoinHandle

    println!("[Processor] Finished ID: {}.", item_id_clone);

    OutputItem {
        id: item_id_clone,
        transformed_payload: transformed_payload_segment,
        processing_duration_ms: actual_duration_ms,
    }
}

fn main() {
    // Configure Actix System to use a multi-threaded Tokio runtime
    let system = actix_rt::System::with_tokio_rt(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4) // Using 4 worker threads for parallelism
            .enable_all() // Enable IO, time, etc., for the Tokio runtime
            .build()
            .expect("Failed to build Tokio multi-threaded runtime")
    });

    // Run the main async logic within the Actix system.
    system.block_on(async {
        // LocalSet is still used here to maintain consistency with other examples
        // and in case some stream operations internally might use task::spawn_local.
        let local_set = LocalSet::new();
        local_set.run_until(async {
            println!("[Main] par_map_unordered Example (True Parallelism with Stream::emits): Starting");

            let items_to_process = vec![
                InputItem { id: 0, payload: "alpha_0".to_string() },
                InputItem { id: 1, payload: "bravo_1".to_string() },
                InputItem { id: 2, payload: "charlie_2".to_string() },
                InputItem { id: 3, payload: "delta_3".to_string() },
                InputItem { id: 4, payload: "echo_4".to_string() },
                InputItem { id: 5, payload: "foxtrot_5".to_string() },
                InputItem { id: 6, payload: "golf_6".to_string() },
                InputItem { id: 7, payload: "hotel_7".to_string() },
            ];
            // Using Stream::emits with the pre-defined vector
            let source_stream = Stream::emits(items_to_process.clone());

            let parallelism_level = 3;
            println!(
                "[Main] Applying par_map_unordered with parallelism: {} (each can spawn_blocking)",
                parallelism_level
            );

            // Apply par_map_unordered
            let processed_stream = source_stream.par_map_unordered(
                parallelism_level,
                move |item: InputItem| {
                    process_item_cpu_intensive(item).boxed() // .boxed() from futures::FutureExt
                },
            );

            println!("[Main] Collecting results from par_map_unordered stream...");
            match processed_stream.compile_to_list().await {
                Ok(mut results) => { // Made results mutable for sorting
                    println!(
                        "\n[Main] par_map_unordered results ({} items, order may vary from input):",
                        results.len()
                    );
                    for (idx, res) in results.iter().enumerate() {
                        println!(
                            "  Result {}: ID: {}, Payload: '{}', Duration: {}ms",
                            idx, res.id, res.transformed_payload, res.processing_duration_ms
                        );
                    }

                    // Verification
                    assert_eq!(
                        results.len(),
                        items_to_process.len(),
                        "Number of processed items does not match number of input items."
                    );

                    // Check that all original IDs are present in the results
                    let result_ids: HashSet<u32> = results.iter().map(|r| r.id).collect();
                    let input_ids: HashSet<u32> =
                        items_to_process.iter().map(|i| i.id).collect();
                    assert_eq!(
                        result_ids, input_ids,
                        "Set of processed item IDs does not match set of input item IDs."
                    );

                    println!(
                        "\n[Main] Verification successful: All items processed and accounted for."
                    );
                    println!("[Main] Observe [Processor] logs. True parallelism happens on spawn_blocking threads.");

                    // Sort results by ID for stable verification of content if needed
                    results.sort_by_key(|r| r.id);
                    println!("\n[Main] Results sorted by ID (for easier visual check):");
                    for res in &results {
                         println!("  Sorted ID: {}, Payload: '{}'", res.id, res.transformed_payload);
                    }
                }
                Err(_) => {
                    eprintln!("[Main] par_map_unordered stream processing failed.");
                }
            }

            println!("\n[Main] par_map_unordered Example (True Parallelism with Stream::emits): Complete.");
        }).await; // End of local_set.run_until

        // Optional: Explicitly stop the system (optional, as block_on will stop it when its future completes)
        // actix_rt::System::current().stop();
    }); // End of system.block_on
}
