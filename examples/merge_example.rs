use actix_rt;
use futures::FutureExt;
use tokio::time::{Duration, sleep};
use vuo::Stream; // For .boxed()

async fn run_merge_example() {
    let local_set = tokio::task::LocalSet::new();
    local_set
        .run_until(async {
            println!("Setting up streams for merge example...");

            // Stream 1: Emits 1, 2, 3. Each item is "processed" with a 50ms delay.
            // We use par_map_ordered with parallelism 1 to simulate sequential asynchronous item production.
            let stream1_source_data = vec![1, 2, 3];
            let stream1 = Stream::emits(stream1_source_data).par_map_ordered(1, |item| {
                async move {
                    // println!("Stream 1: Processing {} (will take 50ms)", item); // Uncomment for verbose logging
                    sleep(Duration::from_millis(50)).await;
                    // println!("Stream 1: Emitting {}", item); // Uncomment for verbose logging
                    item
                }
                .boxed() // Ensure the future is BoxFuture<'static, _> + Send
            });

            // Stream 2: Emits 101, 102, 103. Each item is "processed" with a 30ms delay.
            // This stream is slightly faster per item, which should lead to interleaving.
            let stream2_source_data = vec![101, 102, 103];
            let stream2 = Stream::emits(stream2_source_data).par_map_ordered(1, |item| {
                // Renamed item to avoid capture issues if any, though not strictly needed here
                async move {
                    // println!("Stream 2: Processing {} (will take 30ms)", item); // Uncomment for verbose logging
                    sleep(Duration::from_millis(30)).await;
                    // println!("Stream 2: Emitting {}", item); // Uncomment for verbose logging
                    item
                }
                .boxed() // Ensure the future is BoxFuture<'static, _> + Send
            });

            println!("Merging streams...");
            let merged_stream = stream1.merge(stream2);

            println!("Collecting results from merged stream...");
            let results = merged_stream.compile_to_list().await;

            match results {
                Ok(mut data) => {
                    println!(
                        "Merged results (order may vary due to concurrency): {:?}",
                        data
                    );

                    // Verify that all items from both streams are present.
                    // The exact order of interleaving is non-deterministic.
                    // For stable verification, we sort the collected data.
                    let mut expected_data = vec![1, 2, 3, 101, 102, 103];

                    // Check length first
                    assert_eq!(
                        data.len(),
                        expected_data.len(),
                        "Merged data length does not match expected length."
                    );

                    // Sort both for comparison
                    data.sort();
                    expected_data.sort(); // Should already be sorted, but good practice.

                    println!("Merged results (sorted for verification): {:?}", data);
                    assert_eq!(
                        data, expected_data,
                        "Sorted merged data does not match expected elements."
                    );
                    println!("Merge example successful! All items received and verified.");
                }
                Err(_) => {
                    eprintln!(
                        "Merge example failed: Stream compilation to list resulted in an error."
                    );
                    // Fail the test explicitly if there's an error
                    assert!(false, "Stream compilation failed");
                }
            }
        })
        .await;
}

fn main() {
    // Optional: Initialize a logger (like env_logger) to see any internal vuo logs
    // or the println! statements within the async blocks if you uncomment them.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();

    // Setup Actix system and run the example
    actix_rt::System::new().block_on(run_merge_example());
}
