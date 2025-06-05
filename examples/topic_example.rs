use actix_rt;
use std::time::Duration;
use tokio::task::{self, LocalSet}; // Import LocalSet and task for spawn_local
use vuo::{Stream, Topic};

#[derive(Debug, Clone)]
struct Message {
    id: u32,
    content: String,
}

fn main() {
    // Create an Actix system. This will also initialize a Tokio runtime.
    // By default, System::new() uses a current-thread Tokio runtime.
    let system = actix_rt::System::new();

    // Run the main async logic within the Actix system.
    system.block_on(async {
        // Create a LocalSet to provide context for tokio::task::spawn_local.
        let local_set = LocalSet::new();

        // Run the main application logic on this LocalSet.
        local_set.run_until(async {
            println!("[Main] Topic Example: Starting (Actix System with inner LocalSet)");

            // 1. Create a new topic.
            let topic: Topic<Message> = Topic::new();
            println!("[Main] Topic created.");

            // 2. Subscriber 1 (starts before any publishing)
            let topic_for_subscriber1 = topic.clone();
            // Use task::spawn_local as this task will call Topic::subscribe,
            // which internally may use task::spawn_local.
            let subscriber1_handle = task::spawn_local(async move {
                println!("[Subscriber 1] Subscribing to topic.");
                let stream1: Stream<Message> = topic_for_subscriber1.subscribe();
                println!("[Subscriber 1] Subscribed. Waiting for messages...");

                match stream1.compile_to_list().await {
                    Ok(msgs) => {
                        println!("[Subscriber 1] Stream ended. Received {} messages:", msgs.len());
                        for msg in msgs {
                            println!("[Subscriber 1] > ID: {}, Content: '{}'", msg.id, msg.content);
                        }
                    }
                    Err(_) => eprintln!("[Subscriber 1] Stream processing failed or was closed prematurely."),
                }
                println!("[Subscriber 1] Finished.");
            });

            // Brief pause to allow Subscriber 1 to set up.
            tokio::time::sleep(Duration::from_millis(100)).await;

            // 3. Publisher Task
            let topic_for_publisher = topic.clone();
            // This task also spawns Subscriber 2 using task::spawn_local, so it should also be local.
            let publisher_handle = task::spawn_local(async move {
                println!("[Publisher] Starting to publish first batch of messages...");
                for i in 0..3 { // Publish first batch (IDs 0, 1, 2)
                    let msg = Message { id: i, content: format!("Message Batch 1 - Item {}", i) };
                    println!("[Publisher] Publishing Msg ID: {}", msg.id);
                    topic_for_publisher.publish(msg);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }

                println!("[Publisher] First batch published. Spawning Subscriber 2...");

                let topic_for_subscriber2 = topic_for_publisher.clone();
                // This nested spawn must also be task::spawn_local because it calls subscribe.
                let subscriber2_handle = task::spawn_local(async move {
                    println!("[Subscriber 2] Subscribing to topic.");
                    let stream2: Stream<Message> = topic_for_subscriber2.subscribe();
                    println!("[Subscriber 2] Subscribed. Waiting for messages...");
                    match stream2.compile_to_list().await {
                        Ok(msgs) => {
                            println!("[Subscriber 2] Stream ended. Received {} messages:", msgs.len());
                            for msg in msgs {
                                 println!("[Subscriber 2] > ID: {}, Content: '{}'", msg.id, msg.content);
                            }
                        }
                        Err(_) => eprintln!("[Subscriber 2] Stream processing failed or was closed prematurely."),
                    }
                    println!("[Subscriber 2] Finished.");
                });

                // Brief pause for Subscriber 2 to set up.
                tokio::time::sleep(Duration::from_millis(100)).await;

                println!("[Publisher] Publishing second batch of messages (Subscriber 2 should see these)...");
                for i in 3..6 { // Publish second batch (IDs 3, 4, 5)
                    let msg = Message { id: i, content: format!("Message Batch 2 - Item {}", i) };
                    println!("[Publisher] Publishing Msg ID: {}", msg.id);
                    topic_for_publisher.publish(msg);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }

                println!("[Publisher] All messages published. Closing topic.");
                topic_for_publisher.close();
                println!("[Publisher] Topic close signal sent.");

                if let Err(e) = subscriber2_handle.await {
                     eprintln!("[Publisher] Subscriber 2 task panicked or failed: {:?}", e);
                } else {
                    println!("[Publisher] Subscriber 2 task completed.");
                }
                println!("[Publisher] Publisher task finished.");
            });


            // Wait for all main tasks to complete
            if let Err(e) = publisher_handle.await {
                eprintln!("[Main] Publisher task panicked or failed: {:?}", e);
            } else {
                println!("[Main] Publisher task completed.");
            }

            if let Err(e) = subscriber1_handle.await {
                 eprintln!("[Main] Subscriber 1 task panicked or failed: {:?}", e);
            } else {
                println!("[Main] Subscriber 1 task completed.");
            }

            println!("\n[Main] Topic Example: Complete.");
            println!("[Main] Expected behavior check:");
            println!("  - Subscriber 1 should have received messages with IDs 0 through 5.");
            println!("  - Subscriber 2 should have received messages with IDs 3 through 5.");

            // Optional: Explicitly stop the Actix system if needed.
            // actix_rt::System::current().stop();
        }).await; // End of local_set.run_until
    }); // End of system.block_on
}
