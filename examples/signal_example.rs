use actix_rt;
use std::time::Duration;
use tokio::task::{self, LocalSet};
use vuo::{Signal, Stream}; // Stream is used by signal.discrete()

// Item for the signal. Must satisfy 'static + Send + Clone + Unpin + Debug + Streamable.
// CloneableStreamable (from virta::stream::streamable) covers these if A is Clone.
#[derive(Debug, Clone, PartialEq)] // PartialEq for assertions
struct AppConfig {
    feature_x_enabled: bool,
    retry_attempts: u32,
}

// Ensure AppConfig meets Streamable.
// The virta::stream::Streamable trait requires 'static + Send + Unpin + Debug.
// Our AppConfig struct, with its fields (bool, u32) and derived Debug + Clone,
// will automatically satisfy Send, Sync, Unpin, and 'static under normal circumstances.
// This explicit impl is to make it absolutely clear for the compiler if needed,
// assuming virta::stream::Streamable is accessible.
// The blanket impl in virta/src/stream/streamable.rs will cover AppConfig.
// impl virta::stream::Streamable for AppConfig {}

fn main() {
    let system = actix_rt::System::new();
    system.block_on(async {
        let local_set = LocalSet::new();
        local_set
            .run_until(async {
                println!("[Main] Signal Example: Starting");

                // 1. Create a signal with an initial configuration.
                let initial_config = AppConfig {
                    feature_x_enabled: false,
                    retry_attempts: 3,
                };
                let config_signal: Signal<AppConfig> = Signal::new(initial_config.clone());
                println!("[Main] Initial config signal created: {:?}", initial_config);

                // 2. Subscriber to the discrete stream of config changes.
                let discrete_stream_signal = config_signal.clone();
                let subscriber_handle = task::spawn_local(async move {
                    println!("[Subscriber] Subscribing to discrete config changes.");
                    let config_stream: Stream<AppConfig> = discrete_stream_signal.discrete();

                    let mut collected_configs_for_sub = Vec::new();
                    // Take 3 states: initial + first two changes to make the test deterministic.
                    let limited_stream = config_stream.take(3);

                    match limited_stream.compile_to_list().await {
                        Ok(configs) => {
                            println!(
                                "[Subscriber] Collected {} config states from discrete stream:",
                                configs.len()
                            );
                            for (idx, cfg) in configs.iter().enumerate() {
                                println!("[Subscriber] State {}: {:?}", idx, cfg);
                                collected_configs_for_sub.push(cfg.clone());
                            }
                        }
                        Err(_) => eprintln!("[Subscriber] Config stream processing failed."),
                    }
                    println!("[Subscriber] Finished collecting discrete stream.");
                    collected_configs_for_sub
                });

                // 3. Task that modifies the signal.
                let modifier_signal = config_signal.clone();
                let modifier_task_handle = task::spawn_local(async move {
                    println!("[Modifier] Will update config after a short delay.");
                    tokio::time::sleep(Duration::from_millis(200)).await; // Give subscriber time to subscribe

                    let new_config_1 = AppConfig {
                        feature_x_enabled: true,
                        retry_attempts: 3,
                    };
                    println!("[Modifier] Setting config to (1): {:?}", new_config_1);
                    modifier_signal.set_value(new_config_1.clone()); // set_value is sync

                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let current_val_check1 = modifier_signal.get_value().await;
                    println!(
                        "[Modifier] Current signal value after set 1: {:?}",
                        current_val_check1
                    );
                    assert_eq!(current_val_check1.as_ref(), Some(&new_config_1));

                    let new_config_2 = AppConfig {
                        feature_x_enabled: true,
                        retry_attempts: 5,
                    };
                    println!("[Modifier] Setting config to (2): {:?}", new_config_2);
                    modifier_signal.set_value(new_config_2.clone());

                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let current_val_check2 = modifier_signal.get_value().await;
                    println!(
                        "[Modifier] Current signal value after set 2: {:?}",
                        current_val_check2
                    );
                    assert_eq!(current_val_check2.as_ref(), Some(&new_config_2));

                    // This third change is to test if subscriber correctly stops after take(3)
                    // or if it might pick it up due to timing. `take(3)` should make it ignore this.
                    let new_config_3 = AppConfig {
                        feature_x_enabled: false,
                        retry_attempts: 1,
                    };
                    println!("[Modifier] Setting config to (3): {:?}", new_config_3);
                    modifier_signal.set_value(new_config_3.clone());
                    println!("[Modifier] All modifications sent.");
                });

                // Wait for modifier to finish its planned changes
                modifier_task_handle.await.expect("Modifier task panicked");
                println!("[Main] Modifier task completed.");

                // Wait for subscriber to collect its expected number of states
                let received_configs = subscriber_handle.await.expect("Subscriber task panicked");

                println!("[Main] Assertions for received configs by subscriber:");
                assert_eq!(
                    received_configs.len(),
                    3,
                    "Subscriber should have collected 3 config states due to take(3)"
                );
                if !received_configs.is_empty() {
                    assert_eq!(
                        received_configs[0], initial_config,
                        "First received config should be the initial one."
                    );
                }
                if received_configs.len() >= 2 {
                    assert_eq!(
                        received_configs[1],
                        AppConfig {
                            feature_x_enabled: true,
                            retry_attempts: 3
                        },
                        "Second received config mismatch."
                    );
                }
                if received_configs.len() >= 3 {
                    assert_eq!(
                        received_configs[2],
                        AppConfig {
                            feature_x_enabled: true,
                            retry_attempts: 5
                        },
                        "Third received config mismatch."
                    );
                }

                // Example of getting a value directly after all modifications
                let final_value_check = config_signal.get_value().await;
                println!(
                    "[Main] Final value check on signal directly (after all changes): {:?}",
                    final_value_check
                );
                assert_eq!(
                    final_value_check,
                    Some(AppConfig {
                        feature_x_enabled: false,
                        retry_attempts: 1
                    })
                );

                println!("[Main] Signal Example: Complete.");

                // Optional: Stop the system if it doesn't exit automatically.
                // actix_rt::System::current().stop();
            })
            .await;
    });
}
