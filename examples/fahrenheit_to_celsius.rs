use tokio::task::LocalSet; // Import LocalSet
use vuo::Stream; // Assuming Stream is directly available under vuo

// Function to convert Fahrenheit to Celsius
fn fahrenheit_to_celsius(f: f64) -> f64 {
    (f - 32.0) * 5.0 / 9.0
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("Fahrenheit to Celsius Conversion Example");

    let local_set = LocalSet::new(); // Create a new LocalSet

    local_set
        .run_until(async {
            // Run the stream processing logic within the LocalSet
            let fahrenheit_values = vec![
                0.0, 32.0, 68.0, 100.0, 212.0, -40.0, -500.0, /* invalid */
            ];

            // Absolute zero in Fahrenheit
            let absolute_zero_f = -459.67;

            let source_stream = Stream::emits(fahrenheit_values);

            let celsius_stream = source_stream
                .filter(move |&f_temp| {
                    let is_valid = f_temp >= absolute_zero_f;
                    if !is_valid {
                        // println!("Filtering out invalid temperature: {:.2} F", f_temp);
                    }
                    is_valid
                })
                .map(|f_temp| {
                    let c_temp = fahrenheit_to_celsius(f_temp);
                    // println!("Converted {:.2} F to {:.2} C", f_temp, c_temp);
                    c_temp
                });

            println!("\nConverted Celsius temperatures:");

            // Using eval_map for side-effects (like printing) and then drain.
            // eval_map expects an async closure.
            let final_stream = celsius_stream.eval_map(|c_temp| {
                println!("{:.2} °C", c_temp);
                async {} // eval_map's closure needs to return a future, here Future<Output = ()>
            });

            // The stream processing starts when a terminal operation like drain (or compile_to_list, etc.)
            // is awaited or its setup is executed.
            // In Vuo, drain itself returns a new Stream<()>, which also needs a terminal operation.
            // For this example, we'll use compile_to_list() on the unit stream to run it.
            // If drain was meant to be the final consumer, it'd typically be awaited if it returned a Future<()>.\
            // Given Vuo's design, the result of drain() is another Stream, so:
            let _ = final_stream.compile_to_list().await;

            // Alternative: collect and print
            // let results = celsius_stream.compile_to_list().await;
            // if let Some(temps) = results {
            //     for temp in temps {
            //         println!("{:.2} °C", temp);
            //     }
            // } else {
            //     println!("Stream processing failed or produced no output.");
            // }

            println!("\nProcessing complete.");
        })
        .await; // Await the completion of the LocalSet's execution
}
