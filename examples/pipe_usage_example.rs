use tokio::task::LocalSet;
use vuo::{Pipe, Stream};

#[tokio::main]
async fn main() {
    let local = LocalSet::new();
    local
        .run_until(async {
            println!("--- Pipe Usage Example ---");

            // Create an initial stream of numbers
            let input_elements = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            println!("\nInitial data: {:?}", input_elements);
            let initial_stream = Stream::emits(input_elements.clone());

            // Define a pipe to filter even numbers
            let filter_even_pipe: Pipe<i32, i32> = Pipe::new(|stream: Stream<i32>| {
                println!("Step 1: Filtering even numbers...");
                stream.filter(|x| x % 2 == 0) // e.g., for input [1..10] -> [2, 4, 6, 8, 10]
            });

            // Define a pipe to multiply by 3
            // This pipe takes i32 (output of filter_even_pipe) and produces i32
            let multiply_by_three_pipe: Pipe<i32, i32> = Pipe::new(|stream: Stream<i32>| {
                println!("Step 2: Multiplying by 3...");
                stream.map(|x| x * 3) // e.g., for input [2,4,6,8,10] -> [6, 12, 18, 24, 30]
            });

            // Define a pipe to convert numbers to strings with a prefix
            // This pipe takes i32 (output of multiply_by_three_pipe) and produces String
            let to_string_pipe: Pipe<i32, String> = Pipe::new(|stream: Stream<i32>| {
                println!("Step 3: Converting to String...");
                stream.map(|x| format!("Value: {}", x)) // e.g., for input [6,12..] -> ["Value: 6", "Value: 12", ...]
            });

            // Define a pipe to take only the first two string elements
            // This pipe takes String (output of to_string_pipe) and produces String
            let take_two_pipe: Pipe<String, String> = Pipe::new(|stream: Stream<String>| {
                println!("Step 4: Taking first 2 elements...");
                stream.take(2) // e.g., for input ["Value: 6", "Value: 12", ..] -> ["Value: 6", "Value: 12"]
            });

            // Compose the pipes using `and_then`
            // The type of `composed_pipe` will be Pipe<i32 (initial In), String (final Out)>
            println!(
                "\nComposing pipes: filter_even -> multiply_by_three -> to_string -> take_two"
            );
            let composed_pipe = filter_even_pipe // Output type: i32
                .and_then(multiply_by_three_pipe) // Input: i32, Output: i32. Overall: Pipe<i32, i32>
                .and_then(to_string_pipe) // Input: i32, Output: String. Overall: Pipe<i32, String>
                .and_then(take_two_pipe); // Input: String, Output: String. Overall: Pipe<i32, String>

            // Apply the composed pipe to the initial stream
            println!("\nApplying the composed pipe...");
            let final_stream = composed_pipe.apply(initial_stream);

            // Collect and print the results
            match final_stream.compile_to_list().await {
                Ok(results) => {
                    println!("\nFinal results after composed pipe:");
                    for item in &results {
                        println!("- {}", item);
                    }
                    // Expected transformation:
                    // Initial: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
                    // Step 1 (Filter even): [2, 4, 6, 8, 10]
                    // Step 2 (Multiply by 3): [6, 12, 18, 24, 30]
                    // Step 3 (To String): ["Value: 6", "Value: 12", "Value: 18", "Value: 24", "Value: 30"]
                    // Step 4 (Take 2): ["Value: 6", "Value: 12"]
                    let expected_results = vec!["Value: 6".to_string(), "Value: 12".to_string()];
                    assert_eq!(
                        results, expected_results,
                        "The final results did not match the expected output."
                    );
                    println!("\nPipe `and_then` example finished successfully!");
                }
                Err(e) => {
                    eprintln!("\nError collecting stream results: {:?}", e);
                }
            }

            // Briefly demonstrate Pipe::compose
            // `pipe_c = pipe_b.compose(pipe_a)` means `pipe_a` is applied first, then `pipe_b`.
            // Let p0: A -> B and p1: B -> C. Then `p1.compose(p0)` results in a Pipe<A, C>.

            println!("\n--- Demonstrating Pipe::compose ---");
            let elements_for_compose = vec![1, 6, 2, 7, 3, 8, 4, 9]; // Input for this part
            println!(
                "Initial data for compose example: {:?}",
                elements_for_compose
            );
            let stream_for_compose = Stream::emits(elements_for_compose);

            let p_filter_lt_5: Pipe<i32, i32> = Pipe::new(|s| {
                println!("Compose Step 1: Filtering numbers less than 5...");
                s.filter(|x| *x < 5) // e.g. [1,2,3,4]
            });
            let p_format_num: Pipe<i32, String> = Pipe::new(|s| {
                println!("Compose Step 2: Formatting numbers to strings...");
                s.map(|x| format!("N:{}", x)) // e.g. ["N:1", "N:2", "N:3", "N:4"]
            });

            // We want: p_filter_lt_5 (i32->i32) then p_format_num (i32->String)
            // This is achieved by: p_format_num.compose(p_filter_lt_5)
            let composed_with_compose_method: Pipe<i32, String> =
                p_format_num.compose(p_filter_lt_5);

            println!("\nApplying the 'compose' method pipe...");
            match composed_with_compose_method
                .apply(stream_for_compose)
                .compile_to_list()
                .await
            {
                Ok(res) => {
                    println!("Results from 'compose' chain: {:?}", res);
                    let expected_compose_results = vec![
                        "N:1".to_string(),
                        "N:2".to_string(),
                        "N:3".to_string(),
                        "N:4".to_string(),
                    ];
                    assert_eq!(
                        res, expected_compose_results,
                        "The compose results did not match."
                    );
                    println!("\nPipe `compose` example finished successfully!");
                }
                Err(e) => eprintln!("Error in compose example: {:?}", e),
            }
            println!("\n--- Pipe Usage Example End ---");
        })
        .await;
}
