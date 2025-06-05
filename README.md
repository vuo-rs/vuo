# Vuo: Asynchronous Stream Processing for Rust

Vuo is an asynchronous stream processing library for Rust, built on the Actix actor framework. It provides a flexible way to define, transform, and consume streams of data with a rich set of operators, inspired by functional streaming concepts.

## Overview

Vuo allows you to construct complex data processing pipelines that operate asynchronously. Each stream operation is typically managed by a dedicated actor, enabling concurrent processing while maintaining the defined stream semantics (e.g., sequential concatenation in `flat_map`, parallel execution in `par_map_unordered`).

The library is designed to be extensible and aims to provide a robust foundation for building reactive and data-intensive applications in Rust.

## Features

*   **Asynchronous Stream Processing**: Leverages Actix actors for non-blocking operations.
*   **Rich Set of Operators**: Includes common functional stream operators:
    *   Sources: `emits`, `future`, `unfold`, `eval`
    *   Transformations: `map`, `filter`, `flat_map` (alias `concat_map`), `scan`, `fold`, `chunks`
    *   Timing/Concurrency: `debounce`, `throttle`, `par_map_unordered`, `par_map_ordered`, `merge`, `zip`, `group_within`
    *   Side-effects: `eval_tap`, `drain`
    *   Control Flow: `take`, `take_while`, `drop_while`, `interrupt_when`
*   **Error Handling**: Provides mechanisms like `handle_error_with` and `on_finalize` for managing stream errors. Errors are typically propagated as `String` values.
*   **Actor-Based**: Each stream stage is an Actix actor, enabling fine-grained control and supervision if needed (though supervision is abstracted away by the `Stream` API).

## Getting Started

To use Vuo in your project, add it as a dependency in your `Cargo.toml`:

```toml
[dependencies]
vuo = "0.1.0" # Replace with the desired version
# Ensure actix and futures are also present
actix = "0.13"
futures = "0.3"
```

You'll need an Actix runtime to execute streams.

## Basic Usage

Here's a simple example of how to define and use a stream:

```rust
use vuo::Stream; // Assuming Stream is re-exported from lib.rs

async fn run_example() -> Result<Vec<i32>, String> {
    Stream::emits(vec![1, 2, 3, 4, 5, 6])
        .map(|x| x * 2) // Stream: 2, 4, 6, 8, 10, 12
        .filter(|x| *x > 7) // Stream: 8, 10, 12
        .eval_tap(|x| async move { println!("Tapping element: {}", x); }) // Prints elements
        .flat_map(|x| Stream::emits(vec![x, x + 1])) // Stream: 8, 9, 10, 11, 12, 13
        .take(4) // Stream: 8, 9, 10, 11
        .compile_to_list() // Consumes the stream and collects elements into a Vec
        .await
}

// To run this example (e.g., in a test or main function with Actix runtime):
// #[actix_rt::main]
// async fn main() {
//     match run_example().await {
//         Ok(results) => println!("Final results: {:?}", results), // Expected: [8, 9, 10, 11]
//         Err(e) => eprintln!("Stream error: {}", e),
//     }
// }
```

## Key Operators

Vuo provides a variety of operators to construct and manipulate streams:

*   **Sources**:
    *   `Stream::emits(items)`: Creates a stream from an iterator.
    *   `Stream::future(fut)`: Creates a stream from a future that resolves to a `Result<Item, String>`.
    *   `Stream::unfold(initial_state, fn)`: Creates a stream by repeatedly applying a function to a state.
    *   `Stream::eval(value)`: Creates a stream that emits a single value.
*   **Transformations**:
    *   `.map(fn)`: Applies a function to each element.
    *   `.filter(predicate_fn)`: Keeps elements that satisfy a predicate.
    *   `.flat_map(fn_produces_stream)`: Maps each element to a new stream and concatenates the results. (alias: `concat_map`)
    *   `.scan(initial, fn)`: Applies a folding function and emits each intermediate accumulator state.
    *   `.fold(initial, fn)`: Reduces the stream to a single value, emitted as the last element.
*   **Side-Effects**:
    *   `.eval_tap(fn_returns_future)`: Performs an asynchronous side-effect for each element without modifying it.
    *   `.drain()`: Consumes all elements, emitting a single `()` when the stream ends.
*   **Error Handling**:
    *   `.handle_error_with(fn_err_to_stream)`: Catches errors and switches to a fallback stream.
    *   `.on_finalize(fn_returns_future)`: Executes an asynchronous action when the stream completes or is cancelled, regardless of success or failure.

## Error Handling

Errors that occur during the setup of a stream stage or during its execution (if not handled by a specific operator like `Stream::future`) are generally propagated as `String` values.
The `compile_to_list()` method, for example, returns a `Result<Vec<Out>, String>`.

Use `handle_error_with` to catch these errors and provide alternative stream processing logic. `on_finalize` is useful for cleanup tasks that must run regardless of the stream's outcome.

## Running Tests

To run the tests for this library:

```bash
cargo test
```
