//! A `Pipe<In, Out>` is a stream transformation function `Stream<In> -> Stream<Out>`.

use crate::stream::{Stream, CloneableStreamable}; // Added CloneableStreamable
use std::marker::PhantomData;
use std::sync::Arc;
// Debug is implicitly covered by CloneableStreamable -> Streamable -> Debug
// use std::fmt::Debug; 

/// A `Pipe<In, Out>` represents a function that transforms a `Stream<In>`
/// into a `Stream<Out>`. Pipes can be composed to build complex stream
/// processing pipelines.
///
/// The transformation function itself is type-erased using `Arc<dyn Fn(...)>`.
pub struct Pipe<In, Out>
where
    In: CloneableStreamable, // Simplified bound
    Out: CloneableStreamable, // Simplified bound
{
    // The underlying function that defines the pipe's transformation.
    // Arc is used to make Pipe cloneable and share the function.
    // The function takes a Stream<In> and returns a Stream<Out>.
    func: Arc<dyn Fn(Stream<In>) -> Stream<Out> + Send + Sync + 'static>,
    _phantom_in: PhantomData<In>,
    _phantom_out: PhantomData<Out>,
}

impl<In, Out> Pipe<In, Out>
where
    In: CloneableStreamable,
    Out: CloneableStreamable,
{
    /// Creates a new `Pipe` from a function that transforms a `Stream<In>` to a `Stream<Out>`.
    ///
    /// # Arguments
    /// * `f`: A function `Stream<In> -> Stream<Out>`.
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(Stream<In>) -> Stream<Out> + Send + Sync + 'static,
    {
        Pipe {
            func: Arc::new(f),
            _phantom_in: PhantomData,
            _phantom_out: PhantomData,
        }
    }

    /// Applies this pipe to an input stream, producing an output stream.
    /// This is equivalent to `stream.through(pipe)` if such a method exists on Stream.
    ///
    /// # Arguments
    /// * `stream`: The input `Stream<In>`.
    ///
    /// # Returns
    /// The transformed `Stream<Out>`.
    pub fn apply(&self, stream: Stream<In>) -> Stream<Out> {
        (self.func)(stream)
    }

    /// Composes this pipe with another pipe, creating a new pipe that
    /// first applies this pipe (`In -> Out`) and then applies the `next_pipe` (`Out -> NextOut`).
    /// The result is a `Pipe<In, NextOut>`.
    ///
    /// Equivalent to `this_pipe.and_then(next_pipe)`.
    ///
    /// # Arguments
    /// * `next_pipe`: The `Pipe<Out, NextOut>` to apply after this one.
    ///
    /// # Returns
    /// A new `Pipe<In, NextOut>`.
    pub fn and_then<NextOut>(self, next_pipe: Pipe<Out, NextOut>) -> Pipe<In, NextOut>
    where
        NextOut: CloneableStreamable,
    {
        let first_pipe_func = self.func; 
        let second_pipe_func = next_pipe.func;

        Pipe::new(move |initial_stream: Stream<In>| {
            let intermediate_stream = first_pipe_func(initial_stream);
            second_pipe_func(intermediate_stream)
        })
    }

    /// Composes another pipe with this pipe, creating a new pipe that
    /// first applies the `prev_pipe` (`PrevIn -> In`) and then applies this pipe (`In -> Out`).
    /// The result is a `Pipe<PrevIn, Out>`.
    ///
    /// Equivalent to `prev_pipe.and_then(this_pipe)`.
    ///
    /// # Arguments
    /// * `prev_pipe`: The `Pipe<PrevIn, In>` to apply before this one.
    ///
    /// # Returns
    /// A new `Pipe<PrevIn, Out>`.
    pub fn compose<PrevIn>(self, prev_pipe: Pipe<PrevIn, In>) -> Pipe<PrevIn, Out>
    where
        PrevIn: CloneableStreamable,
    {
        prev_pipe.and_then(self)
    }

    /// Creates an identity pipe that passes through elements unchanged.
    /// `Stream<T> -> Stream<T>`.
    pub fn identity() -> Pipe<In, In>
    where
        In: CloneableStreamable, 
    {
        Pipe::new(|stream: Stream<In>| stream)
    }
}

impl<In, Out> Clone for Pipe<In, Out>
where
    In: CloneableStreamable,
    Out: CloneableStreamable,
{
    fn clone(&self) -> Self {
        Pipe {
            func: Arc::clone(&self.func),
            _phantom_in: PhantomData,
            _phantom_out: PhantomData,
        }
    }
}

// Example usage (would typically be in tests or other modules):
#[cfg(test)]
mod tests {
    use super::*;
    use actix_rt;

    // Dummy Streamable and CloneableStreamable impls for test types if not already covered
    // These might not be necessary if your crate::stream::streamable already provides blanket impls
    // or specific impls for common types like i32 and String.
    // If they cause "conflicting implementation" errors, remove them.

    #[actix_rt::test]
    async fn test_pipe_identity() {
        let identity_pipe: Pipe<i32, i32> = Pipe::<i32, i32>::identity();
        let stream_in = Stream::emits(vec![1, 2, 3]);
        let stream_out = identity_pipe.apply(stream_in);
        
        let result = stream_out.compile_to_list().await;
        assert_eq!(result, Ok(vec![1, 2, 3]));
    }

    #[actix_rt::test]
    async fn test_pipe_map_and_filter() {
        let map_pipe: Pipe<i32, i32> = Pipe::new(|s: Stream<i32>| s.map(|x| x * 2)); // 2, 4, 6, 8, 10
        let filter_pipe: Pipe<i32, i32> = Pipe::new(|s: Stream<i32>| s.filter(|x| *x > 5)); // 6, 8, 10 (from mapped)

        let composed_pipe = map_pipe.and_then(filter_pipe);
        
        let stream_in = Stream::emits(vec![1, 2, 3, 4, 5]);
        let stream_out = composed_pipe.apply(stream_in);

        let result = stream_out.compile_to_list().await;
        assert_eq!(result, Ok(vec![6, 8, 10]));
    }

    #[actix_rt::test]
    async fn test_pipe_compose() {
        let map_pipe: Pipe<i32, i32> = Pipe::new(|s: Stream<i32>| s.map(|x| x * 2)); 
        let filter_pipe: Pipe<i32, i32> = Pipe::new(|s: Stream<i32>| s.filter(|x| *x > 5)); 

        // filter_pipe.compose(map_pipe) means map_pipe runs first, then filter_pipe
        let composed_pipe = filter_pipe.compose(map_pipe); 
        
        let stream_in = Stream::emits(vec![1, 2, 3, 4, 5]);
        let stream_out = composed_pipe.apply(stream_in);

        let result = stream_out.compile_to_list().await;
        assert_eq!(result, Ok(vec![6, 8, 10]));
    }
    
    #[actix_rt::test]
    async fn test_complex_pipe_chain() {
        let to_string_pipe: Pipe<i32, String> = Pipe::new(|s: Stream<i32>| s.map(|x| format!("Item: {}", x)));
        let take_pipe: Pipe<String, String> = Pipe::new(|s: Stream<String>| s.take(2));
        let filter_even_numbers_as_i32: Pipe<i32, i32> = Pipe::new(|s: Stream<i32>| s.filter(|x| x % 2 == 0));

        let complex_pipe: Pipe<i32, String> = filter_even_numbers_as_i32
            .and_then(to_string_pipe)
            .and_then(take_pipe);

        let stream_in = Stream::emits(vec![1, 2, 3, 4, 5, 6]);
        // Expected after filter_even: 2, 4, 6
        // Expected after to_string: "Item: 2", "Item: 4", "Item: 6"
        // Expected after take(2): "Item: 2", "Item: 4"
        let stream_out = complex_pipe.apply(stream_in);
        let result = stream_out.compile_to_list().await;
        assert_eq!(result, Ok(vec!["Item: 2".to_string(), "Item: 4".to_string()]));
    }
}