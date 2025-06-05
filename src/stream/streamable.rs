use std::fmt::Debug;

/// A trait for types that can be used as items in a stream.
///
/// This encapsulates the common bounds required for stream items:
/// - `'static`: No non-static references
/// - `Send`: Safe to send between threads
/// - `Unpin`: Safe to move after being pinned
/// - `Debug`: Can be formatted for debugging
pub trait Streamable: 'static + Send + Unpin + Debug {}

// Blanket implementation for all types that satisfy the bounds
impl<T> Streamable for T where T: 'static + Send + Unpin + Debug {}

/// A trait for types that can be used as items in a stream and are cloneable.
pub trait CloneableStreamable: Streamable + Clone {}
impl<T> CloneableStreamable for T where T: Streamable + Clone {}
