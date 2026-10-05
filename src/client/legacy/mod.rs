#[cfg(any(feature = "http1", feature = "http2"))]
mod client;
#[cfg(any(feature = "http1", feature = "http2"))]
mod metrics;
#[cfg(any(feature = "http1", feature = "http2"))]
pub use client::{Builder, Client, Error, PoolOptions, ResponseFuture, with_pool_options};
#[cfg(any(feature = "http1", feature = "http2"))]
pub use metrics::{ConnectionInfo, OpenConnections, PoolMetrics};

pub mod connect;
#[doc(hidden)]
// Publicly available, but just for legacy purposes. A better pool will be
// designed.
pub mod pool;
