//! Metrics about the connections of a legacy [`Client`](super::Client).

use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use hyper::Version;

use super::client::PoolKey;
use super::pool::{ConnectionCount, Poolable, WeakPool};

/// Metrics about the connections of a [`Client`](super::Client).
///
/// Create a handle and set it on the client with [`Builder::pool_metrics`] or
/// [`PoolOptions::metrics`].
///
/// [`Builder::pool_metrics`]: super::Builder::pool_metrics
/// [`PoolOptions::metrics`]: super::PoolOptions::metrics
#[derive(Clone, Default)]
pub struct PoolMetrics {
    /// A snapshot of the client's pool, set when the client is built. It
    /// returns `None` once the client has been dropped.
    pool: Arc<OnceLock<PoolSnapshot>>,
}

type PoolSnapshot = Box<dyn Fn() -> Option<Vec<ConnectionCount<PoolKey>>> + Send + Sync>;

/// Information about a connection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConnectionInfo {
    /// The URI scheme of the requests sent over the connection.
    pub scheme: http::uri::Scheme,
    /// The host and port of the requests sent over the connection.
    pub authority: http::uri::Authority,
    /// The remote address the connection was made to, if the connector
    /// reported it.
    pub peer: Option<SocketAddr>,
    /// The HTTP version used on the connection.
    pub version: Version,
}

/// The number of open connections to one server address, split into those
/// currently carrying a request and those that are idle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenConnections {
    /// The server and address the connections are to and the HTTP version
    /// they use.
    pub connection: ConnectionInfo,
    /// The number of connections currently carrying at least one request.
    pub active: usize,
    /// The number of connections currently carrying no request.
    pub idle: usize,
}

impl PoolMetrics {
    /// Creates a handle that no client has been built with yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the pool of the client built with this handle. Has no effect if a
    /// client was already built with it.
    pub(super) fn set_pool<T: Poolable>(&self, pool: WeakPool<T, PoolKey>) {
        let snapshot: PoolSnapshot = Box::new(move || Some(pool.upgrade()?.connection_counts()));
        let _ = self.pool.set(snapshot);
    }

    /// Returns the number of open connections per server, remote address and
    /// HTTP version. Connections that are still being established are not
    /// counted. Returns `None` when there is no live client: none has been
    /// built with this handle yet, or it has been dropped.
    pub fn open_connections(&self) -> Option<Vec<OpenConnections>> {
        let counts = (self.pool.get()?)()?;
        Some(
            counts
                .into_iter()
                .map(|count| OpenConnections {
                    connection: ConnectionInfo {
                        scheme: count.key.0,
                        authority: count.key.1,
                        peer: count.peer,
                        version: if count.shared {
                            Version::HTTP_2
                        } else {
                            Version::HTTP_11
                        },
                    },
                    active: count.active,
                    idle: count.idle,
                })
                .collect(),
        )
    }
}

impl fmt::Debug for PoolMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolMetrics").finish()
    }
}
