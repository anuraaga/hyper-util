//! Metrics about the connections of a legacy [`Client`](super::Client).

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use hyper::Version;

use super::client::PoolKey;
use super::pool::{ConnectionCount, Poolable, WeakPool};

/// Metrics about the connections of one or more [`Client`](super::Client)s.
///
/// Create a handle, set it on the client with [`Builder::pool_metrics`] or
/// [`PoolOptions::metrics`].
///
/// [`Builder::pool_metrics`]: super::Builder::pool_metrics
/// [`PoolOptions::metrics`]: super::PoolOptions::metrics
#[derive(Clone, Default)]
pub struct PoolMetrics {
    /// Snapshots of the pools of the clients using this handle. A snapshot
    /// returns `None` once its client has been dropped.
    pools: Arc<Mutex<Vec<PoolSnapshot>>>,
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
    /// Creates a handle with no clients yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds the pool of a client built with this handle.
    pub(super) fn add_pool<T: Poolable>(&self, pool: WeakPool<T, PoolKey>) {
        let snapshot: PoolSnapshot = Box::new(move || Some(pool.upgrade()?.connection_counts()));
        self.pools.lock().unwrap().push(snapshot);
    }

    /// Returns the number of open connections per server, remote address and
    /// HTTP version, across all clients using this handle. Connections that
    /// are still being established are not counted.
    pub fn open_connections(&self) -> Vec<OpenConnections> {
        // (active, idle) per server, address and version.
        let mut counts: HashMap<ConnectionInfo, (usize, usize)> = HashMap::new();
        self.pools.lock().unwrap().retain(|snapshot| {
            let Some(connections) = snapshot() else {
                return false;
            };
            for count in connections {
                let connection = ConnectionInfo {
                    scheme: count.key.0,
                    authority: count.key.1,
                    peer: count.peer,
                    version: if count.shared {
                        Version::HTTP_2
                    } else {
                        Version::HTTP_11
                    },
                };
                let entry = counts.entry(connection).or_default();
                entry.0 += count.active;
                entry.1 += count.idle;
            }
            true
        });
        counts
            .into_iter()
            .map(|(connection, (active, idle))| OpenConnections {
                connection,
                active,
                idle,
            })
            .collect()
    }
}

impl fmt::Debug for PoolMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolMetrics").finish()
    }
}
