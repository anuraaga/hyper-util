#![allow(dead_code)]

use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{self, Poll, ready};

use std::time::{Duration, Instant};

use futures_channel::oneshot;
use tracing::{debug, trace};

use hyper::rt::Timer as _;

use crate::common::{exec, exec::Exec, timer::Timer};

// FIXME: allow() required due to `impl Trait` leaking types to this lint
#[allow(missing_debug_implementations)]
pub struct Pool<T, K: Key> {
    // If the pool is disabled, this is None.
    inner: Option<Arc<Mutex<PoolInner<T, K>>>>,
}

// Before using a pooled connection, make sure the sender is not dead.
//
// This is a trait to allow the `client::pool::tests` to work for `i32`.
//
// See https://github.com/hyperium/hyper/issues/1429
pub trait Poolable: Unpin + Send + Sized + 'static {
    fn is_open(&self) -> bool;
    /// Reserve this connection.
    ///
    /// Allows for HTTP/2 to return a shared reservation.
    fn reserve(self) -> Reservation<Self>;
    fn can_share(&self) -> bool;
    /// The number of leases a shared connection can hold at once.
    ///
    /// Only consulted for connections that `can_share`. Defaults to no limit,
    /// which is the previous behavior of one shared connection taking every
    /// request.
    fn max_shared(&self) -> usize {
        usize::MAX
    }
    /// Where this connection went and what else it could have gone to, for
    /// balancing across addresses.
    fn endpoint(&self) -> Option<&EndpointInfo> {
        None
    }
}

/// The address a pooled connection is connected to, and every address the
/// host resolved to at the time. These carry ports, since a resolver may map
/// one name to several ports.
#[derive(Clone, Debug)]
pub struct EndpointInfo {
    pub remote: SocketAddr,
    pub resolved: Arc<[SocketAddr]>,
}

/// The number of open connections a pool holds for one key and remote
/// address, split into those currently carrying a request and those that are
/// idle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionCount<K> {
    /// The key the connections were pooled under.
    pub key: K,
    /// The remote address of the connections, if the connector reported it.
    pub peer: Option<SocketAddr>,
    /// Whether these are shared (HTTP/2) connections.
    pub shared: bool,
    /// The number of connections currently carrying at least one request.
    pub active: usize,
    /// The number of connections currently carrying no request.
    pub idle: usize,
}

/// A [`Pool`] reference that does not keep the pool alive.
#[allow(missing_debug_implementations)]
pub struct WeakPool<T, K: Key>(WeakOpt<Mutex<PoolInner<T, K>>>);

impl<T, K: Key> WeakPool<T, K> {
    /// Returns the pool if it is still alive.
    pub fn upgrade(&self) -> Option<Pool<T, K>> {
        self.0.upgrade().map(|inner| Pool { inner: Some(inner) })
    }
}

/// How long an address that failed to connect is left out of balancing.
const UNREACHABLE_BACKOFF: Duration = Duration::from_secs(30);

pub trait Key: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}

impl<T> Key for T where T: Eq + Hash + Clone + Debug + Unpin + Send + 'static {}

/// A marker to identify what version a pooled connection is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(dead_code)]
pub enum Ver {
    Auto,
    Http2,
}

/// When checking out a pooled connection, it might be that the connection
/// only supports a single reservation, or it might be usable for many.
///
/// Specifically, HTTP/1 requires a unique reservation, but HTTP/2 can be
/// used for multiple requests.
// FIXME: allow() required due to `impl Trait` leaking types to this lint
#[allow(missing_debug_implementations)]
pub enum Reservation<T> {
    /// This connection could be used multiple times, the first one will be
    /// reinserted into the `idle` pool, and the second will be given to
    /// the `Checkout`.
    #[cfg(feature = "http2")]
    Shared(T, T),
    /// This connection requires unique access. It will be returned after
    /// use is complete.
    Unique(T),
}

/// Simple type alias in case the key type needs to be adjusted.
// pub type Key = (http::uri::Scheme, http::uri::Authority); //Arc<String>;

struct PoolInner<T, K: Eq + Hash> {
    // A flag that a connection is being established, and the connection
    // should be shared. This prevents making multiple HTTP/2 connections
    // to the same host.
    connecting: HashSet<K>,
    // Connections being established per key, of any version.
    connecting_count: HashMap<K, usize>,
    // Unique (HTTP/1) connections checked out per key and address. They are
    // not in `idle` while in use, so this is what makes them countable.
    unique_out: HashMap<K, HashMap<Option<SocketAddr>, usize>>,
    // Every address a key resolved to, as of its newest connection.
    resolved: HashMap<K, Arc<[SocketAddr]>>,
    // These are internal Conns sitting in the event loop in the KeepAlive
    // state, waiting to receive a new Request to send on the socket.
    idle: HashMap<K, Vec<Idle<T>>>,
    max_idle_per_host: usize,
    // Open connections per resolved address; past this a request queues on
    // the least loaded shared connection, or waits for a unique one to be
    // returned, instead of making more.
    max_connections_per_address: usize,
    // Keep a shared connection to every address a host name resolves to.
    dns_load_balancing: bool,
    // The address the next connection for a key should dial first.
    preferred: HashMap<K, SocketAddr>,
    // Addresses that recently failed to connect, and when.
    unreachable: HashMap<SocketAddr, Instant>,
    // These are outstanding Checkouts that are waiting for a socket to be
    // able to send a Request one. This is used when "racing" for a new
    // connection.
    //
    // The Client starts 2 tasks, 1 to connect a new socket, and 1 to wait
    // for the Pool to receive an idle Conn. When a Conn becomes idle,
    // this list is checked for any parked Checkouts, and tries to notify
    // them that the Conn could be used instead of waiting for a brand new
    // connection.
    waiters: HashMap<K, VecDeque<oneshot::Sender<(T, Option<Slot>)>>>,
    // A oneshot channel is used to allow the interval to be notified when
    // the Pool completely drops. That way, the interval can cancel immediately.
    idle_interval_ref: Option<oneshot::Sender<Infallible>>,
    exec: Exec,
    timer: Option<Timer>,
    timeout: Option<Duration>,
}

// This is because `Weak::new()` *allocates* space for `T`, even if it
// doesn't need it!
struct WeakOpt<T>(Option<Weak<T>>);

/// The number of leases currently held on one shared connection.
#[derive(Debug, Default)]
pub struct Load {
    active: AtomicUsize,
}

impl Load {
    fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
}

/// One slot taken on a shared connection, released on drop without touching
/// the pool. Used while handing a connection to a waiter, where the pool lock
/// is already held.
#[derive(Debug)]
pub struct Slot(Arc<Load>);

impl Slot {
    fn take(load: &Arc<Load>) -> Slot {
        load.active.fetch_add(1, Ordering::AcqRel);
        Slot(load.clone())
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}

/// A lease on a shared connection. Dropping it releases the slot and lets the
/// pool hand the connection to a waiting checkout.
pub struct Lease<T: Poolable, K: Key> {
    slot: Option<Slot>,
    key: K,
    pool: WeakOpt<Mutex<PoolInner<T, K>>>,
}

impl<T: Poolable, K: Key> Lease<T, K> {
    fn new(slot: Slot, key: K, pool: WeakOpt<Mutex<PoolInner<T, K>>>) -> Self {
        Lease {
            slot: Some(slot),
            key,
            pool,
        }
    }
}

impl<T: Poolable, K: Key> fmt::Debug for Lease<T, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lease").field("key", &self.key).finish()
    }
}

impl<T: Poolable, K: Key> Drop for Lease<T, K> {
    fn drop(&mut self) {
        let Some(slot) = self.slot.take() else {
            return;
        };
        let load = slot.0.clone();
        // Release the slot before taking the lock so waiters see the capacity.
        drop(slot);
        if let Some(pool) = self.pool.upgrade() {
            if let Ok(mut inner) = pool.lock() {
                inner.release(&self.key, &load);
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub idle_timeout: Option<Duration>,
    pub max_idle_per_host: usize,
    pub max_connections_per_address: usize,
    pub dns_load_balancing: bool,
}

impl Config {
    pub fn is_enabled(&self) -> bool {
        self.max_idle_per_host > 0
    }
}

impl<T, K: Key> Pool<T, K> {
    pub fn new<E, M>(config: Config, executor: E, timer: Option<M>) -> Pool<T, K>
    where
        E: hyper::rt::Executor<exec::BoxSendFuture> + Send + Sync + Clone + 'static,
        M: hyper::rt::Timer + Send + Sync + Clone + 'static,
    {
        let exec = Exec::new(executor);
        let timer = timer.map(|t| Timer::new(t));
        let inner = if config.is_enabled() {
            Some(Arc::new(Mutex::new(PoolInner {
                connecting: HashSet::new(),
                connecting_count: HashMap::new(),
                unique_out: HashMap::new(),
                resolved: HashMap::new(),
                idle: HashMap::new(),
                idle_interval_ref: None,
                max_idle_per_host: config.max_idle_per_host,
                max_connections_per_address: config.max_connections_per_address,
                dns_load_balancing: config.dns_load_balancing,
                preferred: HashMap::new(),
                unreachable: HashMap::new(),
                waiters: HashMap::new(),
                exec,
                timer,
                timeout: config.idle_timeout,
            })))
        } else {
            None
        };

        Pool { inner }
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    fn weak(&self) -> WeakOpt<Mutex<PoolInner<T, K>>> {
        self.inner
            .as_ref()
            .map_or_else(WeakOpt::none, WeakOpt::downgrade)
    }

    /// Returns a reference to this pool that does not keep it alive.
    pub fn downgrade(&self) -> WeakPool<T, K> {
        WeakPool(self.weak())
    }

    #[cfg(test)]
    pub(super) fn no_timer(&self) {
        // Prevent an actual interval from being created for this pool...
        {
            let mut inner = self.inner.as_ref().unwrap().lock().unwrap();
            assert!(inner.idle_interval_ref.is_none(), "timer already spawned");
            let (tx, _) = oneshot::channel();
            inner.idle_interval_ref = Some(tx);
        }
    }
}

impl<T: Poolable, K: Key> Pool<T, K> {
    /// Counts the open connections in the pool. A shared connection is active
    /// while at least one lease is held on it, and a unique connection is
    /// active while it is checked out. Connections that are still being
    /// established are not counted.
    pub fn connection_counts(&self) -> Vec<ConnectionCount<K>> {
        let Some(inner) = &self.inner else {
            return Vec::new();
        };
        let inner = inner.lock().unwrap();
        // (active, idle) per key, remote address, and kind.
        let mut counts: HashMap<(K, Option<SocketAddr>, bool), (usize, usize)> = HashMap::new();
        for (key, list) in &inner.idle {
            for entry in list {
                if !entry.value.is_open() {
                    continue;
                }
                let peer = entry.value.endpoint().map(|endpoint| endpoint.remote);
                let shared = entry.load.is_some();
                let active = entry.load.as_ref().is_some_and(|load| load.active() > 0);
                let count = counts.entry((key.clone(), peer, shared)).or_default();
                if active {
                    count.0 += 1;
                } else {
                    count.1 += 1;
                }
            }
        }
        for (key, out) in &inner.unique_out {
            for (peer, n) in out {
                counts.entry((key.clone(), *peer, false)).or_default().0 += n;
            }
        }
        counts
            .into_iter()
            .map(|((key, peer, shared), (active, idle))| ConnectionCount {
                key,
                peer,
                shared,
                active,
                idle,
            })
            .collect()
    }

    /// The address the next connection for `key` should dial first, chosen by
    /// the balancing policy at checkout.
    pub fn take_preferred(&self, key: &K) -> Option<SocketAddr> {
        self.inner.as_ref()?.lock().unwrap().preferred.remove(key)
    }

    /// Remembers that `ip` could not be connected to, so the balancing policy
    /// stops asking for it for a while.
    pub fn mark_unreachable(&self, ip: SocketAddr) {
        if let Some(inner) = &self.inner {
            let mut inner = inner.lock().unwrap();
            let now = inner.now();
            inner.unreachable.insert(ip, now);
        }
    }

    /// Returns a `Checkout` which is a future that resolves if an idle
    /// connection becomes available.
    pub fn checkout(&self, key: K) -> Checkout<T, K> {
        Checkout {
            key,
            pool: self.clone(),
            waiter: None,
        }
    }

    /// Ensure that there is only ever 1 connecting task for HTTP/2
    /// connections. This does nothing for HTTP/1.
    pub fn connecting(&self, key: &K, ver: Ver) -> Option<Connecting<T, K>> {
        let Some(enabled) = &self.inner else {
            return Some(Connecting {
                key: key.clone(),
                ver,
                pool: WeakOpt::none(),
            });
        };
        let mut inner = enabled.lock().unwrap();
        if inner.at_connection_cap(key) {
            trace!("every address at its connection cap for {:?}", key);
            return None;
        }
        if ver == Ver::Http2 && !inner.connecting.insert(key.clone()) {
            trace!("HTTP/2 connecting already in progress for {:?}", key);
            return None;
        }
        *inner.connecting_count.entry(key.clone()).or_default() += 1;
        Some(Connecting {
            key: key.clone(),
            ver,
            pool: WeakOpt::downgrade(enabled),
        })
    }

    #[cfg(test)]
    fn locked(&self) -> std::sync::MutexGuard<'_, PoolInner<T, K>> {
        self.inner.as_ref().expect("enabled").lock().expect("lock")
    }

    /* Used in client/tests.rs...
    #[cfg(test)]
    pub(super) fn h1_key(&self, s: &str) -> Key {
        Arc::new(s.to_string())
    }

    #[cfg(test)]
    pub(super) fn idle_count(&self, key: &Key) -> usize {
        self
            .locked()
            .idle
            .get(key)
            .map(|list| list.len())
            .unwrap_or(0)
    }
    */

    pub fn pooled(
        &self,
        #[cfg_attr(not(feature = "http2"), allow(unused_mut))] mut connecting: Connecting<T, K>,
        value: T,
    ) -> Pooled<T, K> {
        let mut lease = None;
        let (value, pool_ref) = if let Some(ref enabled) = self.inner {
            match value.reserve() {
                #[cfg(feature = "http2")]
                Reservation::Shared(to_insert, to_return) => {
                    let load = Arc::new(Load::default());
                    lease = Some(Lease::new(
                        Slot::take(&load),
                        connecting.key.clone(),
                        WeakOpt::downgrade(enabled),
                    ));
                    let mut inner = enabled.lock().unwrap();
                    inner.note_resolved(&connecting.key, &to_insert);
                    inner.put_shared(connecting.key.clone(), to_insert, load, enabled);
                    // Do this here instead of Drop for Connecting because we
                    // already have a lock, no need to lock the mutex twice.
                    inner.connected(&connecting.key, connecting.ver);
                    // prevent the Drop of Connecting from repeating inner.connected()
                    connecting.pool = WeakOpt::none();

                    // Shared reservations don't need a reference to the pool,
                    // since the pool always keeps a copy.
                    (to_return, WeakOpt::none())
                }
                Reservation::Unique(value) => {
                    let mut inner = enabled.lock().unwrap();
                    inner.note_resolved(&connecting.key, &value);
                    inner.unique_taken(&connecting.key, &value);
                    inner.connected(&connecting.key, connecting.ver);
                    connecting.pool = WeakOpt::none();
                    // Unique reservations must take a reference to the pool
                    // since they hope to reinsert once the reservation is
                    // completed
                    (value, WeakOpt::downgrade(enabled))
                }
            }
        } else {
            // If pool is not enabled, skip all the things...

            // The Connecting should have had no pool ref
            debug_assert!(connecting.pool.upgrade().is_none());

            (value, WeakOpt::none())
        };
        Pooled {
            key: connecting.key.clone(),
            is_reused: false,
            pool: pool_ref,
            value: Some(value),
            lease,
        }
    }

    fn reuse(&self, key: &K, value: T, lease: Option<Lease<T, K>>) -> Pooled<T, K> {
        debug!("reuse idle connection for {:?}", key);
        // TODO: unhack this
        // In Pool::pooled(), which is used for inserting brand new connections,
        // there's some code that adjusts the pool reference taken depending
        // on if the Reservation can be shared or is unique. By the time
        // reuse() is called, the reservation has already been made, and
        // we just have the final value, without knowledge of if this is
        // unique or shared. So, the hack is to just assume Ver::Http2 means
        // shared... :(
        let mut pool_ref = WeakOpt::none();
        if !value.can_share() {
            if let Some(ref enabled) = self.inner {
                enabled.lock().unwrap().unique_taken(key, &value);
                pool_ref = WeakOpt::downgrade(enabled);
            }
        }

        Pooled {
            is_reused: true,
            key: key.clone(),
            pool: pool_ref,
            value: Some(value),
            lease,
        }
    }
}

/// Pop off this list, looking for a usable connection that hasn't expired.
struct IdlePopper<'a, T, K> {
    key: &'a K,
    list: &'a mut Vec<Idle<T>>,
}

/// A connection taken from the idle list, with the slot held on it when it
/// is shared.
struct Popped<T> {
    value: T,
    slot: Option<Slot>,
}

/// Open connections per address the host resolves to, in resolution order,
/// skipping addresses that recently failed to connect. Shared connections
/// and idle unique ones are in the idle list; unique ones in use are counted
/// separately.
struct AddressStats {
    counts: Vec<(SocketAddr, usize)>,
}

impl AddressStats {
    fn new<T: Poolable>(
        resolved: &[SocketAddr],
        list: &[Idle<T>],
        unique_out: Option<&HashMap<Option<SocketAddr>, usize>>,
        unreachable: &HashMap<SocketAddr, Instant>,
        now: Instant,
    ) -> Option<Self> {
        let counts = resolved
            .iter()
            .copied()
            .filter(|addr| {
                unreachable.get(addr).is_none_or(|failed| {
                    now.saturating_duration_since(*failed) > UNREACHABLE_BACKOFF
                })
            })
            .map(|addr| {
                let idle = list
                    .iter()
                    .filter(|entry| {
                        entry.value.is_open()
                            && entry
                                .value
                                .endpoint()
                                .is_some_and(|endpoint| endpoint.remote == addr)
                    })
                    .count();
                let out = unique_out
                    .and_then(|out| out.get(&Some(addr)))
                    .copied()
                    .unwrap_or(0);
                (addr, idle + out)
            })
            .collect();
        Some(AddressStats { counts })
    }

    fn total(&self) -> usize {
        self.counts.iter().map(|(_, count)| count).sum()
    }

    /// An address with no connection yet.
    fn uncovered(&self) -> Option<SocketAddr> {
        self.counts
            .iter()
            .find(|(_, count)| *count == 0)
            .map(|(ip, _)| *ip)
    }

    /// An address with fewer than `max` connections: the least connected one
    /// when `least`, otherwise the first in resolution order, which is what
    /// the connector would dial on its own.
    fn under_cap(&self, max: usize, least: bool) -> Option<SocketAddr> {
        let under = self.counts.iter().filter(|(_, count)| *count < max);
        if least {
            under.min_by_key(|(_, count)| *count)
        } else {
            under.min_by_key(|_| 0)
        }
        .map(|(ip, _)| *ip)
    }
}

/// How the pool picks between its shared connections and making another.
#[derive(Clone, Copy)]
struct SharedPolicy {
    max_connections_per_address: usize,
    dns_load_balancing: bool,
}

/// Takes the least loaded shared connection, leaving it in the list. Unless
/// `ignore_limit`, only a connection with stream capacity left qualifies.
fn pop_shared<T: Poolable>(list: &mut Vec<Idle<T>>, ignore_limit: bool) -> Option<Popped<T>> {
    let mut best: Option<(usize, usize)> = None;
    for (index, entry) in list.iter().enumerate() {
        let Some(load) = entry.load.as_ref() else {
            continue;
        };
        if !entry.value.is_open() {
            continue;
        }
        let active = load.active();
        if !ignore_limit && active >= entry.value.max_shared() {
            continue;
        }
        if best.is_none_or(|(_, best_active)| active < best_active) {
            best = Some((index, active));
        }
    }
    let (index, _) = best?;
    let Idle {
        value,
        idle_at,
        load,
    } = list.remove(index);
    let load = load.expect("shared entry has load");
    match value.reserve() {
        #[cfg(feature = "http2")]
        Reservation::Shared(to_reinsert, to_checkout) => {
            let slot = Slot::take(&load);
            list.insert(
                index,
                Idle {
                    idle_at,
                    value: to_reinsert,
                    load: Some(load),
                },
            );
            Some(Popped {
                value: to_checkout,
                slot: Some(slot),
            })
        }
        Reservation::Unique(unique) => Some(Popped {
            value: unique,
            slot: None,
        }),
    }
}

impl<'a, T: Poolable + 'a, K: Debug> IdlePopper<'a, T, K> {
    /// Takes a connection for a request, or says which address a new
    /// connection should go to when none should be reused.
    ///
    /// The hint is only set when the policy needs to steer the connector;
    /// `None` with no connection means the connector picks on its own.
    fn pop(
        self,
        expiration: &Expiration,
        now: Instant,
        policy: SharedPolicy,
        stats: Option<AddressStats>,
    ) -> (Option<Popped<T>>, Option<SocketAddr>) {
        // Drop closed and expired connections first. A shared connection
        // with leases is in use and cannot be expired.
        let key = self.key;
        self.list.retain(|entry| {
            if !entry.value.is_open() {
                trace!("removing closed connection for {:?}", key);
                return false;
            }
            let in_use = entry.load.as_ref().is_some_and(|load| load.active() > 0);
            if !in_use && expiration.expires(entry.idle_at, now) {
                trace!("removing expired connection for {:?}", key);
                return false;
            }
            true
        });

        // When balancing, an address without a connection wins over reusing
        // one: ask for a connection to it instead.
        if policy.dns_load_balancing {
            if let Some(ip) = stats.as_ref().and_then(AddressStats::uncovered) {
                trace!("balance; connect to uncovered {:?} for {:?}", ip, key);
                return (None, Some(ip));
            }
        }

        // Prefer a shared connection with capacity.
        if let Some(popped) = pop_shared(self.list, false) {
            return (Some(popped), None);
        }

        // Then the most recently idle unique connection.
        if let Some(index) = self.list.iter().rposition(|entry| entry.load.is_none()) {
            let entry = self.list.remove(index);
            let popped = match entry.value.reserve() {
                #[cfg(feature = "http2")]
                Reservation::Shared(to_reinsert, to_checkout) => {
                    self.list.push(Idle {
                        idle_at: now,
                        value: to_reinsert,
                        load: None,
                    });
                    Popped {
                        value: to_checkout,
                        slot: None,
                    }
                }
                Reservation::Unique(unique) => Popped {
                    value: unique,
                    slot: None,
                },
            };
            return (Some(popped), None);
        }

        let Some(stats) = stats else {
            return (None, None);
        };
        // Nothing to reuse. Make another connection if some address is under
        // its cap, steering the connector unless nothing limits it. Otherwise
        // queue on the least loaded shared connection, or, with only unique
        // connections, wait for one to come back: `Pool::connecting` refuses
        // to dial at the cap.
        let steer = policy.dns_load_balancing || policy.max_connections_per_address != usize::MAX;
        if let Some(addr) = stats.under_cap(
            policy.max_connections_per_address,
            policy.dns_load_balancing,
        ) {
            return (None, steer.then_some(addr));
        }
        if let Some(popped) = pop_shared(self.list, true) {
            trace!(
                "every address at its connection cap for {:?}, queueing",
                key
            );
            return (Some(popped), None);
        }
        trace!("every address at its connection cap for {:?}, waiting", key);
        (None, None)
    }
}

impl<T: Poolable, K: Key> PoolInner<T, K> {
    fn now(&self) -> Instant {
        self.timer
            .as_ref()
            .map_or_else(|| Instant::now(), |t| t.now())
    }

    /// Adds a shared (HTTP/2) connection to the idle list and hands it to
    /// waiters while it has capacity.
    fn put_shared(
        &mut self,
        key: K,
        value: T,
        load: Arc<Load>,
        __pool_ref: &Arc<Mutex<PoolInner<T, K>>>,
    ) {
        let now = self.now();
        let idle_list = self.idle.entry(key.clone()).or_default();
        let shared = idle_list
            .iter()
            .filter(|entry| entry.load.is_some())
            .count();
        if self.max_idle_per_host <= shared {
            trace!("max shared per host for {:?}, dropping connection", key);
            return;
        }
        debug!("pooling shared connection for {:?}", key);
        idle_list.push(Idle {
            value,
            idle_at: now,
            load: Some(load),
        });
        self.serve_waiters(&key);
        self.spawn_idle_interval(__pool_ref);
    }

    /// Hands shared connections with capacity to waiting checkouts.
    fn serve_waiters(&mut self, key: &K) {
        let Some(idle_list) = self.idle.get_mut(key) else {
            return;
        };
        let Some(waiters) = self.waiters.get_mut(key) else {
            return;
        };
        while let Some(tx) = waiters.front() {
            if tx.is_canceled() {
                trace!("serve; removing canceled waiter for {:?}", key);
                waiters.pop_front();
                continue;
            }
            let Some(popped) = pop_shared(idle_list, false) else {
                break;
            };
            let tx = waiters.pop_front().expect("front checked");
            if tx.send((popped.value, popped.slot)).is_err() {
                // The waiter went away between the check and the send; the
                // slot is released with the dropped message.
                trace!("serve; waiter gone for {:?}", key);
            }
        }
        if waiters.is_empty() {
            self.waiters.remove(key);
        }
    }

    /// A lease on a shared connection ended.
    fn release(&mut self, key: &K, load: &Arc<Load>) {
        if load.active() == 0 {
            let now = self.now();
            if let Some(idle_list) = self.idle.get_mut(key) {
                for entry in idle_list.iter_mut() {
                    if entry.load.as_ref().is_some_and(|l| Arc::ptr_eq(l, load)) {
                        entry.idle_at = now;
                    }
                }
            }
        }
        self.serve_waiters(key);
    }

    fn put(&mut self, key: K, value: T, __pool_ref: &Arc<Mutex<PoolInner<T, K>>>) {
        if value.can_share() {
            self.put_shared(key, value, Arc::new(Load::default()), __pool_ref);
            return;
        }
        trace!("put; add idle connection for {:?}", key);
        let mut remove_waiters = false;
        let mut value = Some(value);
        if let Some(waiters) = self.waiters.get_mut(&key) {
            while let Some(tx) = waiters.pop_front() {
                if !tx.is_canceled() {
                    let reserved = value.take().expect("value already sent");
                    let reserved = match reserved.reserve() {
                        #[cfg(feature = "http2")]
                        Reservation::Shared(to_keep, to_send) => {
                            value = Some(to_keep);
                            to_send
                        }
                        Reservation::Unique(uniq) => uniq,
                    };
                    match tx.send((reserved, None)) {
                        Ok(()) => {
                            if value.is_none() {
                                break;
                            } else {
                                continue;
                            }
                        }
                        Err((e, _)) => {
                            value = Some(e);
                        }
                    }
                }

                trace!("put; removing canceled waiter for {:?}", key);
            }
            remove_waiters = waiters.is_empty();
        }
        if remove_waiters {
            self.waiters.remove(&key);
        }

        match value {
            Some(value) => {
                // borrow-check scope...
                {
                    let now = self.now();
                    let idle_list = self.idle.entry(key.clone()).or_default();
                    if self.max_idle_per_host <= idle_list.len() {
                        trace!("max idle per host for {:?}, dropping connection", key);
                        return;
                    }

                    debug!("pooling idle connection for {:?}", key);
                    idle_list.push(Idle {
                        value,
                        idle_at: now,
                        load: None,
                    });
                }

                self.spawn_idle_interval(__pool_ref);
            }
            None => trace!("put; found waiter for {:?}", key),
        }
    }

    /// A `Connecting` task is complete. Not necessarily successfully,
    /// but the lock is going away, so clean up.
    fn connected(&mut self, key: &K, ver: Ver) {
        if let Some(count) = self.connecting_count.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                self.connecting_count.remove(key);
            }
        }
        if ver == Ver::Http2 {
            let existed = self.connecting.remove(key);
            debug_assert!(existed, "Connecting dropped, key not in pool.connecting");
            // cancel any waiters. if there are any, it's because
            // this Connecting task didn't complete successfully, or the
            // connection filled up before they got a slot on it.
            // Either way they need to try again.
            self.waiters.remove(key);
        }
    }

    /// Drops the oldest waiter so that its request tries again, which is how
    /// a request parked at the connection cap learns that the connection it
    /// waited for is gone.
    fn cancel_one_waiter(&mut self, key: &K) {
        if let Some(waiters) = self.waiters.get_mut(key) {
            waiters.pop_front();
            if waiters.is_empty() {
                self.waiters.remove(key);
            }
        }
    }

    fn note_resolved(&mut self, key: &K, value: &T) {
        if let Some(endpoint) = value.endpoint() {
            self.resolved.insert(key.clone(), endpoint.resolved.clone());
        }
    }

    fn unique_taken(&mut self, key: &K, value: &T) {
        let addr = value.endpoint().map(|endpoint| endpoint.remote);
        *self
            .unique_out
            .entry(key.clone())
            .or_default()
            .entry(addr)
            .or_default() += 1;
    }

    fn unique_returned(&mut self, key: &K, value: &T) {
        let addr = value.endpoint().map(|endpoint| endpoint.remote);
        if let Some(out) = self.unique_out.get_mut(key) {
            if let Some(count) = out.get_mut(&addr) {
                *count -= 1;
                if *count == 0 {
                    out.remove(&addr);
                }
            }
            if out.is_empty() {
                self.unique_out.remove(key);
            }
        }
    }

    fn address_stats(&self, key: &K, now: Instant) -> Option<AddressStats> {
        AddressStats::new(
            self.resolved.get(key)?,
            self.idle.get(key).map_or(&[], Vec::as_slice),
            self.unique_out.get(key),
            &self.unreachable,
            now,
        )
    }

    /// Whether no address has room for another connection, counting the
    /// ones being made.
    fn at_connection_cap(&self, key: &K) -> bool {
        let max = self.max_connections_per_address;
        if max == usize::MAX {
            return false;
        }
        let now = self.now();
        let Some(stats) = self.address_stats(key, now) else {
            return false;
        };
        // With every address backing off, a dial is the only way to learn
        // whether one came back.
        if stats.counts.is_empty() {
            return false;
        }
        let connecting = self.connecting_count.get(key).copied().unwrap_or(0);
        stats.under_cap(max, false).is_none()
            || stats.total() + connecting >= max * stats.counts.len()
    }

    fn spawn_idle_interval(&mut self, pool_ref: &Arc<Mutex<PoolInner<T, K>>>) {
        if self.idle_interval_ref.is_some() {
            return;
        }
        let dur = if let Some(dur) = self.timeout {
            dur
        } else {
            return;
        };
        if dur == Duration::ZERO {
            return;
        }
        let timer = if let Some(timer) = self.timer.clone() {
            timer
        } else {
            return;
        };

        // While someone might want a shorter duration, and it will be respected
        // at checkout time, there's no need to wake up and proactively evict
        // faster than this.
        const MIN_CHECK: Duration = Duration::from_millis(90);

        let dur = dur.max(MIN_CHECK);

        let (tx, rx) = oneshot::channel();
        self.idle_interval_ref = Some(tx);

        let interval = IdleTask {
            timer: timer.clone(),
            duration: dur,
            pool: WeakOpt::downgrade(pool_ref),
            pool_drop_notifier: rx,
        };

        self.exec.execute(interval.run());
    }
}

impl<T, K: Eq + Hash> PoolInner<T, K> {
    /// Any `FutureResponse`s that were created will have made a `Checkout`,
    /// and possibly inserted into the pool that it is waiting for an idle
    /// connection. If a user ever dropped that future, we need to clean out
    /// those parked senders.
    fn clean_waiters(&mut self, key: &K) {
        let mut remove_waiters = false;
        if let Some(waiters) = self.waiters.get_mut(key) {
            waiters.retain(|tx| !tx.is_canceled());
            remove_waiters = waiters.is_empty();
        }
        if remove_waiters {
            self.waiters.remove(key);
        }
    }
}

impl<T: Poolable, K: Key> PoolInner<T, K> {
    /// This should *only* be called by the IdleTask
    fn clear_expired(&mut self) {
        let dur = self.timeout.expect("interval assumes timeout");

        let now = self.now();
        //self.last_idle_check_at = now;

        self.idle.retain(|key, values| {
            values.retain(|entry| {
                if !entry.value.is_open() {
                    trace!("idle interval evicting closed for {:?}", key);
                    return false;
                }

                if entry.load.as_ref().is_some_and(|load| load.active() > 0) {
                    return true;
                }

                // Avoid `Instant::sub` to avoid issues like rust-lang/rust#86470.
                if now.saturating_duration_since(entry.idle_at) > dur {
                    trace!("idle interval evicting expired for {:?}", key);
                    return false;
                }

                // Otherwise, keep this value...
                true
            });

            // returning false evicts this key/val
            !values.is_empty()
        });
    }
}

impl<T, K: Key> Clone for Pool<T, K> {
    fn clone(&self) -> Pool<T, K> {
        Pool {
            inner: self.inner.clone(),
        }
    }
}

/// A wrapped poolable value that tries to reinsert to the Pool on Drop.
// Note: The bounds `T: Poolable` is needed for the Drop impl.
pub struct Pooled<T: Poolable, K: Key> {
    value: Option<T>,
    is_reused: bool,
    key: K,
    pool: WeakOpt<Mutex<PoolInner<T, K>>>,
    lease: Option<Lease<T, K>>,
}

impl<T: Poolable, K: Key> Pooled<T, K> {
    pub fn is_reused(&self) -> bool {
        self.is_reused
    }

    /// Takes the lease held on a shared connection, if any, so that it can
    /// outlive this handle, for example until a response body is finished.
    pub fn take_lease(&mut self) -> Option<Lease<T, K>> {
        self.lease.take()
    }

    pub fn is_pool_enabled(&self) -> bool {
        self.pool.0.is_some()
    }

    fn as_ref(&self) -> &T {
        self.value.as_ref().expect("not dropped")
    }

    fn as_mut(&mut self) -> &mut T {
        self.value.as_mut().expect("not dropped")
    }
}

impl<T: Poolable, K: Key> Deref for Pooled<T, K> {
    type Target = T;
    fn deref(&self) -> &T {
        self.as_ref()
    }
}

impl<T: Poolable, K: Key> DerefMut for Pooled<T, K> {
    fn deref_mut(&mut self) -> &mut T {
        self.as_mut()
    }
}

impl<T: Poolable, K: Key> Drop for Pooled<T, K> {
    fn drop(&mut self) {
        let Some(value) = self.value.take() else {
            return;
        };
        if value.can_share() {
            // Ver::Http2 is already in the Pool (or dead), so we wouldn't
            // have an actual reference to the Pool.
            return;
        }
        let Some(pool) = self.pool.upgrade() else {
            trace!("pool dropped, dropping pooled ({:?})", self.key);
            return;
        };
        if let Ok(mut inner) = pool.lock() {
            inner.unique_returned(&self.key, &value);
            if value.is_open() {
                inner.put(self.key.clone(), value, &pool);
            } else {
                // If we *already* know the connection is done here,
                // it shouldn't be re-inserted back into the pool. A request
                // waiting for it at the connection cap must try again.
                inner.cancel_one_waiter(&self.key);
            }
        }
    }
}

impl<T: Poolable, K: Key> fmt::Debug for Pooled<T, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pooled").field("key", &self.key).finish()
    }
}

struct Idle<T> {
    idle_at: Instant,
    value: T,
    // Present for shared connections, counting leases held on them.
    load: Option<Arc<Load>>,
}

// FIXME: allow() required due to `impl Trait` leaking types to this lint
#[allow(missing_debug_implementations)]
pub struct Checkout<T, K: Key> {
    key: K,
    pool: Pool<T, K>,
    waiter: Option<oneshot::Receiver<(T, Option<Slot>)>>,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    PoolDisabled,
    CheckoutNoLongerWanted,
    CheckedOutClosedValue,
}

impl Error {
    /// Whether the request never got a connection through no fault of the
    /// destination, so the client should try again: the value it was handed
    /// had closed, or the connection being made filled up before this waiter
    /// got a slot on it.
    pub(super) fn is_canceled(&self) -> bool {
        matches!(
            self,
            Error::CheckedOutClosedValue | Error::CheckoutNoLongerWanted
        )
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Error::PoolDisabled => "pool is disabled",
            Error::CheckedOutClosedValue => "checked out connection was closed",
            Error::CheckoutNoLongerWanted => "request was canceled",
        })
    }
}

impl StdError for Error {}

impl<T: Poolable, K: Key> Checkout<T, K> {
    fn poll_waiter(
        &mut self,
        cx: &mut task::Context<'_>,
    ) -> Poll<Option<Result<Pooled<T, K>, Error>>> {
        if let Some(mut rx) = self.waiter.take() {
            match Pin::new(&mut rx).poll(cx) {
                Poll::Ready(Ok((value, slot))) => {
                    if value.is_open() {
                        let lease =
                            slot.map(|slot| Lease::new(slot, self.key.clone(), self.pool.weak()));
                        Poll::Ready(Some(Ok(self.pool.reuse(&self.key, value, lease))))
                    } else {
                        Poll::Ready(Some(Err(Error::CheckedOutClosedValue)))
                    }
                }
                Poll::Pending => {
                    self.waiter = Some(rx);
                    Poll::Pending
                }
                Poll::Ready(Err(_canceled)) => {
                    Poll::Ready(Some(Err(Error::CheckoutNoLongerWanted)))
                }
            }
        } else {
            Poll::Ready(None)
        }
    }

    fn checkout(&mut self, cx: &mut task::Context<'_>) -> Option<Pooled<T, K>> {
        let entry = {
            let mut inner = self.pool.inner.as_ref()?.lock().unwrap();
            let expiration = Expiration::new(inner.timeout);
            let now = inner.now();
            let inner = &mut *inner;
            let policy = SharedPolicy {
                max_connections_per_address: inner.max_connections_per_address,
                dns_load_balancing: inner.dns_load_balancing,
            };
            let mut preferred = None;
            let maybe_entry = inner.idle.get_mut(&self.key).map(|list| {
                trace!("take? {:?}: expiration = {:?}", self.key, expiration.0);
                // A block to end the mutable borrow on list,
                // so the map below can check is_empty()
                let popped = {
                    let stats = inner.resolved.get(&self.key).and_then(|resolved| {
                        AddressStats::new(
                            resolved,
                            list,
                            inner.unique_out.get(&self.key),
                            &inner.unreachable,
                            now,
                        )
                    });
                    let popper = IdlePopper {
                        key: &self.key,
                        list,
                    };
                    let (popped, hint) = popper.pop(&expiration, now, policy, stats);
                    preferred = hint;
                    popped
                };
                // Shared connections without capacity stay in the list, so
                // only drop the list when it is actually empty.
                (popped, list.is_empty())
            });
            if let Some(ip) = preferred {
                inner.preferred.insert(self.key.clone(), ip);
            }

            let (entry, empty) = maybe_entry.unwrap_or((None, true));
            if empty {
                //TODO: This could be done with the HashMap::entry API instead.
                inner.idle.remove(&self.key);
            }

            if entry.is_none() && self.waiter.is_none() {
                let (tx, mut rx) = oneshot::channel();
                trace!("checkout waiting for idle connection: {:?}", self.key);
                inner
                    .waiters
                    .entry(self.key.clone())
                    .or_insert_with(VecDeque::new)
                    .push_back(tx);

                // register the waker with this oneshot
                assert!(Pin::new(&mut rx).poll(cx).is_pending());
                self.waiter = Some(rx);
            }

            entry
        };

        entry.map(|popped| {
            let lease = popped
                .slot
                .map(|slot| Lease::new(slot, self.key.clone(), self.pool.weak()));
            self.pool.reuse(&self.key, popped.value, lease)
        })
    }
}

impl<T: Poolable, K: Key> Future for Checkout<T, K> {
    type Output = Result<Pooled<T, K>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
        if let Some(pooled) = ready!(self.poll_waiter(cx)?) {
            return Poll::Ready(Ok(pooled));
        }

        if let Some(pooled) = self.checkout(cx) {
            Poll::Ready(Ok(pooled))
        } else if !self.pool.is_enabled() {
            Poll::Ready(Err(Error::PoolDisabled))
        } else {
            // There's a new waiter, already registered in self.checkout()
            debug_assert!(self.waiter.is_some());
            Poll::Pending
        }
    }
}

impl<T, K: Key> Drop for Checkout<T, K> {
    fn drop(&mut self) {
        if self.waiter.take().is_some() {
            trace!("checkout dropped for {:?}", self.key);
            if let Some(Ok(mut inner)) = self.pool.inner.as_ref().map(|i| i.lock()) {
                inner.clean_waiters(&self.key);
            }
        }
    }
}

// FIXME: allow() required due to `impl Trait` leaking types to this lint
#[allow(missing_debug_implementations)]
pub struct Connecting<T: Poolable, K: Key> {
    key: K,
    ver: Ver,
    pool: WeakOpt<Mutex<PoolInner<T, K>>>,
}

impl<T: Poolable, K: Key> Connecting<T, K> {
    pub fn alpn_h2(mut self, pool: &Pool<T, K>) -> Option<Self> {
        debug_assert!(
            self.ver != Ver::Http2,
            "Connecting::alpn_h2 but already Http2"
        );
        if let Some(enabled) = &pool.inner {
            let mut inner = enabled.lock().unwrap();
            if !inner.connecting.insert(self.key.clone()) {
                trace!("HTTP/2 connecting already in progress for {:?}", self.key);
                // This attempt is abandoned, not failed: the request will be
                // served by the connection that won, so no waiter is canceled.
                inner.connected(&self.key, self.ver);
                self.pool = WeakOpt::none();
                return None;
            }
        }
        self.ver = Ver::Http2;
        Some(self)
    }
}

impl<T: Poolable, K: Key> Drop for Connecting<T, K> {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            // No need to panic on drop, that could abort!
            if let Ok(mut inner) = pool.lock() {
                // Only a failed connect gets here: `pooled` clears the pool
                // reference. A request waiting at the cap counted on it.
                inner.connected(&self.key, self.ver);
                inner.cancel_one_waiter(&self.key);
            }
        }
    }
}

struct Expiration(Option<Duration>);

impl Expiration {
    fn new(dur: Option<Duration>) -> Expiration {
        Expiration(dur)
    }

    fn expires(&self, instant: Instant, now: Instant) -> bool {
        match self.0 {
            // Avoid `Instant::elapsed` to avoid issues like rust-lang/rust#86470.
            Some(timeout) => now.saturating_duration_since(instant) > timeout,
            None => false,
        }
    }
}

struct IdleTask<T, K: Key> {
    timer: Timer,
    duration: Duration,
    pool: WeakOpt<Mutex<PoolInner<T, K>>>,
    // This allows the IdleTask to be notified as soon as the entire
    // Pool is fully dropped, and shutdown. This channel is never sent on,
    // but Err(Canceled) will be received when the Pool is dropped.
    pool_drop_notifier: oneshot::Receiver<Infallible>,
}

impl<T: Poolable + 'static, K: Key> IdleTask<T, K> {
    async fn run(self) {
        use futures_util::future;

        let mut sleep = self.timer.sleep_until(self.timer.now() + self.duration);
        let mut on_pool_drop = self.pool_drop_notifier;
        loop {
            match future::select(&mut on_pool_drop, &mut sleep).await {
                future::Either::Left(_) => {
                    // pool dropped, bah-bye
                    break;
                }
                future::Either::Right(((), _)) => {
                    if let Some(inner) = self.pool.upgrade() {
                        if let Ok(mut inner) = inner.lock() {
                            trace!("idle interval checking for expired");
                            inner.clear_expired();
                            if inner.idle.is_empty() {
                                inner.idle_interval_ref = None;
                                trace!("pool empty, canceling idle interval");
                                return;
                            }
                        }
                    }

                    let deadline = self.timer.now() + self.duration;
                    self.timer.reset(&mut sleep, deadline);
                }
            }
        }

        trace!("pool closed, canceling idle interval");
        return;
    }
}

impl<T> WeakOpt<T> {
    fn none() -> Self {
        WeakOpt(None)
    }

    fn downgrade(arc: &Arc<T>) -> Self {
        WeakOpt(Some(Arc::downgrade(arc)))
    }

    fn upgrade(&self) -> Option<Arc<T>> {
        self.0.as_ref().and_then(Weak::upgrade)
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::hash::Hash;
    use std::pin::Pin;
    use std::task::{self, Poll};
    use std::time::Duration;

    use super::{Connecting, Key, Pool, Poolable, Reservation, Ver, WeakOpt};
    use crate::rt::{TokioExecutor, TokioTimer};

    use crate::common::timer;

    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    struct KeyImpl(http::uri::Scheme, http::uri::Authority);

    type KeyTuple = (http::uri::Scheme, http::uri::Authority);

    /// Test unique reservations.
    #[derive(Debug, PartialEq, Eq)]
    struct Uniq<T>(T);

    impl<T: Send + 'static + Unpin> Poolable for Uniq<T> {
        fn is_open(&self) -> bool {
            true
        }

        fn reserve(self) -> Reservation<Self> {
            Reservation::Unique(self)
        }

        fn can_share(&self) -> bool {
            false
        }
    }

    fn c<T: Poolable, K: Key>(key: K) -> Connecting<T, K> {
        Connecting {
            key,
            ver: Ver::Auto,
            pool: WeakOpt::none(),
        }
    }

    fn host_key(s: &str) -> KeyImpl {
        KeyImpl(http::uri::Scheme::HTTP, s.parse().expect("host key"))
    }

    fn pool_no_timer<T, K: Key>() -> Pool<T, K> {
        pool_max_idle_no_timer(usize::MAX)
    }

    fn pool_max_idle_no_timer<T, K: Key>(max_idle: usize) -> Pool<T, K> {
        let pool = Pool::new(
            super::Config {
                idle_timeout: Some(Duration::from_millis(100)),
                max_idle_per_host: max_idle,
                max_connections_per_address: usize::MAX,
                dns_load_balancing: false,
            },
            TokioExecutor::new(),
            Option::<timer::Timer>::None,
        );
        pool.no_timer();
        pool
    }

    #[tokio::test]
    async fn test_pool_checkout_smoke() {
        let pool = pool_no_timer();
        let key = host_key("foo");
        let pooled = pool.pooled(c(key.clone()), Uniq(41));

        drop(pooled);

        match pool.checkout(key).await {
            Ok(pooled) => assert_eq!(*pooled, Uniq(41)),
            Err(_) => panic!("not ready"),
        };
    }

    /// Helper to check if the future is ready after polling once.
    struct PollOnce<'a, F>(&'a mut F);

    impl<F, T, U> Future for PollOnce<'_, F>
    where
        F: Future<Output = Result<T, U>> + Unpin,
    {
        type Output = Option<()>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Self::Output> {
            match Pin::new(&mut self.0).poll(cx) {
                Poll::Ready(Ok(_)) => Poll::Ready(Some(())),
                Poll::Ready(Err(_)) => Poll::Ready(Some(())),
                Poll::Pending => Poll::Ready(None),
            }
        }
    }

    #[tokio::test]
    async fn test_pool_checkout_returns_none_if_expired() {
        let pool = pool_no_timer();
        let key = host_key("foo");
        let pooled = pool.pooled(c(key.clone()), Uniq(41));

        drop(pooled);
        tokio::time::sleep(pool.locked().timeout.unwrap()).await;
        let mut checkout = pool.checkout(key);
        let poll_once = PollOnce(&mut checkout);
        let is_not_ready = poll_once.await.is_none();
        assert!(is_not_ready);
    }

    #[tokio::test]
    async fn test_pool_checkout_removes_expired() {
        let pool = pool_no_timer();
        let key = host_key("foo");

        pool.pooled(c(key.clone()), Uniq(41));
        pool.pooled(c(key.clone()), Uniq(5));
        pool.pooled(c(key.clone()), Uniq(99));

        assert_eq!(
            pool.locked().idle.get(&key).map(|entries| entries.len()),
            Some(3)
        );
        tokio::time::sleep(pool.locked().timeout.unwrap()).await;

        let mut checkout = pool.checkout(key.clone());
        let poll_once = PollOnce(&mut checkout);
        // checkout.await should clean out the expired
        poll_once.await;
        assert!(!pool.locked().idle.contains_key(&key));
    }

    #[test]
    fn test_pool_max_idle_per_host() {
        let pool = pool_max_idle_no_timer(2);
        let key = host_key("foo");

        pool.pooled(c(key.clone()), Uniq(41));
        pool.pooled(c(key.clone()), Uniq(5));
        pool.pooled(c(key.clone()), Uniq(99));

        // pooled and dropped 3, max_idle should only allow 2
        assert_eq!(
            pool.locked().idle.get(&key).map(|entries| entries.len()),
            Some(2)
        );
    }

    #[tokio::test]
    async fn test_pool_timer_removes_expired_realtime() {
        test_pool_timer_removes_expired_inner().await
    }

    #[tokio::test(start_paused = true)]
    async fn test_pool_timer_removes_expired_faketime() {
        test_pool_timer_removes_expired_inner().await
    }

    async fn test_pool_timer_removes_expired_inner() {
        let pool = Pool::new(
            super::Config {
                idle_timeout: Some(Duration::from_millis(10)),
                max_idle_per_host: usize::MAX,
                max_connections_per_address: usize::MAX,
                dns_load_balancing: false,
            },
            TokioExecutor::new(),
            Some(TokioTimer::new()),
        );

        let key = host_key("foo");

        pool.pooled(c(key.clone()), Uniq(41));
        pool.pooled(c(key.clone()), Uniq(5));
        pool.pooled(c(key.clone()), Uniq(99));

        assert_eq!(
            pool.locked().idle.get(&key).map(|entries| entries.len()),
            Some(3)
        );
        assert!(pool.locked().idle_interval_ref.is_some());

        // Let the timer tick passed the expiration...
        tokio::time::sleep(Duration::from_millis(30)).await;

        // But minimum interval is higher, so nothing should have been reaped
        assert_eq!(
            pool.locked().idle.get(&key).map(|entries| entries.len()),
            Some(3)
        );
        assert!(pool.locked().idle_interval_ref.is_some());

        // Now wait passed the minimum interval more
        tokio::time::sleep(Duration::from_millis(70)).await;
        // Yield in case other task hasn't been able to run :shrug:
        tokio::task::yield_now().await;

        assert!(!pool.locked().idle.contains_key(&key));
        assert!(pool.locked().idle_interval_ref.is_none());

        // Insert new key and check timer is recreated
        pool.pooled(c(key.clone()), Uniq(7));
        assert!(pool.locked().idle_interval_ref.is_some());
    }

    #[tokio::test]
    async fn test_pool_checkout_task_unparked() {
        use futures_util::FutureExt;
        use futures_util::future::join;

        let pool = pool_no_timer();
        let key = host_key("foo");
        let pooled = pool.pooled(c(key.clone()), Uniq(41));

        let checkout = join(pool.checkout(key), async {
            // the checkout future will park first,
            // and then this lazy future will be polled, which will insert
            // the pooled back into the pool
            //
            // this test makes sure that doing so will unpark the checkout
            drop(pooled);
        })
        .map(|(entry, _)| entry);

        assert_eq!(*checkout.await.unwrap(), Uniq(41));
    }

    #[tokio::test]
    async fn test_pool_checkout_drop_cleans_up_waiters() {
        let pool = pool_no_timer::<Uniq<i32>, KeyImpl>();
        let key = host_key("foo");

        let mut checkout1 = pool.checkout(key.clone());
        let mut checkout2 = pool.checkout(key.clone());

        let poll_once1 = PollOnce(&mut checkout1);
        let poll_once2 = PollOnce(&mut checkout2);

        // first poll needed to get into Pool's parked
        poll_once1.await;
        assert_eq!(pool.locked().waiters.get(&key).unwrap().len(), 1);
        poll_once2.await;
        assert_eq!(pool.locked().waiters.get(&key).unwrap().len(), 2);

        // on drop, clean up Pool
        drop(checkout1);
        assert_eq!(pool.locked().waiters.get(&key).unwrap().len(), 1);

        drop(checkout2);
        assert!(!pool.locked().waiters.contains_key(&key));
    }

    #[derive(Debug)]
    struct CanClose {
        #[allow(unused)]
        val: i32,
        closed: bool,
    }

    impl Poolable for CanClose {
        fn is_open(&self) -> bool {
            !self.closed
        }

        fn reserve(self) -> Reservation<Self> {
            Reservation::Unique(self)
        }

        fn can_share(&self) -> bool {
            false
        }
    }

    #[test]
    fn pooled_drop_if_closed_doesnt_reinsert() {
        let pool = pool_no_timer();
        let key = host_key("foo");
        pool.pooled(
            c(key.clone()),
            CanClose {
                val: 57,
                closed: true,
            },
        );

        assert!(!pool.locked().idle.contains_key(&key));
    }
}
