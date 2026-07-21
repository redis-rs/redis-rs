//! Opt-in diagnostics for the asynchronous cluster driver.
//!
//! The counters in this module are deliberately aggregate-only: enabling the
//! feature does not allocate or emit an event for every command. Durations are
//! represented as count/total/max nanoseconds so callers can export them to
//! their metrics system without redis-rs choosing a histogram implementation.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Default)]
struct Timing {
    count: AtomicU64,
    total_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl Timing {
    fn record(&self, duration: Duration) {
        let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_nanos.fetch_add(nanos, Ordering::Relaxed);
        update_max_u64(&self.max_nanos, nanos);
    }

    fn snapshot(&self) -> ClusterTimingSnapshot {
        ClusterTimingSnapshot {
            count: self.count.load(Ordering::Relaxed),
            total_nanos: self.total_nanos.load(Ordering::Relaxed),
            max_nanos: self.max_nanos.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct Inner {
    api_calls: AtomicU64,
    enqueued: AtomicU64,
    driver_received: AtomicU64,
    routed: AtomicU64,
    node_enqueued: AtomicU64,
    socket_buffered: AtomicU64,
    socket_flushes: AtomicU64,
    responses_received: AtomicU64,
    driver_completed: AtomicU64,
    caller_delivered: AtomicU64,
    cancelled_before_dispatch: AtomicU64,
    cancelled_in_flight: AtomicU64,
    refresh_started: AtomicU64,
    refresh_completed: AtomicU64,
    refresh_failed: AtomicU64,
    reconnect_started: AtomicU64,
    reconnect_completed: AtomicU64,
    reconnect_failed: AtomicU64,
    reconnect_triggers: AtomicU64,
    reconnect_claimed: AtomicU64,
    reconnect_deduplicated: AtomicU64,
    reconnect_installed: AtomicU64,
    reconnect_discarded: AtomicU64,
    reconnect_active: AtomicUsize,
    max_reconnect_active: AtomicUsize,
    top_channel_immediate: AtomicU64,
    top_channel_full: AtomicU64,
    node_channel_immediate: AtomicU64,
    node_channel_full: AtomicU64,
    connection_lock_immediate: AtomicU64,
    connection_lock_contended: AtomicU64,
    driver_polls: AtomicU64,
    commands_dispatched: AtomicU64,
    completions_processed: AtomicU64,
    max_received_per_poll: AtomicUsize,
    max_dispatched_per_poll: AtomicUsize,
    max_completions_per_poll: AtomicUsize,
    top_level_queue_depth: AtomicUsize,
    max_top_level_queue_depth: AtomicUsize,
    pending_depth: AtomicUsize,
    max_pending_depth: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    enqueue_wait: Timing,
    cluster_queue_wait: Timing,
    routing: Timing,
    node_enqueue_wait: Timing,
    node_queue_wait: Timing,
    socket_flush: Timing,
    request_total: Timing,
    refresh: Timing,
    reconnect: Timing,
    connection_lock_wait: Timing,
    routing_sync_wall: Timing,
    routing_sync_cpu: Timing,
    driver_poll_wall: Timing,
    driver_poll_cpu: Timing,
    driver_poll_off_cpu: Timing,
    dispatch_wall: Timing,
    dispatch_cpu: Timing,
    dispatch_off_cpu: Timing,
    completion_wall: Timing,
    completion_cpu: Timing,
    completion_off_cpu: Timing,
    profile_sequence: AtomicU64,
}

/// Aggregate timing values for one phase of the request path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClusterTimingSnapshot {
    /// Number of observations.
    pub count: u64,
    /// Sum of all observed durations, in nanoseconds.
    pub total_nanos: u64,
    /// Largest observed duration, in nanoseconds.
    pub max_nanos: u64,
}

impl ClusterTimingSnapshot {
    /// Returns the arithmetic mean, or `None` when no values were observed.
    pub fn mean(&self) -> Option<Duration> {
        (self.count > 0).then(|| Duration::from_nanos(self.total_nanos / self.count))
    }

    /// Returns the maximum observed duration.
    pub fn max(&self) -> Duration {
        Duration::from_nanos(self.max_nanos)
    }

    fn merge(&mut self, other: &Self) {
        self.count = self.count.saturating_add(other.count);
        self.total_nanos = self.total_nanos.saturating_add(other.total_nanos);
        self.max_nanos = self.max_nanos.max(other.max_nanos);
    }

    fn delta_since(&self, earlier: &Self) -> Self {
        Self {
            count: self.count.saturating_sub(earlier.count),
            total_nanos: self.total_nanos.saturating_sub(earlier.total_nanos),
            // A cumulative maximum cannot be losslessly differenced. Retain it
            // so an interval exporter never hides a process-lifetime peak.
            max_nanos: self.max_nanos,
        }
    }
}

/// A point-in-time snapshot of async cluster-driver activity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterDiagnosticsSnapshot {
    /// API calls entering this `ClusterConnection` driver.
    pub api_calls: u64,
    /// Commands successfully enqueued on the top-level driver channel.
    pub enqueued: u64,
    /// Commands received by the cluster driver.
    pub driver_received: u64,
    /// Commands for which cluster routing completed.
    pub routed: u64,
    /// Commands successfully enqueued on a node connection pipeline.
    pub node_enqueued: u64,
    /// Commands copied into a node connection's socket codec buffer.
    pub socket_buffered: u64,
    /// Successful socket-buffer flush polls.
    pub socket_flushes: u64,
    /// RESP replies decoded by node connections.
    pub responses_received: u64,
    /// Requests completed by the cluster driver, before caller wake-up.
    pub driver_completed: u64,
    /// Requests observed as complete by their API caller.
    pub caller_delivered: u64,
    /// Cancelled requests discarded before node dispatch.
    pub cancelled_before_dispatch: u64,
    /// Cancelled requests removed while in flight in the cluster driver.
    pub cancelled_in_flight: u64,
    /// Topology refresh attempts started.
    pub refresh_started: u64,
    /// Successful topology refresh attempts.
    pub refresh_completed: u64,
    /// Failed topology refresh attempts.
    pub refresh_failed: u64,
    /// Node reconnect attempts started.
    pub reconnect_started: u64,
    /// Successful node reconnect attempts.
    pub reconnect_completed: u64,
    /// Failed node reconnect attempts.
    pub reconnect_failed: u64,
    /// Failure notifications that requested a node reconnect.
    pub reconnect_triggers: u64,
    /// Reconnect triggers that won single-flight ownership.
    pub reconnect_claimed: u64,
    /// Reconnect triggers suppressed because a repair was already active.
    pub reconnect_deduplicated: u64,
    /// Repaired connections atomically installed for routing.
    pub reconnect_installed: u64,
    /// Successfully-created connections discarded because another repair won.
    pub reconnect_discarded: u64,
    /// Current number of connection attempts in progress.
    pub reconnect_active: usize,
    /// Largest number of concurrent connection attempts.
    pub max_reconnect_active: usize,
    /// Top-level channel admissions that obtained capacity immediately.
    pub top_channel_immediate: u64,
    /// Top-level channel admissions that initially found the channel full.
    pub top_channel_full: u64,
    /// Node-pipeline admissions that obtained capacity immediately.
    pub node_channel_immediate: u64,
    /// Node-pipeline admissions that initially found the channel full.
    pub node_channel_full: u64,
    /// Connection-map read locks obtained without waiting.
    pub connection_lock_immediate: u64,
    /// Connection-map read locks that initially reported contention.
    pub connection_lock_contended: u64,
    /// Calls to the cluster driver's `poll_flush` implementation.
    pub driver_polls: u64,
    /// Requests dispatched by the cluster driver.
    pub commands_dispatched: u64,
    /// Ready requests processed from the in-flight set.
    pub completions_processed: u64,
    /// Largest top-level intake batch observed before a driver flush poll.
    pub max_received_per_poll: usize,
    /// Largest request-dispatch batch in one driver poll.
    pub max_dispatched_per_poll: usize,
    /// Largest ready-completion batch in one driver poll.
    pub max_completions_per_poll: usize,
    /// Current top-level intake backlog: commands in the bounded channel plus
    /// an item, if any, handed to the stream/sink adapter but not yet received
    /// by the cluster driver.
    pub top_level_queue_depth: usize,
    /// Largest observed top-level intake backlog. Because the forwarding
    /// adapter can hold one item, this can be one larger than channel capacity.
    pub max_top_level_queue_depth: usize,
    /// Current number of requests awaiting dispatch inside the driver.
    pub pending_depth: usize,
    /// Largest observed internal pending depth.
    pub max_pending_depth: usize,
    /// Current number of request futures owned by the cluster driver.
    pub in_flight: usize,
    /// Largest observed cluster-driver in-flight count.
    pub max_in_flight: usize,
    /// Time awaiting capacity in the top-level bounded channel.
    pub enqueue_wait: ClusterTimingSnapshot,
    /// Time from top-level enqueue completion until cluster-driver receipt.
    pub cluster_queue_wait: ClusterTimingSnapshot,
    /// Time spent selecting or establishing the routed node connection.
    pub routing: ClusterTimingSnapshot,
    /// Time awaiting capacity in a node connection's bounded pipeline channel.
    pub node_enqueue_wait: ClusterTimingSnapshot,
    /// Time queued between node-channel enqueue and node-driver receipt.
    pub node_queue_wait: ClusterTimingSnapshot,
    /// Time spent in successful node socket flush polls.
    pub socket_flush: ClusterTimingSnapshot,
    /// End-to-end time from cluster API entry until caller delivery.
    pub request_total: ClusterTimingSnapshot,
    /// Topology refresh duration.
    pub refresh: ClusterTimingSnapshot,
    /// Node reconnect-attempt duration.
    pub reconnect: ClusterTimingSnapshot,
    /// Wait after an initially-contended connection-map read lock.
    pub connection_lock_wait: ClusterTimingSnapshot,
    /// Sampled wall time spent in synchronous route/connection selection.
    pub routing_sync_wall: ClusterTimingSnapshot,
    /// Sampled thread CPU time spent in synchronous route/connection selection.
    pub routing_sync_cpu: ClusterTimingSnapshot,
    /// Sampled wall time inside one cluster-driver `poll_flush` invocation.
    pub driver_poll_wall: ClusterTimingSnapshot,
    /// Sampled thread CPU time inside one cluster-driver `poll_flush` invocation.
    pub driver_poll_cpu: ClusterTimingSnapshot,
    /// Sampled driver-poll wall time not accounted for by thread CPU time.
    pub driver_poll_off_cpu: ClusterTimingSnapshot,
    /// Sampled wall time spent dispatching pending requests.
    pub dispatch_wall: ClusterTimingSnapshot,
    /// Sampled thread CPU time spent dispatching pending requests.
    pub dispatch_cpu: ClusterTimingSnapshot,
    /// Sampled dispatch wall time not accounted for by thread CPU time.
    pub dispatch_off_cpu: ClusterTimingSnapshot,
    /// Sampled wall time spent polling ready request completions.
    pub completion_wall: ClusterTimingSnapshot,
    /// Sampled thread CPU time spent polling ready request completions.
    pub completion_cpu: ClusterTimingSnapshot,
    /// Sampled completion wall time not accounted for by thread CPU time.
    pub completion_off_cpu: ClusterTimingSnapshot,
}

impl ClusterDiagnosticsSnapshot {
    /// Returns counter and timing deltas since `earlier`.
    ///
    /// Current gauges and cumulative maxima are copied from the newer
    /// snapshot. Maxima cannot be losslessly differenced without resetting the
    /// shared instrumentation, which would make concurrent exporters race.
    pub fn delta_since(&self, earlier: &Self) -> Self {
        macro_rules! difference {
            ($result:ident; $($field:ident),+ $(,)?) => {
                $($result.$field = self.$field.saturating_sub(earlier.$field);)+
            };
        }

        let mut result = Self {
            max_received_per_poll: self.max_received_per_poll,
            max_dispatched_per_poll: self.max_dispatched_per_poll,
            max_completions_per_poll: self.max_completions_per_poll,
            top_level_queue_depth: self.top_level_queue_depth,
            max_top_level_queue_depth: self.max_top_level_queue_depth,
            pending_depth: self.pending_depth,
            max_pending_depth: self.max_pending_depth,
            in_flight: self.in_flight,
            max_in_flight: self.max_in_flight,
            reconnect_active: self.reconnect_active,
            max_reconnect_active: self.max_reconnect_active,
            enqueue_wait: self.enqueue_wait.delta_since(&earlier.enqueue_wait),
            cluster_queue_wait: self
                .cluster_queue_wait
                .delta_since(&earlier.cluster_queue_wait),
            routing: self.routing.delta_since(&earlier.routing),
            node_enqueue_wait: self
                .node_enqueue_wait
                .delta_since(&earlier.node_enqueue_wait),
            node_queue_wait: self.node_queue_wait.delta_since(&earlier.node_queue_wait),
            socket_flush: self.socket_flush.delta_since(&earlier.socket_flush),
            request_total: self.request_total.delta_since(&earlier.request_total),
            refresh: self.refresh.delta_since(&earlier.refresh),
            reconnect: self.reconnect.delta_since(&earlier.reconnect),
            connection_lock_wait: self
                .connection_lock_wait
                .delta_since(&earlier.connection_lock_wait),
            routing_sync_wall: self
                .routing_sync_wall
                .delta_since(&earlier.routing_sync_wall),
            routing_sync_cpu: self.routing_sync_cpu.delta_since(&earlier.routing_sync_cpu),
            driver_poll_wall: self.driver_poll_wall.delta_since(&earlier.driver_poll_wall),
            driver_poll_cpu: self.driver_poll_cpu.delta_since(&earlier.driver_poll_cpu),
            driver_poll_off_cpu: self
                .driver_poll_off_cpu
                .delta_since(&earlier.driver_poll_off_cpu),
            dispatch_wall: self.dispatch_wall.delta_since(&earlier.dispatch_wall),
            dispatch_cpu: self.dispatch_cpu.delta_since(&earlier.dispatch_cpu),
            dispatch_off_cpu: self.dispatch_off_cpu.delta_since(&earlier.dispatch_off_cpu),
            completion_wall: self.completion_wall.delta_since(&earlier.completion_wall),
            completion_cpu: self.completion_cpu.delta_since(&earlier.completion_cpu),
            completion_off_cpu: self
                .completion_off_cpu
                .delta_since(&earlier.completion_off_cpu),
            ..Self::default()
        };
        difference!(result;
            api_calls,
            enqueued,
            driver_received,
            routed,
            node_enqueued,
            socket_buffered,
            socket_flushes,
            responses_received,
            driver_completed,
            caller_delivered,
            cancelled_before_dispatch,
            cancelled_in_flight,
            refresh_started,
            refresh_completed,
            refresh_failed,
            reconnect_started,
            reconnect_completed,
            reconnect_failed,
            reconnect_triggers,
            reconnect_claimed,
            reconnect_deduplicated,
            reconnect_installed,
            reconnect_discarded,
            top_channel_immediate,
            top_channel_full,
            node_channel_immediate,
            node_channel_full,
            connection_lock_immediate,
            connection_lock_contended,
            driver_polls,
            commands_dispatched,
            completions_processed,
        );
        result
    }

    /// Merges another snapshot into this one.
    ///
    /// Counters, current gauges, and timing counts/totals are summed; maxima
    /// retain the largest value. This is useful for aggregating an application
    /// pool containing multiple independent `ClusterConnection` drivers.
    pub fn merge(&mut self, other: &Self) {
        macro_rules! sum {
            ($($field:ident),+ $(,)?) => {
                $(self.$field = self.$field.saturating_add(other.$field);)+
            };
        }
        macro_rules! maximum {
            ($($field:ident),+ $(,)?) => {
                $(self.$field = self.$field.max(other.$field);)+
            };
        }

        sum!(
            api_calls,
            enqueued,
            driver_received,
            routed,
            node_enqueued,
            socket_buffered,
            socket_flushes,
            responses_received,
            driver_completed,
            caller_delivered,
            cancelled_before_dispatch,
            cancelled_in_flight,
            refresh_started,
            refresh_completed,
            refresh_failed,
            reconnect_started,
            reconnect_completed,
            reconnect_failed,
            reconnect_triggers,
            reconnect_claimed,
            reconnect_deduplicated,
            reconnect_installed,
            reconnect_discarded,
            top_channel_immediate,
            top_channel_full,
            node_channel_immediate,
            node_channel_full,
            connection_lock_immediate,
            connection_lock_contended,
            driver_polls,
            commands_dispatched,
            completions_processed,
            top_level_queue_depth,
            pending_depth,
            in_flight,
            reconnect_active,
        );
        maximum!(
            max_received_per_poll,
            max_dispatched_per_poll,
            max_completions_per_poll,
            max_top_level_queue_depth,
            max_pending_depth,
            max_in_flight,
            max_reconnect_active,
        );
        self.enqueue_wait.merge(&other.enqueue_wait);
        self.cluster_queue_wait.merge(&other.cluster_queue_wait);
        self.routing.merge(&other.routing);
        self.node_enqueue_wait.merge(&other.node_enqueue_wait);
        self.node_queue_wait.merge(&other.node_queue_wait);
        self.socket_flush.merge(&other.socket_flush);
        self.request_total.merge(&other.request_total);
        self.refresh.merge(&other.refresh);
        self.reconnect.merge(&other.reconnect);
        self.connection_lock_wait.merge(&other.connection_lock_wait);
        self.routing_sync_wall.merge(&other.routing_sync_wall);
        self.routing_sync_cpu.merge(&other.routing_sync_cpu);
        self.driver_poll_wall.merge(&other.driver_poll_wall);
        self.driver_poll_cpu.merge(&other.driver_poll_cpu);
        self.driver_poll_off_cpu.merge(&other.driver_poll_off_cpu);
        self.dispatch_wall.merge(&other.dispatch_wall);
        self.dispatch_cpu.merge(&other.dispatch_cpu);
        self.dispatch_off_cpu.merge(&other.dispatch_off_cpu);
        self.completion_wall.merge(&other.completion_wall);
        self.completion_cpu.merge(&other.completion_cpu);
        self.completion_off_cpu.merge(&other.completion_off_cpu);
    }
}

/// Shared aggregate diagnostics for one async cluster connection driver.
///
/// Cloned `ClusterConnection` handles report the same diagnostics, matching
/// their shared driver and queue. A separately-created connection has its own
/// counters.
#[derive(Clone, Default)]
pub struct ClusterDiagnostics {
    inner: Arc<Inner>,
}

#[cfg(feature = "cluster-async-profiling")]
const PROFILE_SAMPLE_MASK: u64 = 1024 - 1;

#[cfg(feature = "cluster-async-profiling")]
#[derive(Clone, Copy)]
pub(crate) enum ProfilePhase {
    RoutingSync,
    DriverPoll,
    Dispatch,
    Completion,
}

/// A sampled synchronous section. Because a future cannot migrate while one
/// invocation of its `poll` method is executing, Linux thread CPU time can be
/// compared with wall time for these sections without attributing another
/// executor thread's work to this driver.
#[cfg(feature = "cluster-async-profiling")]
pub(crate) struct ProfileGuard {
    diagnostics: ClusterDiagnostics,
    phase: ProfilePhase,
    wall_started: Instant,
    cpu_started: Option<Duration>,
}

#[cfg(feature = "cluster-async-profiling")]
impl Drop for ProfileGuard {
    fn drop(&mut self) {
        let wall = self.wall_started.elapsed();
        let cpu = self
            .cpu_started
            .zip(thread_cpu_time())
            .map(|(started, finished)| finished.saturating_sub(started));
        self.diagnostics.record_profile(self.phase, wall, cpu);
    }
}

impl std::fmt::Debug for ClusterDiagnostics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.snapshot().fmt(f)
    }
}

impl ClusterDiagnostics {
    /// Captures the current counters, gauges, and aggregate timings.
    pub fn snapshot(&self) -> ClusterDiagnosticsSnapshot {
        let inner = &self.inner;
        ClusterDiagnosticsSnapshot {
            api_calls: load(&inner.api_calls),
            enqueued: load(&inner.enqueued),
            driver_received: load(&inner.driver_received),
            routed: load(&inner.routed),
            node_enqueued: load(&inner.node_enqueued),
            socket_buffered: load(&inner.socket_buffered),
            socket_flushes: load(&inner.socket_flushes),
            responses_received: load(&inner.responses_received),
            driver_completed: load(&inner.driver_completed),
            caller_delivered: load(&inner.caller_delivered),
            cancelled_before_dispatch: load(&inner.cancelled_before_dispatch),
            cancelled_in_flight: load(&inner.cancelled_in_flight),
            refresh_started: load(&inner.refresh_started),
            refresh_completed: load(&inner.refresh_completed),
            refresh_failed: load(&inner.refresh_failed),
            reconnect_started: load(&inner.reconnect_started),
            reconnect_completed: load(&inner.reconnect_completed),
            reconnect_failed: load(&inner.reconnect_failed),
            reconnect_triggers: load(&inner.reconnect_triggers),
            reconnect_claimed: load(&inner.reconnect_claimed),
            reconnect_deduplicated: load(&inner.reconnect_deduplicated),
            reconnect_installed: load(&inner.reconnect_installed),
            reconnect_discarded: load(&inner.reconnect_discarded),
            reconnect_active: load_usize(&inner.reconnect_active),
            max_reconnect_active: load_usize(&inner.max_reconnect_active),
            top_channel_immediate: load(&inner.top_channel_immediate),
            top_channel_full: load(&inner.top_channel_full),
            node_channel_immediate: load(&inner.node_channel_immediate),
            node_channel_full: load(&inner.node_channel_full),
            connection_lock_immediate: load(&inner.connection_lock_immediate),
            connection_lock_contended: load(&inner.connection_lock_contended),
            driver_polls: load(&inner.driver_polls),
            commands_dispatched: load(&inner.commands_dispatched),
            completions_processed: load(&inner.completions_processed),
            max_received_per_poll: load_usize(&inner.max_received_per_poll),
            max_dispatched_per_poll: load_usize(&inner.max_dispatched_per_poll),
            max_completions_per_poll: load_usize(&inner.max_completions_per_poll),
            top_level_queue_depth: load_usize(&inner.top_level_queue_depth),
            max_top_level_queue_depth: load_usize(&inner.max_top_level_queue_depth),
            pending_depth: load_usize(&inner.pending_depth),
            max_pending_depth: load_usize(&inner.max_pending_depth),
            in_flight: load_usize(&inner.in_flight),
            max_in_flight: load_usize(&inner.max_in_flight),
            enqueue_wait: inner.enqueue_wait.snapshot(),
            cluster_queue_wait: inner.cluster_queue_wait.snapshot(),
            routing: inner.routing.snapshot(),
            node_enqueue_wait: inner.node_enqueue_wait.snapshot(),
            node_queue_wait: inner.node_queue_wait.snapshot(),
            socket_flush: inner.socket_flush.snapshot(),
            request_total: inner.request_total.snapshot(),
            refresh: inner.refresh.snapshot(),
            reconnect: inner.reconnect.snapshot(),
            connection_lock_wait: inner.connection_lock_wait.snapshot(),
            routing_sync_wall: inner.routing_sync_wall.snapshot(),
            routing_sync_cpu: inner.routing_sync_cpu.snapshot(),
            driver_poll_wall: inner.driver_poll_wall.snapshot(),
            driver_poll_cpu: inner.driver_poll_cpu.snapshot(),
            driver_poll_off_cpu: inner.driver_poll_off_cpu.snapshot(),
            dispatch_wall: inner.dispatch_wall.snapshot(),
            dispatch_cpu: inner.dispatch_cpu.snapshot(),
            dispatch_off_cpu: inner.dispatch_off_cpu.snapshot(),
            completion_wall: inner.completion_wall.snapshot(),
            completion_cpu: inner.completion_cpu.snapshot(),
            completion_off_cpu: inner.completion_off_cpu.snapshot(),
        }
    }

    pub(crate) fn api_started(&self) {
        self.inner.api_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn enqueued(&self, started: Instant) {
        self.inner.enqueued.fetch_add(1, Ordering::Relaxed);
        self.inner.enqueue_wait.record(started.elapsed());
        increment_gauge(
            &self.inner.top_level_queue_depth,
            &self.inner.max_top_level_queue_depth,
        );
    }

    pub(crate) fn driver_received(&self, enqueued_at: Instant) {
        self.inner.driver_received.fetch_add(1, Ordering::Relaxed);
        self.inner.cluster_queue_wait.record(enqueued_at.elapsed());
        decrement_gauge(&self.inner.top_level_queue_depth);
    }

    pub(crate) fn pending_added(&self) {
        increment_gauge(&self.inner.pending_depth, &self.inner.max_pending_depth);
    }

    pub(crate) fn pending_removed(&self) {
        decrement_gauge(&self.inner.pending_depth);
    }

    pub(crate) fn routing_completed(&self, started: Instant) {
        self.inner.routed.fetch_add(1, Ordering::Relaxed);
        self.inner.routing.record(started.elapsed());
    }

    pub(crate) fn node_enqueued(&self, started: Instant) {
        self.inner.node_enqueued.fetch_add(1, Ordering::Relaxed);
        self.inner.node_enqueue_wait.record(started.elapsed());
    }

    pub(crate) fn socket_buffered(&self, enqueued_at: Instant) {
        self.inner.socket_buffered.fetch_add(1, Ordering::Relaxed);
        self.inner.node_queue_wait.record(enqueued_at.elapsed());
    }

    pub(crate) fn socket_flushed(&self, started: Instant) {
        self.inner.socket_flushes.fetch_add(1, Ordering::Relaxed);
        self.inner.socket_flush.record(started.elapsed());
    }

    pub(crate) fn response_received(&self) {
        self.inner
            .responses_received
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn driver_completed(&self) {
        self.inner.driver_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn caller_delivered(&self, started: Instant) {
        self.inner.caller_delivered.fetch_add(1, Ordering::Relaxed);
        self.inner.request_total.record(started.elapsed());
    }

    pub(crate) fn cancelled_before_dispatch(&self) {
        self.inner
            .cancelled_before_dispatch
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn cancelled_in_flight(&self) {
        self.inner
            .cancelled_in_flight
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn refresh_started(&self) -> Instant {
        self.inner.refresh_started.fetch_add(1, Ordering::Relaxed);
        Instant::now()
    }

    pub(crate) fn refresh_finished(&self, started: Instant, success: bool) {
        let counter = if success {
            &self.inner.refresh_completed
        } else {
            &self.inner.refresh_failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
        self.inner.refresh.record(started.elapsed());
    }

    pub(crate) fn reconnect_started(&self) -> Instant {
        self.inner.reconnect_started.fetch_add(1, Ordering::Relaxed);
        increment_gauge(
            &self.inner.reconnect_active,
            &self.inner.max_reconnect_active,
        );
        Instant::now()
    }

    pub(crate) fn reconnect_finished(&self, started: Instant, success: bool) {
        let counter = if success {
            &self.inner.reconnect_completed
        } else {
            &self.inner.reconnect_failed
        };
        counter.fetch_add(1, Ordering::Relaxed);
        decrement_gauge(&self.inner.reconnect_active);
        self.inner.reconnect.record(started.elapsed());
    }

    pub(crate) fn reconnect_triggered(&self, claimed: bool) {
        self.inner
            .reconnect_triggers
            .fetch_add(1, Ordering::Relaxed);
        let counter = if claimed {
            &self.inner.reconnect_claimed
        } else {
            &self.inner.reconnect_deduplicated
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn reconnect_install(&self, installed: bool) {
        let counter = if installed {
            &self.inner.reconnect_installed
        } else {
            &self.inner.reconnect_discarded
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn driver_poll(&self, received: usize) {
        self.inner.driver_polls.fetch_add(1, Ordering::Relaxed);
        update_max_usize(&self.inner.max_received_per_poll, received);
    }

    pub(crate) fn dispatch_batch(&self, count: usize, in_flight: usize) {
        self.inner
            .commands_dispatched
            .fetch_add(count as u64, Ordering::Relaxed);
        update_max_usize(&self.inner.max_dispatched_per_poll, count);
        self.inner.in_flight.store(in_flight, Ordering::Relaxed);
        update_max_usize(&self.inner.max_in_flight, in_flight);
    }

    pub(crate) fn completion_batch(&self, count: usize, in_flight: usize) {
        self.inner
            .completions_processed
            .fetch_add(count as u64, Ordering::Relaxed);
        update_max_usize(&self.inner.max_completions_per_poll, count);
        self.inner.in_flight.store(in_flight, Ordering::Relaxed);
    }

    #[cfg(feature = "cluster-async-profiling")]
    pub(crate) fn top_channel_admission(&self, immediate: bool) {
        let counter = if immediate {
            &self.inner.top_channel_immediate
        } else {
            &self.inner.top_channel_full
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "cluster-async-profiling")]
    pub(crate) fn node_channel_admission(&self, immediate: bool) {
        let counter = if immediate {
            &self.inner.node_channel_immediate
        } else {
            &self.inner.node_channel_full
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(all(
        feature = "cluster-async-profiling",
        not(feature = "cluster-async-atomic-node-connections")
    ))]
    pub(crate) fn connection_lock_acquired(&self, waited_since: Option<Instant>) {
        match waited_since {
            Some(started) => {
                self.inner
                    .connection_lock_contended
                    .fetch_add(1, Ordering::Relaxed);
                self.inner.connection_lock_wait.record(started.elapsed());
            }
            None => {
                self.inner
                    .connection_lock_immediate
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[cfg(feature = "cluster-async-profiling")]
    pub(crate) fn profile(&self, phase: ProfilePhase) -> Option<ProfileGuard> {
        let sequence = self.inner.profile_sequence.fetch_add(1, Ordering::Relaxed);
        if sequence & PROFILE_SAMPLE_MASK != 0 {
            return None;
        }
        Some(ProfileGuard {
            diagnostics: self.clone(),
            phase,
            wall_started: Instant::now(),
            cpu_started: thread_cpu_time(),
        })
    }

    #[cfg(feature = "cluster-async-profiling")]
    fn record_profile(&self, phase: ProfilePhase, wall: Duration, cpu: Option<Duration>) {
        let (wall_timing, cpu_timing, off_cpu_timing) = match phase {
            ProfilePhase::RoutingSync => {
                self.inner.routing_sync_wall.record(wall);
                if let Some(cpu) = cpu {
                    self.inner.routing_sync_cpu.record(cpu);
                }
                return;
            }
            ProfilePhase::DriverPoll => (
                &self.inner.driver_poll_wall,
                &self.inner.driver_poll_cpu,
                &self.inner.driver_poll_off_cpu,
            ),
            ProfilePhase::Dispatch => (
                &self.inner.dispatch_wall,
                &self.inner.dispatch_cpu,
                &self.inner.dispatch_off_cpu,
            ),
            ProfilePhase::Completion => (
                &self.inner.completion_wall,
                &self.inner.completion_cpu,
                &self.inner.completion_off_cpu,
            ),
        };
        wall_timing.record(wall);
        if let Some(cpu) = cpu {
            cpu_timing.record(cpu);
            off_cpu_timing.record(wall.saturating_sub(cpu));
        }
    }
}

#[cfg(all(feature = "cluster-async-profiling", target_os = "linux"))]
fn thread_cpu_time() -> Option<Duration> {
    let mut value = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: `clock_gettime` initializes the supplied `timespec` on success,
    // and the pointer is valid for the duration of the call.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, value.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: the return code above confirmed successful initialization.
    let value = unsafe { value.assume_init() };
    Some(Duration::new(value.tv_sec as u64, value.tv_nsec as u32))
}

#[cfg(all(feature = "cluster-async-profiling", not(target_os = "linux")))]
fn thread_cpu_time() -> Option<Duration> {
    None
}

fn load(value: &AtomicU64) -> u64 {
    value.load(Ordering::Relaxed)
}

fn load_usize(value: &AtomicUsize) -> usize {
    value.load(Ordering::Relaxed)
}

fn increment_gauge(value: &AtomicUsize, maximum: &AtomicUsize) {
    let next = value.fetch_add(1, Ordering::Relaxed) + 1;
    update_max_usize(maximum, next);
}

fn decrement_gauge(value: &AtomicUsize) {
    let _ = value.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}

fn update_max_u64(maximum: &AtomicU64, candidate: u64) {
    let _ = maximum.fetch_max(candidate, Ordering::Relaxed);
}

fn update_max_usize(maximum: &AtomicUsize, candidate: usize) {
    let _ = maximum.fetch_max(candidate, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_tracks_counts_gauges_and_maxima() {
        let diagnostics = ClusterDiagnostics::default();
        diagnostics.api_started();
        diagnostics.enqueued(Instant::now());
        diagnostics.driver_received(Instant::now());
        diagnostics.pending_removed();
        diagnostics.dispatch_batch(3, 3);
        diagnostics.completion_batch(2, 1);

        let snapshot = diagnostics.snapshot();
        assert_eq!(snapshot.api_calls, 1);
        assert_eq!(snapshot.enqueued, 1);
        assert_eq!(snapshot.driver_received, 1);
        assert_eq!(snapshot.top_level_queue_depth, 0);
        assert_eq!(snapshot.pending_depth, 0);
        assert_eq!(snapshot.commands_dispatched, 3);
        assert_eq!(snapshot.completions_processed, 2);
        assert_eq!(snapshot.max_dispatched_per_poll, 3);
        assert_eq!(snapshot.max_in_flight, 3);
        assert_eq!(snapshot.in_flight, 1);
    }
}
