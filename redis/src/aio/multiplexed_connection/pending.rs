//! Request deadlines and requests not written yet, for the multiplexed connection's driver.
//!
//! The driver arms one timer for the earliest deadline instead of a runtime timer per request.

use super::{InFlight, PipelineOutput, PipelineResponseExpectation};
use crate::aio::runtime::{DriverTimer, Elapsed, Runtime};
use async_lock::{Semaphore, SemaphoreGuardArc};
use std::collections::VecDeque;
use std::sync::Arc;
use std::task;
use std::time::{Duration, Instant};

/// Deadline for a request sent now. No deadline if it overflows `Instant` (`Duration::MAX`).
pub(super) fn deadline_after(timeout: Option<Duration>) -> Option<Instant> {
    timeout.and_then(|timeout| Runtime::locate().now().checked_add(timeout))
}

/// Released when the request is written or dropped.
pub(super) type BacklogSlot = SemaphoreGuardArc;

/// Limits how many requests without a response can wait to be written. Requests that wait for a
/// response don't need this, each handle has only one of them in flight.
#[derive(Clone)]
pub(super) struct Backlog(Arc<Semaphore>);

impl Backlog {
    pub(super) fn new(size: usize) -> Self {
        Self(Arc::new(Semaphore::new(size.max(1))))
    }

    /// Waits for a free slot if the request expects no response. Gives up at `deadline`.
    pub(super) async fn admit(
        &self,
        expects_response: bool,
        deadline: Option<Instant>,
    ) -> Result<Option<BacklogSlot>, Elapsed> {
        if expects_response {
            return Ok(None);
        }
        if let Some(slot) = self.0.try_acquire_arc() {
            return Ok(Some(slot));
        }
        let acquire = self.0.acquire_arc();
        let slot = match deadline {
            Some(deadline) => {
                let runtime = Runtime::locate();
                let remaining = deadline.saturating_duration_since(runtime.now());
                runtime.timeout(remaining, acquire).await?
            }
            None => acquire.await,
        };
        Ok(Some(slot))
    }
}

/// A request waiting for the transport to become writable.
pub(super) struct Staged {
    pub(super) input: Vec<u8>,
    pub(super) output: Option<PipelineOutput>,
    pub(super) expectation: Option<PipelineResponseExpectation>,
    pub(super) deadline: Option<Instant>,
    pub(super) _slot: Option<BacklogSlot>,
}

impl Staged {
    /// False once the request expired or its caller went away.
    fn is_live(&self) -> bool {
        match &self.output {
            Some(output) => !output.is_canceled(),
            None => self.deadline.is_none(),
        }
    }
}

/// Cancelled requests are pruned whenever the staged queue doubles, starting at this length.
const MIN_PRUNE_AT: usize = 64;

/// The driver's timer and its requests that are not written yet.
pub(super) struct Pending {
    timer: DriverTimer,
    /// The deadline the timer is armed for, if any.
    armed: Option<Instant>,
    staged: VecDeque<Staged>,
    /// Queue length at which to prune next.
    prune_at: usize,
    runtime: Runtime,
}

impl Pending {
    pub(super) fn new() -> Self {
        let runtime = Runtime::locate();
        Self {
            timer: runtime.driver_timer(),
            armed: None,
            staged: VecDeque::new(),
            prune_at: MIN_PRUNE_AT,
            runtime,
        }
    }

    /// Makes sure the timer fires no later than `deadline`.
    pub(super) fn track(&mut self, deadline: Option<Instant>) {
        if let Some(deadline) = deadline
            && self.armed.is_none_or(|armed| deadline < armed)
        {
            self.armed = Some(deadline);
            self.timer.arm(deadline);
        }
    }

    pub(super) fn has_staged(&self) -> bool {
        !self.staged.is_empty()
    }

    /// Stages a request until the transport is writable.
    pub(super) fn stage(&mut self, request: Staged) {
        // Otherwise callers that cancel and retry during a stall grow the queue forever.
        if self.staged.len() >= self.prune_at {
            self.staged.retain(Staged::is_live);
            self.prune_at = (self.staged.len() * 2).max(MIN_PRUNE_AT);
        }
        self.staged.push_back(request);
    }

    /// The oldest staged request that still has to be written.
    pub(super) fn next_staged(&mut self) -> Option<Staged> {
        while let Some(request) = self.staged.pop_front() {
            if request.is_live() {
                return Some(request);
            }
        }
        None
    }

    /// Fails all requests whose deadline has passed and re-arms the timer. Expired requests stay
    /// in `in_flight`, so their late replies are discarded.
    pub(super) fn poll_expired(
        &mut self,
        cx: &mut task::Context,
        in_flight: &mut VecDeque<InFlight>,
    ) {
        while let Some(fired) = self.armed {
            if self.timer.poll_elapsed(cx).is_pending() {
                return;
            }
            // The clock may read slightly before the deadline that fired.
            let now = self.runtime.now().max(fired);
            let mut next = None;
            for request in in_flight.iter_mut() {
                expire(&mut request.output, request.deadline, now, &mut next);
            }
            for request in self.staged.iter_mut() {
                expire(&mut request.output, request.deadline, now, &mut next);
            }
            self.staged.retain(Staged::is_live);
            self.armed = next;
            if let Some(next) = next {
                self.timer.arm(next);
            }
        }
    }
}

/// Fails a request whose deadline is at or before `now`, or lowers `next` to its deadline.
fn expire(
    output: &mut Option<PipelineOutput>,
    deadline: Option<Instant>,
    now: Instant,
    next: &mut Option<Instant>,
) {
    let (Some(deadline), Some(_)) = (deadline, output.as_ref()) else {
        return;
    };
    if deadline <= now {
        if let Some(output) = output.take() {
            _ = output.send(Err(Elapsed::new().into()));
        }
    } else {
        *next = Some(next.map_or(deadline, |next| next.min(deadline)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_channel::oneshot;

    /// Cancelled requests are pruned from the staged queue, live ones are kept.
    #[tokio::test]
    async fn test_cancelled_staged_requests_are_dropped() {
        let mut pending = Pending::new();
        let mut waiting = Vec::new();
        for i in 0..10_000 {
            let (output, receiver) = oneshot::channel();
            pending.stage(Staged {
                input: b"*1\r\n$4\r\nPING\r\n".to_vec(),
                output: Some(output),
                expectation: None,
                deadline: None,
                _slot: None,
            });
            // Every tenth caller keeps waiting; the others give up.
            if i % 10 == 0 {
                waiting.push(receiver);
            }
        }
        let live = pending
            .staged
            .iter()
            .filter(|request| request.is_live())
            .count();
        assert_eq!(live, waiting.len());
        assert!(
            pending.staged.len() <= 2 * waiting.len() + MIN_PRUNE_AT,
            "{} requests staged",
            pending.staged.len()
        );
    }
}
