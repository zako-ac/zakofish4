//! The tap's own request deadline: when a request stops making progress, the
//! process ends itself instead of serving a connection it will never answer.
//!
//! This is the protofish4 half of the self-heal that `zako3-tap-sdk` grew for
//! protofish3 (zako-ac/zako3#128). `/health` already reports the same state — a
//! request in flight with nothing written back past `TAP_HEALTH_STALL_SECS` —
//! and a liveness probe acts on it. That path has two hops of latency before
//! kubelet sends a signal (probe period x failureThreshold) and it cannot run at
//! all when the process has stopped scheduling. This module is the other half:
//!
//! - **the watchdog** fires at exactly the deadline, from inside the process,
//!   with the numbers that explain it, and works whenever the runtime is still
//!   scheduling even if every request handler is stuck;
//! - **the liveness probe** fires when the process is so far gone that it cannot
//!   run its own watchdog (the `SIGSTOP` shape measured on 2026-11-27 04:38 KST:
//!   the port stays open, `/health` never answers, nothing inside runs again).
//!
//! The two overlap on purpose and neither replaces the other. Where both would
//! fire, the watchdog wins the race because it is not gated on a probe period.
//!
//! ## Why an explicit exit, and not abort or a memory kill
//!
//! The point of the deadline is that k8s restarts the pod *and* that the reason
//! is readable afterwards. `abort()` is a SIGABRT with a core-dump shape: the
//! log ends mid-word and a core dump on a container filesystem is a cost with no
//! reader. Deliberately exhausting memory turns the reason into `OOMKilled`,
//! which is a lie about what happened and sends the next person looking for a
//! leak. `std::process::exit` with a code of its own is the only one of the
//! three that leaves a clean verdict:
//!
//! - the last log line is the breach, with `in_flight`, `silent_ms` and the
//!   deadline;
//! - `tap_request_deadline_exceeded_total` is incremented before the exit (a
//!   scrape only sees it if one lands in that window — the log is the durable
//!   record, the counter is the dashboard's);
//! - the container exits non-zero, so k8s restarts it under
//!   `restartPolicy: Always`, and the pod's `RESTARTS` counter carries the fact
//!   forward even though the process's own counters are gone.

use std::sync::Arc;
use std::time::Duration;

use crate::metrics::{TapMetrics, TapSnapshot, unix_millis};

/// How long a request may be in flight with nothing written back before the tap
/// ends itself.
///
/// Above every request any tap has been measured serving (supertonic synthesis:
/// p99 635ms, worst 753ms over a day) by two orders of magnitude, and above the
/// 20s `/health` threshold so the state-based probe still has something to say
/// before the process takes itself out. Overridable with
/// `TAP_REQUEST_DEADLINE_SECS`; `0` disables the watchdog. The name is the pf3
/// SDK's, so one variable in `taps-common-config` serves either transport.
pub const DEFAULT_REQUEST_DEADLINE: Duration = Duration::from_secs(60);

/// How often the watchdog reads the request state.
///
/// One timer a second per tap. The deadline is a minute, so the granularity of
/// the check is not what decides when it fires.
pub const DEFAULT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// The exit code of a tap that ended itself on a request deadline.
///
/// `sysexits.h`'s `EX_SOFTWARE` (70). Distinct from 1 (a start-up failure) and
/// from 137 (SIGKILL, which is what an unresponsive pod looks like when the
/// kubelet gives up), so `kubectl get pod -o json` says which of the three
/// happened.
pub const REQUEST_DEADLINE_EXIT_CODE: i32 = 70;

/// A request that has been in flight with nothing moving for too long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Breach {
    /// Requests being served when the deadline passed.
    pub in_flight: u64,
    /// How long nothing had been written back, in milliseconds.
    pub silent_ms: u64,
}

/// The decision, as a pure function of a snapshot and the clock.
///
/// Two properties carry the design:
///
/// - **Idle is never a breach.** A tap nobody is using is not stuck; counting
///   silence alone would restart every replica of a quiet voice channel.
/// - **Silence is measured from the last activity**, which the request path
///   stamps on arrival and again on every audio frame. A request that has been
///   streaming for ten minutes is making progress and must not be killed; a
///   request that was taken off the wire and then produced nothing is exactly
///   the S2b signature.
pub fn breach(snapshot: &TapSnapshot, now_ms: u64, deadline: Duration) -> Option<Breach> {
    if snapshot.in_flight == 0 {
        return None;
    }

    // A tap that has never served anything has no activity timestamp; it cannot
    // have a request in flight without one either, so this only guards against
    // a snapshot read mid-update.
    if snapshot.last_activity_at_ms == 0 {
        return None;
    }

    let silent_ms = now_ms.saturating_sub(snapshot.last_activity_at_ms);
    (silent_ms > deadline.as_millis() as u64).then_some(Breach {
        in_flight: snapshot.in_flight,
        silent_ms,
    })
}

/// Start the watchdog on the current runtime.
///
/// Called once from the tap builder, before the first connection. A spawn rather
/// than an await: the tap's own loop owns the process's task, and the watchdog
/// must keep running whatever the connection is doing.
pub fn spawn(metrics: Arc<TapMetrics>, deadline: Duration) {
    tokio::spawn(run(metrics, deadline, DEFAULT_CHECK_INTERVAL));
}

/// The watchdog loop. Never returns: a breach ends the process.
pub async fn run(metrics: Arc<TapMetrics>, deadline: Duration, interval: Duration) -> ! {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let snapshot = metrics.snapshot();
        if let Some(breach) = breach(&snapshot, unix_millis(), deadline) {
            expire(&metrics, &snapshot, breach, deadline);
        }
    }
}

/// Record the breach and end the process. Never returns.
fn expire(metrics: &TapMetrics, snapshot: &TapSnapshot, breach: Breach, deadline: Duration) -> ! {
    metrics.request_deadline_exceeded();

    tracing::error!(
        in_flight = breach.in_flight,
        silent_ms = breach.silent_ms,
        deadline_ms = deadline.as_millis() as u64,
        last_request_at_ms = snapshot.last_request_at_ms,
        last_reply_at_ms = snapshot.last_reply_at_ms,
        exit_code = REQUEST_DEADLINE_EXIT_CODE,
        "{}",
        reason(breach, deadline)
    );

    std::process::exit(REQUEST_DEADLINE_EXIT_CODE)
}

/// The sentence the dying process leaves behind.
///
/// A function rather than a literal in the event above so a test can assert the
/// reason exists without spawning a subscriber that would have to outlive the
/// process it is watching. The numbers travel as structured fields beside it.
fn reason(breach: Breach, deadline: Duration) -> String {
    format!(
        "request deadline exceeded with in-flight work that stopped making progress after {}ms \
         (in_flight={}, deadline={}ms) — exiting so the pod restarts",
        breach.silent_ms,
        breach.in_flight,
        deadline.as_millis()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::RequestKind;

    fn snapshot(in_flight: u64, last_activity_at_ms: u64) -> TapSnapshot {
        TapSnapshot {
            in_flight,
            last_request_at_ms: last_activity_at_ms,
            last_reply_at_ms: last_activity_at_ms,
            last_activity_at_ms,
            connections: 1,
        }
    }

    /// A quiet channel is not a stuck tap: nothing in flight can never breach,
    /// however long the process has been up.
    #[test]
    fn an_idle_tap_never_breaches() {
        assert_eq!(
            breach(&snapshot(0, 1_000), 10_000_000, DEFAULT_REQUEST_DEADLINE),
            None
        );
        assert_eq!(
            breach(&snapshot(0, 0), 10_000_000, DEFAULT_REQUEST_DEADLINE),
            None
        );
    }

    /// The S2b shape: taken off the wire, then nothing.
    #[test]
    fn a_request_with_no_progress_past_the_deadline_breaches() {
        let arrived = 1_000_000;
        assert_eq!(
            breach(
                &snapshot(1, arrived),
                arrived + 59_999,
                DEFAULT_REQUEST_DEADLINE
            ),
            None,
            "one millisecond inside the deadline is not a breach"
        );
        assert_eq!(
            breach(
                &snapshot(1, arrived),
                arrived + 60_001,
                DEFAULT_REQUEST_DEADLINE
            ),
            Some(Breach {
                in_flight: 1,
                silent_ms: 60_001
            })
        );
    }

    /// A stream that keeps producing frames resets the clock: the deadline is on
    /// progress, not on how long a request has been running.
    #[test]
    fn a_frame_that_keeps_moving_is_never_a_breach() {
        let metrics = TapMetrics::new();
        let _guard = metrics.request_received(RequestKind::Audio);
        // Ten minutes of a long stream, a frame a second.
        for _ in 0..600 {
            metrics.reply_sent();
        }
        let snapshot = metrics.snapshot();
        assert_eq!(
            breach(&snapshot, unix_millis(), DEFAULT_REQUEST_DEADLINE),
            None,
            "a request that is still writing is still making progress"
        );
    }

    #[test]
    fn the_default_deadline_is_a_minute_and_the_exit_code_is_its_own() {
        assert_eq!(DEFAULT_REQUEST_DEADLINE, Duration::from_secs(60));
        assert_eq!(REQUEST_DEADLINE_EXIT_CODE, 70);
        assert_ne!(REQUEST_DEADLINE_EXIT_CODE, 1, "not a start-up failure");
        assert_ne!(REQUEST_DEADLINE_EXIT_CODE, 137, "not a SIGKILL");
    }

    /// The acceptance criterion, end to end: a request that stops making
    /// progress ends the process.
    ///
    /// Run in a child process because the thing being asserted *is* the exit:
    /// `std::process::exit` in-process would take the test harness with it. The
    /// child is this same test binary, filtered to this one test, with the env
    /// var telling it which half to be. The reason the exit leaves behind is
    /// asserted on [`reason`] in its own test, because a subscriber that prints
    /// it cannot outlive the process the way the container's own logging does.
    #[test]
    fn a_stalled_request_ends_the_process() {
        const CHILD: &str = "ZAKO_TAP_DEADLINE_SELFTEST";

        if std::env::var_os(CHILD).is_none() {
            let exe = std::env::current_exe().expect("the test binary's path");
            let output = std::process::Command::new(exe)
                .args([
                    "--exact",
                    "watchdog::tests::a_stalled_request_ends_the_process",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .expect("spawn the child test binary");

            assert_eq!(
                output.status.code(),
                Some(REQUEST_DEADLINE_EXIT_CODE),
                "the child did not exit on the deadline: {:?}\nstdout: {}\nstderr: {}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        // Child: a request arrives and is never answered, and the watchdog runs
        // with a deadline short enough to fit in a test.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build a runtime");

        let exit_code = runtime.block_on(async {
            let metrics = TapMetrics::new();
            let _in_flight = metrics.request_received(RequestKind::Audio);

            // Bounded here rather than in the watchdog: if the deadline logic
            // ever stops firing, the test has to fail rather than hang. 99 is
            // chosen so it cannot be mistaken for the real exit code.
            match tokio::time::timeout(
                Duration::from_secs(5),
                run(
                    metrics,
                    Duration::from_millis(50),
                    Duration::from_millis(10),
                ),
            )
            .await
            {
                Ok(never) => match never {},
                Err(_) => 99,
            }
        });

        std::process::exit(exit_code);
    }

    /// The reason a reader finds after the restart, and the counter the
    /// dashboard reads. Both are what makes the exit diagnosable rather than
    /// just another restart.
    #[test]
    fn the_breach_leaves_a_reason_and_moves_its_counter() {
        let breach = Breach {
            in_flight: 2,
            silent_ms: 61_500,
        };
        let text = reason(breach, DEFAULT_REQUEST_DEADLINE);
        assert!(text.contains("request deadline exceeded"), "{text}");
        assert!(text.contains("in_flight=2"), "{text}");
        assert!(text.contains("after 61500ms"), "{text}");
        assert!(text.contains("deadline=60000ms"), "{text}");

        let metrics = TapMetrics::new();
        assert_eq!(metrics.deadline_exceeded(), 0);
        metrics.request_deadline_exceeded();
        assert_eq!(metrics.deadline_exceeded(), 1);
        assert!(
            metrics
                .render()
                .contains("tap_request_deadline_exceeded_total 1"),
            "the counter has to be on /metrics or nobody can see it"
        );
    }
}
