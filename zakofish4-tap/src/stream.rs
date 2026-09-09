use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;

/// One Opus frame on its way to the sink.
pub(crate) struct OutFrame {
    pub ts_ms: u64,
    pub payload: Bytes,
}

/// Standard Opus framing the hub assumes: 48 kHz, 960 samples, 20 ms a frame.
pub const FRAME_MS: u64 = 20;

/// Handle for pushing Opus frames to whichever sink the hub nominated.
///
/// The implementor never sees a socket, an address or a key. Dropping this ends
/// the transfer cleanly; the runtime then reports the outcome to the hub.
///
/// `send_frame` applies backpressure and will wait. That is deliberate — see
/// the pacing note below — so a tap should push frames as it produces them rather than
/// buffering a whole track first.
pub struct AudioStreamSender {
    tx: mpsc::Sender<OutFrame>,
}

impl AudioStreamSender {
    pub(crate) fn new(tx: mpsc::Sender<OutFrame>) -> Self {
        Self { tx }
    }

    /// Send one frame with an explicit timestamp in milliseconds.
    ///
    /// Returns `false` once the sink has gone away and frames are no longer
    /// being consumed; stop sending at that point.
    pub async fn send_frame(&self, ts_ms: u64, data: Bytes) -> bool {
        self.tx.send(OutFrame { ts_ms, payload: data }).await.is_ok()
    }

    /// Send the `frame_index`-th 20 ms Opus frame, computing its timestamp.
    pub async fn send_opus_frame(&self, frame_index: u64, data: Bytes) -> bool {
        self.send_frame(frame_index * FRAME_MS, data).await
    }
}

/// Holds a tap to roughly realtime.
///
/// Without this a tap decoding from a file or a subprocess emits frames as fast
/// as it can parse them — a five-minute track is about 4 MB, which goes out in
/// a couple of seconds and saturates a residential uplink. That causes exactly
/// the mass loss the retransmission machinery then has to repair, so it is far
/// cheaper to simply not send that fast.
///
/// A lead is allowed so playback still starts promptly: the first
/// `lead` worth of audio goes out as fast as it is produced, and only after
/// that does the pacer hold the sender to wall-clock time.
pub(crate) struct Pacer {
    start: tokio::time::Instant,
    lead: Duration,
}

impl Pacer {
    pub fn new(lead: Duration) -> Self {
        Self { start: tokio::time::Instant::now(), lead }
    }

    /// Wait until this frame is due.
    pub async fn wait_for(&self, ts_ms: u64) {
        let due = self.start + Duration::from_millis(ts_ms);
        let release = due.checked_sub(self.lead).unwrap_or(self.start);
        if release > tokio::time::Instant::now() {
            tokio::time::sleep_until(release).await;
        }
    }
}
