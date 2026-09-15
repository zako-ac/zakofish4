use tokio::sync::mpsc;
use zakofish4_common::event::HubEvent;
use zakofish4_common::model::RequestId;
use zakofish4_common::state::PendingRequest;

/// Dispatches work into one live tap connection.
///
/// Cheap to clone. Every method is non-blocking and returns `false` once the
/// connection is gone, so a caller holding a stale handle degrades to "this tap
/// is unavailable" rather than hanging.
#[derive(Debug, Clone)]
pub struct TapHandle {
    tx: mpsc::Sender<HubEvent>,
}

impl TapHandle {
    pub(crate) fn new(tx: mpsc::Sender<HubEvent>) -> Self {
        Self { tx }
    }

    /// Ask this tap to serve a request.
    pub fn dispatch(&self, request: PendingRequest) -> bool {
        self.tx.try_send(HubEvent::DispatchRequest(request)).is_ok()
    }

    /// Tell the tap to stop streaming a request.
    ///
    /// Best effort by design: if the connection is gone the tap will not hear
    /// it, and the sink's silence plus the tap's own acknowledgement watchdog
    /// tear the transfer down instead.
    pub fn cancel(&self, request_id: RequestId) -> bool {
        self.tx
            .try_send(HubEvent::CancelRequest(request_id))
            .is_ok()
    }

    /// Ask this tap to prove it can still synthesize.
    ///
    /// Returns `false` once the connection is gone, like every other method
    /// here, so a caller holding a stale handle reads it as "no answer" rather
    /// than hanging on one.
    pub fn probe(&self, probe_id: u64) -> bool {
        self.tx.try_send(HubEvent::DispatchProbe(probe_id)).is_ok()
    }

    pub fn is_connected(&self) -> bool {
        !self.tx.is_closed()
    }
}
