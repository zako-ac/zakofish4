use async_trait::async_trait;
use zakofish4_common::action::DisconnectReason;
use zakofish4_common::event::RequestOutcome;
use zakofish4_common::messages::{TapClientHello, TapServerReject};
use zakofish4_common::model::{RequestId, TapId};

use crate::handle::TapHandle;

/// Everything the driver needs from the outside world.
///
/// Deliberately narrow. The state machine decides *what* should happen; this
/// trait is only how the answers get fetched and the results delivered, which
/// keeps gateway plumbing separate from audio-request business logic. Without
/// that separation, WebSocket termination could never later be split into its
/// own service.
#[async_trait]
pub trait HubBackend: Send + Sync + 'static {
    /// Check a tap's credentials.
    async fn validate(&self, hello: &TapClientHello) -> Result<(), TapServerReject>;

    /// A tap is now serving requests. The handle is how requests get dispatched
    /// to it; the backend registers it and drops it on disconnect.
    async fn on_authenticated(&self, hello: &TapClientHello, handle: TapHandle);

    /// A request reached a terminal state. Resolve whoever was waiting.
    async fn on_complete(&self, tap_id: &TapId, request_id: RequestId, outcome: RequestOutcome);

    /// The connection ended.
    ///
    /// `reason` is `None` for a clean close. It is recorded because
    /// distinguishing "the tap left" from "we dropped it, and why" is the
    /// evidence for whether this protocol actually disconnects less than the
    /// one it replaces.
    async fn on_disconnected(&self, tap_id: Option<&TapId>, reason: Option<DisconnectReason>);
}
