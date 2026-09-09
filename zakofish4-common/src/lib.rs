//! Wire messages and the sans-IO hub state machine for the Zako4 tap gateway.
//!
//! A tap holds one WebSocket to HQ carrying control only: authentication,
//! audio and metadata requests, cancellation, and stream outcomes. The audio
//! itself never crosses this connection — it goes straight from the tap to a
//! sink over protofish4/UDP, addressed by the `deliver_to` and `encryption_key`
//! in each request.
//!
//! [`state::handle_event`] is the whole protocol: an event in, a new state and
//! a list of actions out. It reads no clock and touches no socket, so timeouts
//! arrive as [`event::HubEvent::TimerFired`] and leave as
//! [`action::HubAction::StartTimer`] — which is what keeps the state machine,
//! rather than whichever adapter is driving it, the single source of truth for
//! what is outstanding.

pub mod action;
pub mod codec;
pub mod config;
pub mod error;
pub mod event;
pub mod messages;
pub mod model;
pub mod state;

pub use action::{DisconnectReason, HubAction};
pub use codec::{CodecError, decode_from_hub, decode_from_tap, encode_to_hub, encode_to_tap};
pub use config::HubConfig;
pub use error::HubError;
pub use event::{HubEvent, RequestOutcome, TimerId};
pub use messages::{
    HubToTapMessage, PROTOCOL_VERSION, Request, RequestVariant, Response, ResponseVariant,
    TapClientHello, TapToHubMessage,
};
pub use model::{
    AudioRequestString, DeliverTo, DiscordUserId, EncryptionKey, RequestId, StreamOutcome, TapId,
};
pub use state::{Kind, PendingRequest, Phase, TapState, Tracked, handle_event, on_connect, on_disconnect};
