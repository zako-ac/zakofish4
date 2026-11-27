//! SDK for building a Zako4 tap.
//!
//! A tap holds one WebSocket to the hub carrying control only, and sends audio
//! straight to whichever sink the hub nominates over protofish4/UDP. Both are
//! handled here: implement [`TapHandler`], push Opus frames into the
//! [`AudioStreamSender`] you are handed, and the SDK does the rest.
//!
//! ```no_run
//! use std::sync::Arc;
//! use zakofish4_tap::tap;
//! # async fn run(handler: Arc<dyn zakofish4_tap::TapHandler>) -> Result<(), Box<dyn std::error::Error>> {
//! tap()
//!     .hub("wss://api.zako.ac/gateway")
//!     .tap_id("299520271348404224")
//!     .friendly_name("YouTube Tap")
//!     .api_token("zk_...")
//!     .run(handler)
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! Three things the SDK handles that a tap would otherwise get wrong:
//!
//! - **Pacing.** A tap decoding from a file emits frames far faster than
//!   realtime, which saturates a residential uplink and causes the very loss
//!   the transport then has to repair. Frames are held to roughly realtime,
//!   with a configurable lead so playback still starts promptly.
//! - **Resumption.** A dropped WebSocket does not stop a transfer in progress.
//!   In-flight requests are re-announced on reconnect so the hub re-adopts them
//!   rather than treating the audio as lost.
//! - **Delivery.** `deliver_to` is a preference list, so a tap follows the
//!   move from a shared proxy to per-sink addresses with no code change.
//!
//! Two optional features carry over from the protofish3-era SDK unchanged, so
//! that migrating an existing tap really is a dependency swap and a new hub
//! URL:
//!
//! - `auto-encode` — [`decode_and_stream`], which pipes an encoded file through
//!   ffmpeg and sends the Opus frames out.
//! - `healthcheck` — [`TapBuilder::healthcheck_port`], serving `GET /health`.
//!
//! Two more things the SDK does on its own, because they are the difference
//! between a wedged tap and one that comes back:
//!
//! - **Drain.** `ZAKO_DRAIN_TIMEOUT_SECS` after SIGTERM, accepted transfers are
//!   given a window to finish before the process exits.
//! - **The request deadline.** A request in flight with nothing written back for
//!   `TAP_REQUEST_DEADLINE_SECS` (default 60) ends the process with exit code 70,
//!   so k8s restarts it and the reason lands in the log and in
//!   `tap_request_deadline_exceeded_total`. See [`watchdog`].

pub mod builder;
#[cfg(feature = "auto-encode")]
pub mod encode;
pub mod error;
pub mod handler;
#[cfg(feature = "healthcheck")]
mod healthcheck;
pub mod metrics;
mod runtime;
pub mod source;
pub mod stream;
pub mod watchdog;

pub use builder::{TapBuilder, tap};
#[cfg(feature = "auto-encode")]
pub use encode::{EncodeError, decode_and_stream};
pub use error::{SdkError, TapError};
pub use handler::TapHandler;
pub use metrics::{RequestKind, TapMetrics, TapSnapshot, unix_millis};
pub use source::AudioSource;
pub use stream::{AudioStreamSender, FRAME_MS};
pub use watchdog::{DEFAULT_REQUEST_DEADLINE, REQUEST_DEADLINE_EXIT_CODE};

pub use zakofish4_common as common;
pub use zakofish4_common::messages::AttachedMetadata;
pub use zakofish4_common::messages::ProbeResult;
pub use zakofish4_common::messages::{AudioMetadataSuccessMessage, AudioRequestSuccessMessage};
pub use zakofish4_common::model::{
    AudioCachePolicy, AudioCacheType, AudioMetadata, DiscordUserId, TapId,
};
