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

pub mod builder;
pub mod error;
pub mod handler;
mod runtime;
pub mod source;
pub mod stream;

pub use builder::{TapBuilder, tap};
pub use error::{SdkError, TapError};
pub use handler::TapHandler;
pub use source::AudioSource;
pub use stream::{AudioStreamSender, FRAME_MS};

pub use zakofish4_common as common;
pub use zakofish4_common::messages::{AudioMetadataSuccessMessage, AudioRequestSuccessMessage};
pub use zakofish4_common::model::{
    AudioCachePolicy, AudioCacheType, AudioMetadata, DiscordUserId, TapId,
};
pub use zakofish4_common::messages::AttachedMetadata;
