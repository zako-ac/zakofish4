use serde::{Deserialize, Serialize};

use crate::model::{
    AudioCachePolicy, AudioMetadata, AudioRequestString, DeliverTo, DiscordUserId, EncryptionKey,
    HubRejectReasonType, RequestId, StreamOutcome, TapId,
};

/// The wire version a tap speaks.
///
/// Bump this when the schema changes incompatibly. It exists because taps are
/// run by third parties and cannot be upgraded in lockstep — and it is the one
/// field that genuinely cannot be added later, since without it the hub has no
/// way to tell an old tap from a malformed one.
///
/// Version 2 added [`HubToTapMessage::Probe`]. The message itself is additive,
/// so a v1 tap that receives one simply fails to decode it and ignores the
/// frame — which is fine, except that the hub cannot tell that apart from a tap
/// that is wedged, and would mark every un-upgraded tap unhealthy. The version
/// is therefore what the hub gates probing on: it probes v2 and above, and
/// leaves v1 taps exactly as they were.
pub const PROTOCOL_VERSION: u32 = 2;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum AttachedMetadata {
    /// Reuse whatever the cache already holds under this request's ARHash.
    /// The hub resolves it; the tap never sees the result.
    #[serde(rename = "use_cached")]
    UseCached,
    #[serde(rename = "metadatas")]
    Metadatas(Vec<AudioMetadata>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioRequestMessage {
    pub ars: AudioRequestString,
    /// Who is asking. The hub checks tap permissions against this.
    pub discord_user_id: DiscordUserId,
    /// Authenticates the UDP datagrams this request's audio travels in.
    pub encryption_key: EncryptionKey,
    /// Where to send the audio, in preference order.
    pub deliver_to: DeliverTo,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioMetadataRequestMessage {
    pub ars: AudioRequestString,
    pub discord_user_id: DiscordUserId,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioRequestSuccessMessage {
    pub cache: AudioCachePolicy,
    pub duration_secs: Option<f32>,
    pub metadatas: AttachedMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioMetadataSuccessMessage {
    pub metadatas: Vec<AudioMetadata>,
    pub cache: AudioCachePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioRequestFailureMessage {
    pub reason: String,
    /// Whether the hub should try a different tap. Maps to the SDK's
    /// `Retriable` / `Permanent` split so implementors never touch the flag.
    pub try_others: bool,
}

/// Metadata lookups fail differently from audio requests and need their own
/// variant: the hub distinguishes a tap-authored refusal ("video is
/// age-restricted", worth showing the user verbatim) from a transport failure,
/// and without this the two collapse into a generic error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioMetadataFailureMessage {
    pub reason: String,
    pub try_others: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TapClientHello {
    pub protocol_version: u32,
    pub tap_id: TapId,
    pub friendly_name: String,
    pub api_token: String,
    pub selection_weight: f32,
    /// Requests this tap is still streaming from a previous connection.
    ///
    /// A dropped WebSocket does not stop the audio, so a tap that reconnects
    /// may still be mid-transfer. Listing those here lets the hub re-attach
    /// them to the new connection instead of orphaning them.
    #[serde(default)]
    pub resuming: Vec<RequestId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TapServerReject {
    pub reason_type: HubRejectReasonType,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum RequestVariant {
    AudioRequest(AudioRequestMessage),
    AudioMetadataRequest(AudioMetadataRequestMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub request_id: RequestId,
    pub variant: RequestVariant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum ResponseVariant {
    AudioRequestSuccess(AudioRequestSuccessMessage),
    AudioRequestFailure(AudioRequestFailureMessage),
    AudioMetadataSuccess(AudioMetadataSuccessMessage),
    AudioMetadataFailure(AudioMetadataFailureMessage),
}

impl ResponseVariant {
    /// Whether this response answers an audio request rather than a metadata
    /// one. The hub uses it to reject a tap answering the wrong question.
    pub fn is_audio(&self) -> bool {
        matches!(
            self,
            ResponseVariant::AudioRequestSuccess(_) | ResponseVariant::AudioRequestFailure(_)
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub request_id: RequestId,
    pub variant: ResponseVariant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum HubToTapMessage {
    Accept,
    Reject(TapServerReject),

    Request(Request),

    /// Stop streaming this request; the listener is gone.
    ///
    /// With control and audio on separate transports the tap would otherwise
    /// happily stream a whole track into a dead session. Best-effort: if the
    /// WebSocket is down the sink simply stops acknowledging and the tap's own
    /// watchdog tears the transfer down instead.
    Cancel {
        request_id: RequestId,
    },

    /// Liveness probe. A black-holed TCP connection is invisible to the
    /// WebSocket layer, and undetected disconnects are the exact problem this
    /// protocol exists to fix, so the check is explicit.
    Ping {
        nonce: u64,
    },

    /// Ask the tap to prove it can still synthesize, without involving a sink.
    ///
    /// `Ping` proves the application loop is alive and nothing more. A tap
    /// whose script engine, cache or encoder has wedged answers `Pong` happily
    /// and then fails every request, which is exactly the state a retry cannot
    /// see and a listener experiences as silence. So the hub has to ask.
    ///
    /// The tap is expected to synthesize a short fixed phrase, report how long
    /// the first sample took, and throw the audio away. It must not send
    /// anything over the network: there is no sink and no `deliver_to`.
    ///
    /// `probe_id` is only a handle for the answer, in the same spirit as
    /// `Ping`'s nonce.
    Probe {
        probe_id: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum TapToHubMessage {
    ClientHello(TapClientHello),

    Response(Response),

    /// How a transfer ended. The hub never sees the audio, so without this it
    /// knows only what it dispatched, not what was delivered.
    StreamOutcome {
        request_id: RequestId,
        outcome: StreamOutcome,
    },

    Pong {
        nonce: u64,
    },

    /// The answer to [`HubToTapMessage::Probe`].
    ProbeResult {
        probe_id: u64,
        result: ProbeResult,
    },
}

/// What a tap found when it tried to prove it can synthesize.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum ProbeResult {
    /// It synthesized; this is how long the first sample took.
    Ready { time_to_first_sample_ms: u64 },

    /// It tried and could not. The hub deprioritises the connection.
    Failed { reason: String },

    /// It does not implement the probe.
    ///
    /// Deliberately not a failure. A tap that has no opinion must be left alone
    /// rather than taken out of service — there is nothing to route to instead.
    Unsupported,
}
