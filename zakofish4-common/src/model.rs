use serde::{Deserialize, Serialize};

pub use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HubRejectReasonType {
    #[serde(rename = "unauthorized")]
    Unauthorized,

    #[serde(rename = "internal_error")]
    InternalError,

    /// The tap speaks a protocol version the hub cannot serve.
    #[serde(rename = "unsupported_version")]
    UnsupportedVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TapId(pub String);

/// Identifies one audio request end to end: over this WebSocket, in the UDP
/// datagrams that carry the audio, and in `ae_proxy`'s routing table.
///
/// A v4 UUID rather than a counter for three reasons. It is minted by the sink
/// — an audio engine or the cache worker — so several independent processes
/// mint into the same space and must not collide. It travels in the clear on
/// every UDP datagram as the proxy's routing *and* source-pinning key, so it
/// must not be guessable: 32 bits are sprayable and an attacker only needs to
/// hit *any* live request. And it is the only correlation handle the UDP path
/// has, since that path carries no trace context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RequestId(pub Uuid);

impl RequestId {
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// The shared secret protecting one request's UDP datagrams.
///
/// Minted by the sink, handed to HQ over the internal RPC, and forwarded to the
/// tap over this WebSocket. It authenticates every datagram in both directions;
/// its job is anti-injection, not secrecy.
///
/// Never log this. `Debug` is redacted deliberately, and serialises as bytes
/// rather than a string so there is no convenient way to print it.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionKey(pub [u8; 32]);

impl std::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EncryptionKey(<redacted>)")
    }
}

impl std::fmt::Display for TapId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for TapId {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl From<String> for TapId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Discord user on whose behalf a request is made. The hub needs it to check
/// tap permissions, so it has to be on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiscordUserId(pub String);

impl std::fmt::Display for DiscordUserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AudioRequestString(pub String);

impl std::fmt::Display for AudioRequestString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for AudioRequestString {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl From<String> for AudioRequestString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AudioCacheType {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "ar_hash")]
    ARHash,
    #[serde(rename = "key")]
    CacheKey(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioCachePolicy {
    pub cache_type: AudioCacheType,
    pub ttl_seconds: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case", content = "value")]
pub enum AudioMetadata {
    Title(String),
    Description(String),
    Artist(String),
    Album(String),
    ImageUrl(String),
    Url(String),
}

/// Where a tap should send the audio.
///
/// A list rather than a single address, in preference order. During the IP
/// transit this carries the shared proxy first; afterwards it carries the
/// sink's own address, and it can carry both during the cutover so a tap falls
/// through on its own. Making this a list later would be a breaking change for
/// every third-party tap, so it is a list from the start.
///
/// Entries are authority strings — `"1.2.3.4:5000"`, `"[2001:db8::1]:5000"`,
/// `"sink.example.com:5000"` — resolved by the tap.
pub type DeliverTo = Vec<String>;

/// How a tap's transfer ended, reported back so the hub can see something it
/// otherwise never observes: it dispatches requests but never touches the audio.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "snake_case")]
pub enum StreamOutcome {
    /// Every frame was sent and the sink confirmed the tail.
    Completed { frames_sent: u64 },
    /// The tap stopped early.
    Aborted { frames_sent: u64, reason: String },
    /// The tap could not reach the sink at all, or nothing ever acknowledged
    /// it. Distinguished from `Aborted` because it points at the network path
    /// rather than at the tap.
    Undeliverable { reason: String },
}
