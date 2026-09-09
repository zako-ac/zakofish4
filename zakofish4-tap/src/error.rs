/// A failure a tap reports for one request.
///
/// The two variants map onto the hub's `try_others` flag, so implementors never
/// touch the wire representation — they just say whether trying a different tap
/// could plausibly help.
#[derive(Debug, thiserror::Error)]
pub enum TapError {
    /// Transient. The hub may try another tap: network trouble, a rate limit,
    /// a timeout, a subprocess that died.
    #[error("{0}")]
    Retriable(String),

    /// Permanent. No other tap will do better: an unsupported URL scheme, a
    /// video that does not exist, age-restricted content.
    #[error("{0}")]
    Permanent(String),
}

impl TapError {
    pub(crate) fn try_others(&self) -> bool {
        matches!(self, TapError::Retriable(_))
    }

    pub(crate) fn reason(self) -> String {
        match self {
            TapError::Retriable(r) | TapError::Permanent(r) => r,
        }
    }
}

/// Top-level failure from [`crate::TapBuilder::run`].
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    #[error("hub rejected this tap: {0}")]
    Rejected(String),

    #[error("hub speaks a protocol this SDK does not: {0}")]
    UnsupportedByHub(String),

    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("codec error: {0}")]
    Codec(#[from] zakofish4_common::codec::CodecError),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("configuration is incomplete: {0}")]
    Config(&'static str),
}
