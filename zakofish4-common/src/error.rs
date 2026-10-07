#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum HubError {
    #[error("Invalid message: {0}")]
    InvalidMessage(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Unauthorized")]
    Unauthorized,

    #[error("Tap speaks protocol version {got}, hub serves {supported}")]
    UnsupportedVersion { got: u32, supported: u32 },
}
