use zakofish4_common::model::{AudioRequestString, DiscordUserId};

/// What was asked for, and by whom.
#[derive(Debug, Clone)]
pub struct AudioSource {
    inner: String,
    requester: DiscordUserId,
}

impl AudioSource {
    pub(crate) fn new(ars: AudioRequestString, requester: DiscordUserId) -> Self {
        Self { inner: ars.0, requester }
    }

    pub fn as_str(&self) -> &str {
        &self.inner
    }

    /// The Discord user this request is for. Useful for per-user rate limiting
    /// inside a tap; the hub has already checked permissions.
    pub fn requester(&self) -> &DiscordUserId {
        &self.requester
    }
}

impl std::fmt::Display for AudioSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.inner)
    }
}
