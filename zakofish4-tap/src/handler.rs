use async_trait::async_trait;
use zakofish4_common::messages::{
    AudioMetadataSuccessMessage, AudioRequestSuccessMessage, ProbeResult,
};

use crate::error::TapError;
use crate::source::AudioSource;
use crate::stream::AudioStreamSender;

/// What a tap implements.
///
/// Intentionally the same shape as the protofish3-era SDK, so migrating an
/// existing tap is a dependency swap and a change of hub URL. Everything that
/// actually changed — the audio now leaving over UDP to a third party rather
/// than back up the hub connection — is hidden behind
/// [`AudioStreamSender`].
#[async_trait]
pub trait TapHandler: Send + Sync + 'static {
    /// Return metadata for a source, without fetching any audio.
    async fn handle_audio_metadata_request(
        &self,
        source: AudioSource,
    ) -> Result<AudioMetadataSuccessMessage, TapError>;

    /// Begin streaming audio.
    ///
    /// Return the success message as soon as duration and metadata are known —
    /// the hub is waiting on it, and a listener is waiting on the hub. Push
    /// Opus frames through `stream` from a spawned task; dropping it ends the
    /// transfer cleanly.
    async fn handle_audio_request(
        &self,
        source: AudioSource,
        stream: AudioStreamSender,
    ) -> Result<AudioRequestSuccessMessage, TapError>;

    /// Prove this tap can still synthesize, without a sink and without the
    /// network.
    ///
    /// The hub asks because a tap that keeps its control connection alive while
    /// its synthesis pipeline has wedged is invisible: it answers heartbeats,
    /// it answers `AudioRequestSuccess`, and the listener hears nothing. The
    /// same pipeline the real requests take should be exercised — script,
    /// cache, encoder — on a short fixed phrase, and the result thrown away.
    /// Nothing must be sent anywhere: there is no `deliver_to` and no sink.
    ///
    /// The default reports [`ProbeResult::Unsupported`], which the hub records
    /// as "no opinion" and routes exactly as before. That is deliberately the
    /// safe answer for a tap that has not implemented this yet — inheriting it
    /// must not cost the tap its traffic.
    async fn probe(&self) -> ProbeResult {
        ProbeResult::Unsupported
    }
}
