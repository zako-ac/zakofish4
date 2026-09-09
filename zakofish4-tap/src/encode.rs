//! Turning an arbitrary audio file into the Opus frames a tap must send.
//!
//! Most taps hold a whole encoded file — an MP3 from a TTS API, a WebM from
//! yt-dlp — and need it as 20 ms Opus frames. This pipes it through ffmpeg and
//! forwards each packet, which is the same helper the protofish3-era SDK
//! carried, so a tap migrating to Zako4 changes its imports and nothing else.
//!
//! Requires `ffmpeg` on `PATH`.

use crate::stream::AudioStreamSender;

/// Pipe an encoded audio file through ffmpeg and stream the Opus frames out.
///
/// Push frames as they are produced rather than collecting them first: the
/// sender paces to roughly realtime on purpose, and `send_opus_frame` waiting
/// is that pacing working.
///
/// # Example
/// ```ignore
/// let bytes = reqwest::get(url).await?.bytes().await?;
/// let cursor = std::io::Cursor::new(bytes.to_vec());
/// decode_and_stream(cursor, stream).await?;
/// ```
pub async fn decode_and_stream(
    reader: std::io::Cursor<Vec<u8>>,
    stream: AudioStreamSender,
) -> Result<(), EncodeError> {
    use std::process::Stdio;
    use tokio::io::AsyncWriteExt;
    use tokio::process::Command;
    use tokio_stream::StreamExt;

    let mut ffmpeg = Command::new("ffmpeg")
        .args([
            "-v", "quiet", "-i", "pipe:0", "-vn", "-c:a", "libopus",
            // Pinned rather than left to the default, because the frame index
            // below is turned into a timestamp by multiplying by 20 ms. Under
            // Zako4 that timestamp is the receiver's jitter-buffer key rather
            // than an opaque prefix, so a different frame duration would not be
            // a harmless mismatch — it would be audible.
            "-frame_duration", "20",
            "-f", "ogg", "pipe:1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(EncodeError::Spawn)?;

    let mut ffmpeg_in = ffmpeg.stdin.take().expect("stdin was piped");
    let ffmpeg_out = ffmpeg.stdout.take().expect("stdout was piped");

    // Fed from its own task: ffmpeg will not drain stdin while we are not
    // reading stdout, so writing inline would deadlock on anything larger than
    // a pipe buffer.
    let data = reader.into_inner();
    tokio::spawn(async move {
        ffmpeg_in.write_all(&data).await.ok();
    });

    let mut packets = ogg::reading::async_api::PacketReader::new(ffmpeg_out);
    let mut frame_index = 0u64;

    while let Some(result) = packets.next().await {
        match result {
            Ok(packet) => {
                // Header packets, not audio: counting them would shift every
                // timestamp that follows.
                if packet.data.starts_with(b"OpusHead") || packet.data.starts_with(b"OpusTags") {
                    continue;
                }
                let data = bytes::Bytes::copy_from_slice(&packet.data);
                if !stream.send_opus_frame(frame_index, data).await {
                    // The sink is gone. Nothing to report — the runtime already
                    // knows, and it owns telling the hub.
                    break;
                }
                frame_index += 1;
            }
            Err(e) => {
                tracing::warn!(%e, "ogg packet read error");
                break;
            }
        }
    }

    // Dropping the child kills ffmpeg, which is what should happen when the
    // sink went away mid-track.
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("failed to spawn ffmpeg: {0}")]
    Spawn(#[from] std::io::Error),
}
