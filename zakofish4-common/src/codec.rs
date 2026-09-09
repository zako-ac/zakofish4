//! MessagePack framing for the gateway WebSocket.
//!
//! Both ends encode through here so the wire format has exactly one
//! definition. zako3 learned this the hard way with protofish3: the 8-byte
//! timestamp prefix ended up hand-rolled in two separate crates, and they had
//! to be kept in step by hand.
//!
//! Structs are encoded as maps rather than positional arrays, so adding an
//! optional field does not break a tap built against an older schema — which
//! matters when the taps belong to other people.

use serde::{Serialize, de::DeserializeOwned};

use crate::messages::{HubToTapMessage, TapToHubMessage};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("encode failed: {0}")]
    Encode(#[from] rmp_serde::encode::Error),
    #[error("decode failed: {0}")]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("message of {got} bytes exceeds the {max}-byte limit")]
    TooLarge { got: usize, max: usize },
}

/// Ceiling on one control frame.
///
/// Control messages are small; anything approaching this is a bug or an
/// attempt to exhaust memory, and either way it should be refused before it is
/// parsed.
pub const MAX_FRAME_BYTES: usize = 256 * 1024;

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    let mut buf = Vec::new();
    let mut ser = rmp_serde::Serializer::new(&mut buf).with_struct_map();
    value.serialize(&mut ser)?;
    if buf.len() > MAX_FRAME_BYTES {
        return Err(CodecError::TooLarge { got: buf.len(), max: MAX_FRAME_BYTES });
    }
    Ok(buf)
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(CodecError::TooLarge { got: bytes.len(), max: MAX_FRAME_BYTES });
    }
    Ok(rmp_serde::from_slice(bytes)?)
}

pub fn encode_to_tap(msg: &HubToTapMessage) -> Result<Vec<u8>, CodecError> {
    encode(msg)
}

pub fn decode_from_tap(bytes: &[u8]) -> Result<TapToHubMessage, CodecError> {
    decode(bytes)
}

pub fn encode_to_hub(msg: &TapToHubMessage) -> Result<Vec<u8>, CodecError> {
    encode(msg)
}

pub fn decode_from_hub(bytes: &[u8]) -> Result<HubToTapMessage, CodecError> {
    decode(bytes)
}
