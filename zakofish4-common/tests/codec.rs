//! Wire-format round trips, and the schema-evolution property third-party taps
//! depend on.

use zakofish4_common::codec::{self, MAX_FRAME_BYTES};
use zakofish4_common::messages::*;
use zakofish4_common::model::*;

fn audio_request() -> HubToTapMessage {
    HubToTapMessage::Request(Request {
        request_id: RequestId::random(),
        variant: RequestVariant::AudioRequest(AudioRequestMessage {
            ars: AudioRequestString("https://example.invalid/x".into()),
            discord_user_id: DiscordUserId("42".into()),
            encryption_key: EncryptionKey([3u8; 32]),
            deliver_to: vec!["1.2.3.4:5000".into(), "[2001:db8::1]:5000".into()],
            headers: [("traceparent".to_string(), "00-abc-def-01".to_string())]
                .into_iter()
                .collect(),
        }),
    })
}

#[test]
fn hub_to_tap_messages_round_trip() {
    let cases = vec![
        HubToTapMessage::Accept,
        HubToTapMessage::Reject(TapServerReject {
            reason_type: HubRejectReasonType::Unauthorized,
            reason: "nope".into(),
        }),
        audio_request(),
        HubToTapMessage::Cancel { request_id: RequestId::random() },
        HubToTapMessage::Ping { nonce: 0xDEAD_BEEF },
    ];

    for msg in cases {
        let bytes = codec::encode_to_tap(&msg).expect("encode");
        let back = codec::decode_from_hub(&bytes).expect("decode");
        assert_eq!(format!("{msg:?}"), format!("{back:?}"));
    }
}

#[test]
fn tap_to_hub_messages_round_trip() {
    let cases = vec![
        TapToHubMessage::ClientHello(TapClientHello {
            protocol_version: PROTOCOL_VERSION,
            tap_id: TapId("tap-1".into()),
            friendly_name: "Tap".into(),
            api_token: "zk_x".into(),
            selection_weight: 1.5,
            resuming: vec![RequestId::random()],
        }),
        TapToHubMessage::Response(Response {
            request_id: RequestId::random(),
            variant: ResponseVariant::AudioMetadataFailure(AudioMetadataFailureMessage {
                reason: "age restricted".into(),
                try_others: false,
            }),
        }),
        TapToHubMessage::StreamOutcome {
            request_id: RequestId::random(),
            outcome: StreamOutcome::Undeliverable { reason: "no route".into() },
        },
        TapToHubMessage::Pong { nonce: 7 },
    ];

    for msg in cases {
        let bytes = codec::encode_to_hub(&msg).expect("encode");
        let back = codec::decode_from_tap(&bytes).expect("decode");
        assert_eq!(format!("{msg:?}"), format!("{back:?}"));
    }
}

/// The key must survive the wire exactly — a mangled key means every datagram
/// fails to authenticate, which would look like a network fault rather than a
/// serialisation bug.
#[test]
fn the_encryption_key_survives_intact() {
    let key: [u8; 32] = core::array::from_fn(|i| (i * 7 + 1) as u8);
    let msg = HubToTapMessage::Request(Request {
        request_id: RequestId::random(),
        variant: RequestVariant::AudioRequest(AudioRequestMessage {
            ars: AudioRequestString("x".into()),
            discord_user_id: DiscordUserId("1".into()),
            encryption_key: EncryptionKey(key),
            deliver_to: vec!["h:1".into()],
            headers: Default::default(),
        }),
    });

    let bytes = codec::encode_to_tap(&msg).unwrap();
    let HubToTapMessage::Request(Request {
        variant: RequestVariant::AudioRequest(req),
        ..
    }) = codec::decode_from_hub(&bytes).unwrap()
    else {
        panic!("wrong variant");
    };
    assert_eq!(req.encryption_key.0, key);
}

/// A key must not be printable by accident. It travels through logs-adjacent
/// code on both sides, and `Debug` is the easiest way to leak one.
#[test]
fn the_encryption_key_is_redacted_in_debug_output() {
    let key = EncryptionKey([0xAB; 32]);
    let shown = format!("{key:?}");
    assert!(!shown.contains("171"), "{shown}");
    assert!(!shown.contains("ab"), "{shown}");
    assert!(shown.contains("redacted"));
}

/// Structs encode as maps, so a tap built against an older schema keeps working
/// when a field is added. Since taps belong to other people, this is the
/// property that makes the schema evolvable at all.
#[test]
fn unknown_fields_do_not_break_older_readers() {
    #[derive(serde::Serialize)]
    struct HelloPlusFutureField {
        protocol_version: u32,
        tap_id: TapId,
        friendly_name: String,
        api_token: String,
        selection_weight: f32,
        resuming: Vec<RequestId>,
        something_added_later: u64,
    }

    let future = HelloPlusFutureField {
        protocol_version: PROTOCOL_VERSION,
        tap_id: TapId("tap-1".into()),
        friendly_name: "Tap".into(),
        api_token: "zk_x".into(),
        selection_weight: 1.0,
        resuming: vec![],
        something_added_later: 99,
    };

    let bytes = codec::encode(&future).unwrap();
    let hello: TapClientHello = codec::decode(&bytes).expect("older reader must cope");
    assert_eq!(hello.tap_id.0, "tap-1");
}

/// `resuming` is `#[serde(default)]`, so a tap that predates it still connects.
#[test]
fn an_older_hello_without_resuming_still_decodes() {
    #[derive(serde::Serialize)]
    struct OldHello {
        protocol_version: u32,
        tap_id: TapId,
        friendly_name: String,
        api_token: String,
        selection_weight: f32,
    }

    let bytes = codec::encode(&OldHello {
        protocol_version: PROTOCOL_VERSION,
        tap_id: TapId("tap-1".into()),
        friendly_name: "Tap".into(),
        api_token: "zk_x".into(),
        selection_weight: 1.0,
    })
    .unwrap();

    let hello: TapClientHello = codec::decode(&bytes).expect("decode");
    assert!(hello.resuming.is_empty());
}

#[test]
fn oversized_frames_are_refused_before_parsing() {
    let huge = vec![0u8; MAX_FRAME_BYTES + 1];
    let err = codec::decode::<TapToHubMessage>(&huge).unwrap_err();
    assert!(matches!(err, codec::CodecError::TooLarge { .. }));
}

#[test]
fn garbage_is_rejected_rather_than_panicking() {
    let err = codec::decode_from_tap(&[0xC1, 0xFF, 0x00, 0x42]).unwrap_err();
    assert!(matches!(err, codec::CodecError::Decode(_)));
}
