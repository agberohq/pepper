//! Unit tests for the runtime crate: wire compatibility and blob handling.
//!
//! Run with `cargo test` inside `runtime/rust`. These cover the parts the Go
//! integration test cannot reach cheaply (decode edge cases, framing, blobs).

use crate::cap::{new_tensor, BlobRef};
use crate::envelope::{self, msg, unframe, Envelope};
use crate::stream::{Semaphore, StreamState};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[test]
fn stream_state_delivers_chunks_in_order_then_eof() {
    let state = StreamState::new();
    let producer = Arc::clone(&state);
    let handle = std::thread::spawn(move || {
        for i in 0..3 {
            producer.push(json!(i));
        }
        producer.close();
    });

    let mut seen = Vec::new();
    while let Some(chunk) = state.next(Some(Duration::from_secs(5))) {
        seen.push(chunk.as_i64().unwrap_or(-1));
    }
    handle.join().unwrap();
    assert_eq!(seen, vec![0, 1, 2], "chunks must arrive in order, then EOF");
}

#[test]
fn stream_state_returns_none_after_close_when_empty() {
    let state = StreamState::new();
    state.close();
    assert!(state.next(Some(Duration::from_millis(50))).is_none());
}

#[test]
fn semaphore_bounds_concurrent_holders() {
    let sem = Semaphore::new(2);
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..6 {
        let sem = Arc::clone(&sem);
        let active = Arc::clone(&active);
        let peak = Arc::clone(&peak);
        handles.push(std::thread::spawn(move || {
            let _permit = sem.acquire();
            let now = active.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            peak.fetch_max(now, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(20));
            active.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert!(
        peak.load(std::sync::atomic::Ordering::SeqCst) <= 2,
        "semaphore allowed more than 2 concurrent holders"
    );
}

#[test]
fn semaphore_does_not_delay_when_free() {
    let sem = Semaphore::new(4);
    let started = Instant::now();
    {
        let _a = sem.acquire();
        let _b = sem.acquire();
    }
    assert!(started.elapsed() < Duration::from_millis(100));
}


#[test]
fn envelope_round_trip_uses_named_fields() {
    let mut env = Envelope::new(msg::RES);
    env.worker_id = "w-1".into();
    env.origin_id = "origin-1".into();
    env.cap = "rust.echo".into();
    env.payload = Some(serde_bytes::ByteBuf::from(b"payload".to_vec()));

    let data = envelope::encode(&env).expect("encode");
    // A map, not a positional array: fixmap (0x80..=0x8f), map16 (0xde) or
    // map32 (0xdf). An array would start 0x9_, a struct-as-array would break the
    // Python/Julia/Node runtimes, which read fields by name.
    assert!(
        matches!(data[0], 0x80..=0x8f | 0xde | 0xdf),
        "envelope must encode as a map, got {:#x}",
        data[0]
    );

    let back = envelope::decode(&data).expect("decode");
    assert_eq!(back.msg_type, msg::RES);
    assert_eq!(back.worker_id, "w-1");
    assert_eq!(back.payload_bytes(), b"payload");
    assert_eq!(back.reply_topic(), "pepper.res.origin-1");
}

#[test]
fn nil_containers_decode_as_empty() {
    // Go sends nil for empty slices and maps; `serde(default)` alone would fail
    // because the field is present with a unit value.
    let value = json!({
        "proto_ver": 1,
        "msg_type": "cap_load",
        "cap": "rust.echo",
        "groups": null,
        "caps": null,
        "deps": null,
        "pipe_subs": null,
        "pipe_pubs": null,
        "meta": null,
        "config": null
    });
    let data = rmp_serde::to_vec_named(&value).expect("encode fixture");
    let env = envelope::decode(&data).expect("decode cap_load with nils");
    assert_eq!(env.cap, "rust.echo");
    assert!(env.groups.is_empty());
    assert!(env.caps.is_empty());
    assert!(env.meta.is_empty());
    assert!(env.config.is_empty());
}

#[test]
fn frames_carry_topic_and_payload() {
    let framed = envelope::frame("pepper.res.abc", b"body");
    let (topic, payload) = unframe(&framed);
    assert_eq!(topic, "pepper.res.abc");
    assert_eq!(payload, b"body");

    // A truncated frame must not panic.
    assert_eq!(unframe(&[0x00]).0, "");
    assert_eq!(unframe(&[0x00, 0x20, b'x']).0, "");
}

#[test]
fn heartbeat_and_error_topics() {
    let mut hb = Envelope::new(msg::HB_PING);
    hb.worker_id = "w-9".into();
    assert_eq!(hb.reply_topic(), "pepper.hb.w-9");

    let mut other = Envelope::new(msg::CAP_READY);
    other.worker_id = "w-9".into();
    assert_eq!(other.reply_topic(), "pepper.control");
}

#[test]
fn blobs_round_trip_through_the_blob_dir() {
    let dir = std::env::temp_dir().join(format!("pepper-rust-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("mkdir");
    std::env::set_var(crate::wire::env::BLOB_DIR, &dir);

    let samples: Vec<f32> = vec![1.0, 2.0, 3.5, -4.25];
    let bytes: Vec<u8> = samples.iter().flat_map(|f| f.to_le_bytes()).collect();
    let value = new_tensor(&bytes, "float32", &[2, 2]).expect("new_tensor");

    let blob = BlobRef::from_value(&value).expect("blob ref");
    assert_eq!(blob.dtype, "float32");
    assert_eq!(blob.shape, vec![2, 2]);
    assert_eq!(blob.numel(), 4);
    assert_eq!(blob.size, bytes.len() as u64);

    // Copied read and memory-mapped read must agree.
    assert_eq!(blob.as_bytes().expect("as_bytes"), bytes);
    let mapped = blob.as_f32().expect("as_f32");
    assert_eq!(&mapped[..], &samples[..]);

    let _ = std::fs::remove_file(&blob.path);
    let _ = std::fs::remove_dir(&dir);
}

#[test]
fn non_blob_value_is_not_a_blob() {
    assert!(BlobRef::from_value(&json!({"path": "/tmp/x"})).is_none());
    assert!(BlobRef::from_value(&json!({"_pepper_blob": false})).is_none());
    assert!(BlobRef::from_value(&json!("scalar")).is_none());
}
