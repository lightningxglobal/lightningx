//! Decode-path robustness tests — the in-CI layer of the fuzzing plan.
//!
//! Every wire decoder must tolerate hostile input: random garbage,
//! truncations at every length, and bit-flips of valid frames must return
//! None/Err — never panic, never read out of bounds. This runs on every
//! `cargo test`; the cargo-fuzz harness (nightly, coverage-guided) reuses
//! the same entry-point list for longer offline campaigns.
//!
//! Deterministic: seeded LCG, a failure reproduces exactly.

use lightning_exchange::sbe;
use lightning_exchange::transport::persist_event::{
    AccountSetPayload, PersistFrame, pack_str,
};
use lightning_exchange::ws_sbe;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn fill(&mut self, buf: &mut [u8]) {
        for b in buf.iter_mut() {
            *b = self.next() as u8;
        }
    }
}

/// Run every decoder against one buffer (shared with the cargo-fuzz
/// harness — single source of truth in the library).
fn exercise_all_decoders(buf: &[u8]) {
    lightning_exchange::transport::fuzz_exercise_decoders(buf);
}

#[test]
fn decoders_survive_random_garbage() {
    let mut rng = Lcg(0xDEAD_BEEF_0BAD_F00D);
    let mut buf = vec![0u8; 1024];
    for _ in 0..2_000 {
        let len = (rng.next() as usize) % buf.len();
        rng.fill(&mut buf[..len]);
        exercise_all_decoders(&buf[..len]);
    }
}

#[test]
fn decoders_survive_every_truncation_of_valid_frames() {
    // A valid encoded NewOrder frame, truncated at every possible length.
    let req = sbe::NewOrderRequest {
        client_order_id: 1,
        participant_id: 2,
        price_ticks: 10_050,
        quantity_lots: 1_000_000,
        side: 0,
        time_in_force: 0,
        response_stream_id: 200,
        reduce_only: 0,
        _pad: [0; 9],
        symbol: *b"BTC_USDT\0\0\0\0\0\0\0\0",
    };
    let mut frame = [0u8; 128];
    let n = sbe::encode_new_order(&req, &mut frame).expect("encode");
    for cut in 0..=n {
        exercise_all_decoders(&frame[..cut]);
    }
    // Truncating below the declared size must yield None, not a partial read.
    assert!(sbe::decode_new_order(&frame[..n - 1]).is_none());
    assert!(sbe::decode_new_order(&frame[..8]).is_none());
    assert!(sbe::decode_new_order(&[]).is_none());

    // Same for a persist frame.
    let pf = PersistFrame::account_set(AccountSetPayload {
        user_id: 1,
        asset: pack_str("USDT"),
        balance: 1.0,
        frozen: 0.0,
        balance_atoms: 100_000_000,
        frozen_atoms: 0,
    });
    let bytes = pf.as_bytes();
    for cut in 0..bytes.len() {
        assert!(
            PersistFrame::from_bytes(&bytes[..cut]).is_none(),
            "truncated persist frame at {cut} must not parse"
        );
        exercise_all_decoders(&bytes[..cut]);
    }
    assert!(PersistFrame::from_bytes(bytes).is_some());
}

#[test]
fn decoders_survive_bitflips_of_valid_frames() {
    let req = sbe::NewOrderRequest {
        client_order_id: 7,
        participant_id: 8,
        price_ticks: 5_000_000,
        quantity_lots: 123,
        side: 1,
        time_in_force: 2,
        response_stream_id: 201,
        reduce_only: 0,
        _pad: [0; 9],
        symbol: *b"ETH_USDT\0\0\0\0\0\0\0\0",
    };
    let mut frame = [0u8; 128];
    let n = sbe::encode_new_order(&req, &mut frame).expect("encode");
    let original = frame;
    // Flip every bit of the frame one at a time (header corruption included).
    for byte in 0..n {
        for bit in 0..8 {
            frame = original;
            frame[byte] ^= 1 << bit;
            exercise_all_decoders(&frame[..n]);
        }
    }
}

#[test]
fn rejected_reason_with_invalid_utf8_does_not_panic() {
    // decode_order_rejected extracts a String from attacker-controlled
    // bytes; invalid UTF-8 must be handled, not unwrapped.
    let valid = ws_sbe::encode_order_rejected(42, "reason");
    let mut corrupted = valid.clone();
    // Stomp the tail (where the reason text lives) with invalid UTF-8.
    let len = corrupted.len();
    for b in corrupted[len.saturating_sub(6)..].iter_mut() {
        *b = 0xFF;
    }
    let _ = ws_sbe::decode_order_rejected(&corrupted);
    let _ = ws_sbe::decode_order_rejected(&valid);
}
