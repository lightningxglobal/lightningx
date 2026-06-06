//! Coverage-guided fuzzing over every wire decoder. The entry-point list
//! lives in the library (transport::fuzz_exercise_decoders) and is shared
//! with the deterministic in-CI robustness suite.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    lightning_exchange::transport::fuzz_exercise_decoders(data);
});
