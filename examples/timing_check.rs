use std::time::Instant;
use std::sync::OnceLock;

fn current_time_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(|| Instant::now());
    start.elapsed().as_nanos() as u64
}

fn main() {
    println!("Testing timing in synthetic benchmark loop:\n");

    let test_start = Instant::now();
    let mut count = 0;
    let mut max_time = 0u64;
    let mut sample_times = vec![];

    // Simulate fast loop
    while test_start.elapsed().as_secs_f64() < 0.5 {
        // Simulate 100 "orders" per iteration
        for _ in 0..100 {
            let now = current_time_ns();
            max_time = max_time.max(now);
            count += 1;

            // Check sampling condition every 10M iterations
            if count % 10_000_000 == 0 {
                sample_times.push((count, now));
                println!("Iteration {}: current_time_ns()={}", count, now);
            }
        }
    }

    let test_elapsed = test_start.elapsed().as_secs_f64();
    println!("\n【Test Results】");
    println!("Test wall-clock time: {:.4} seconds", test_elapsed);
    println!("Total iterations: {}", count);
    println!("Max time seen: {} ns = {:.6} ms", max_time, max_time as f64 / 1_000_000.0);
    println!("Operations/second: {:.2}M", count as f64 / test_elapsed / 1_000_000.0);
    
    // Show sampling timeline
    println!("\nSampling timeline:");
    for (iter, t) in sample_times {
        println!("  Iter {}: {}ns ({:.4}ms)", iter, t, t as f64 / 1_000_000.0);
    }
}
