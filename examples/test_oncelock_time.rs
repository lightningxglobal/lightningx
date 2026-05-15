use std::time::Instant;
use std::sync::OnceLock;

fn current_time_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    let start = START.get_or_init(|| Instant::now());
    start.elapsed().as_nanos() as u64
}

fn main() {
    println!("Testing OnceLock-based time source:");
    for i in 0..10 {
        let ns = current_time_ns();
        println!("  Call {}: {} ns ({:.4} ms)", i + 1, ns, ns as f64 / 1_000_000.0);
    }
    
    println!("\nDirect Instant test:");
    let start = Instant::now();
    for i in 0..10 {
        let ns = start.elapsed().as_nanos() as u64;
        println!("  Call {}: {} ns ({:.4} ms)", i + 1, ns, ns as f64 / 1_000_000.0);
    }
}
