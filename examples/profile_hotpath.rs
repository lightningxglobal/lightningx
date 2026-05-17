use matching_engine::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    
    // Prime the engine with a few buy orders
    for i in 0..5 {
        let order = Order::new(
            i,
            Side::Buy,
            50000.0 - i as f64,
            10.0,
            TimeInForce::GTC,
            0,
        );
        engine.place_order(order)?;
    }
    
    // Now measure sell orders that will match
    let iterations = 10000;
    let start = Instant::now();
    for i in 0..iterations {
        let order = Order::new(
            1000 + i as u64,
            Side::Sell,
            50000.0 - (i % 5) as f64,
            1.0,
            TimeInForce::IOC,
            0,
        );
        let _result = engine.place_order(order)?;
    }
    let elapsed = start.elapsed();
    
    let tps = iterations as f64 / elapsed.as_secs_f64();
    let ns_per_order = elapsed.as_nanos() as f64 / iterations as f64;
    
    println!("Iterations: {}", iterations);
    println!("Total time: {:?}", elapsed);
    println!("TPS: {:.2}M", tps / 1_000_000.0);
    println!("ns/order: {:.0}", ns_per_order);
    
    Ok(())
}
