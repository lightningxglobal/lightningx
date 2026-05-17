use matching_engine::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let mut total_time = 0u128;
    let mut order_creation_time = 0u128;
    let mut place_order_time = 0u128;
    
    const ITERATIONS: usize = 10000;
    
    // Prime engine
    for i in 0..5 {
        engine.place_order(Order::new(i, Side::Buy, 50000.0 - i as f64, 10.0, TimeInForce::GTC, 0))?;
    }
    
    // Profile with breakdown
    for i in 0..ITERATIONS {
        let iter_start = Instant::now();
        
        let create_start = Instant::now();
        let order = Order::new(
            1000 + i as u64,
            Side::Sell,
            50000.0 - (i % 5) as f64,
            1.0,
            TimeInForce::IOC,
            0,
        );
        order_creation_time += create_start.elapsed().as_nanos();
        
        let po_start = Instant::now();
        engine.place_order(order)?;
        place_order_time += po_start.elapsed().as_nanos();
        
        total_time += iter_start.elapsed().as_nanos();
    }
    
    println!("Iterations: {}", ITERATIONS);
    println!("Total time: {:.2}us", total_time as f64 / 1000.0);
    println!("ns/iteration: {:.0}", total_time as f64 / ITERATIONS as f64);
    println!();
    println!("Order::new() time: {:.2}us ({:.1}%)", order_creation_time as f64 / 1000.0, 
             order_creation_time as f64 / total_time as f64 * 100.0);
    println!("place_order() time: {:.2}us ({:.1}%)", place_order_time as f64 / 1000.0,
             place_order_time as f64 / total_time as f64 * 100.0);
    println!("Other overhead: {:.1}%", 
             (100.0 - (order_creation_time + place_order_time) as f64 / total_time as f64 * 100.0));
    
    let tps = ITERATIONS as f64 / (total_time as f64 / 1_000_000_000.0);
    println!();
    println!("Total TPS: {:.2}M", tps / 1_000_000.0);
    
    Ok(())
}
