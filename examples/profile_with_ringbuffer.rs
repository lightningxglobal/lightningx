use matching_engine::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = rtrb::RingBuffer::<TradeEvent>::new(100_000);
    engine.set_trade_event_sender(tx);  // WITH RingBuffer
    
    // Prime engine
    for i in 0..5 {
        engine.place_order(Order::new(i, Side::Buy, 50000.0 - i as f64, 10.0, TimeInForce::GTC, 0))?;
    }
    
    const ITERATIONS: usize = 10000;
    let start = Instant::now();
    
    for i in 0..ITERATIONS {
        let order = Order::new(
            1000 + i as u64,
            Side::Sell,
            50000.0 - (i % 5) as f64,
            1.0,
            TimeInForce::IOC,
            0,
        );
        engine.place_order(order)?;
    }
    
    let elapsed = start.elapsed();
    let tps = ITERATIONS as f64 / elapsed.as_secs_f64();
    let ns_per = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    
    println!("WITH RingBuffer:");
    println!("TPS: {:.2}M", tps / 1_000_000.0);
    println!("ns/order: {:.0}", ns_per);
    
    Ok(())
}
