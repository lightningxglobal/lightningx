use lightning_exchange::*;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, _rx) = rtrb::RingBuffer::<TradeEvent>::new(100_000);
    engine.set_trade_event_sender(tx);
    
    // Fill pool near capacity
    let pool_capacity = 10000; // PoolConfig default
    
    for fill_pct in &[10, 50, 90, 99] {
        let mut engine = MatchingEngine::new(PoolConfig::default())?;
        let (tx, _rx) = rtrb::RingBuffer::<TradeEvent>::new(100_000);
        engine.set_trade_event_sender(tx);
        
        let fill_count = (pool_capacity * fill_pct) / 100;
        
        // Fill pool by adding orders that don't match
        for i in 0..fill_count {
            let order = Order::new(i, Side::Buy, 50000.0 + i as f64, 1.0, TimeInForce::GTC, 0);
            engine.place_order(order)?;
        }
        
        // Now measure sell orders (that will match and release pool space)
        let mut slow_count = 0;
        let iterations = 100;
        let threshold_ns = 500u64; // 500ns threshold
        
        for i in 0..iterations {
            let order = Order::new(
                10000 + i,
                Side::Sell,
                50000.0,
                1.0,
                TimeInForce::IOC,
                0,
            );
            
            let start = Instant::now();
            engine.place_order(order)?;
            let elapsed = start.elapsed().as_nanos() as u64;
            if elapsed > threshold_ns {
                slow_count += 1;
            }
        }
        
        println!("Pool fill {}% | Slow ops (>500ns): {}/{}", fill_pct, slow_count, iterations);
    }
    
    Ok(())
}
