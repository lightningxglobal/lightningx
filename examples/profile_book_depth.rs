use matching_engine::*;
use std::time::Instant;
use hdrhistogram::Histogram;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    
    // Test at different book depths
    for depth in &[1, 5, 10, 20, 50, 100, 200] {
        let mut engine = MatchingEngine::new(PoolConfig::default())?;
        let (tx, _rx) = rtrb::RingBuffer::<TradeEvent>::new(100_000);
        engine.set_trade_event_sender(tx);
        
        // Fill buy book to target depth
        for i in 0..*depth {
            let order = Order::new(
                i as u64,
                Side::Buy,
                50000.0 - i as f64,
                10.0,
                TimeInForce::GTC,
                0,
            );
            engine.place_order(order)?;
        }
        
        // Now measure sell orders that match at different points
        let iterations = 1000;
        let mut histogram = Histogram::<u64>::new(3)?;
        
        for i in 0..iterations {
            let order = Order::new(
                10000 + i,
                Side::Sell,
                50000.0 - (i % depth) as f64,
                1.0,
                TimeInForce::IOC,
                0,
            );
            
            let start = Instant::now();
            engine.place_order(order)?;
            histogram.record(start.elapsed().as_nanos() as u64)?;
        }
        
        println!("Book depth: {} | P50: {} ns | P95: {} ns | P99: {} ns | Max: {} ns",
                 depth,
                 histogram.value_at_percentile(50.0),
                 histogram.value_at_percentile(95.0),
                 histogram.value_at_percentile(99.0),
                 histogram.max());
    }
    
    Ok(())
}
