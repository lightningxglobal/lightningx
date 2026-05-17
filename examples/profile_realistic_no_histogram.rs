use matching_engine::*;
use std::time::Instant;
use rtrb::RingBuffer;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_price = 50000.0;
    let num_batches = 5000;
    
    let mut engine = MatchingEngine::new(PoolConfig::default())?;
    let (tx, mut rx) = RingBuffer::<TradeEvent>::new(1_000_000);
    engine.set_trade_event_sender(tx);
    
    let total_start = Instant::now();
    
    for round in 0..num_batches {
        // 5个买单
        for i in 0..5 {
            let price = base_price - 2.0 + i as f64;
            let qty = 10.0;
            let order = Order::new(round as u64 * 1000 + i as u64, Side::Buy, price, qty, TimeInForce::GTC, 0);
            engine.place_order(order)?;
        }
        
        // 5个卖单
        for i in 0..5 {
            let price = base_price + i as f64;
            let qty = 10.0;
            let order = Order::new(round as u64 * 1000 + 5 + i as u64, Side::Sell, price, qty, TimeInForce::IOC, 0);
            engine.place_order(order)?;
        }
        
        // Drain RingBuffer to prevent backpressure
        while let Ok(_) = rx.pop() {}
    }
    
    let elapsed = total_start.elapsed();
    let total_orders = num_batches * 10;
    let tps = total_orders as f64 / elapsed.as_secs_f64();
    
    println!("Realistic pattern WITHOUT Histogram:");
    println!("Total orders: {}", total_orders);
    println!("Total time: {:.3}s", elapsed.as_secs_f64());
    println!("TPS: {:.2}M", tps / 1_000_000.0);
    println!("ns/order: {:.0}", elapsed.as_nanos() as f64 / total_orders as f64);
    
    Ok(())
}
