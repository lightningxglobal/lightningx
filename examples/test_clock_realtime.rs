fn current_time_ns() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
        let ret = unsafe {
            libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts as *mut libc::timespec)
        };
        if ret == 0 {
            (ts.tv_sec as u64).saturating_mul(1_000_000_000)
                .saturating_add(ts.tv_nsec as u64)
        } else {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

fn main() {
    println!("Testing CLOCK_REALTIME advancement:\n");
    
    let start = std::time::Instant::now();
    let mut last = current_time_ns();
    let mut count = 0;
    let mut advances = 0;
    
    // Spin for 0.5 seconds
    while start.elapsed().as_secs_f64() < 0.5 {
        let now = current_time_ns();
        count += 1;
        if now != last {
            advances += 1;
            if advances <= 10 || advances % 100 == 0 {
                println!("Advance #{}: +{} ns", advances, now - last);
            }
            last = now;
        }
    }
    
    println!("\nIn 0.5 seconds:");
    println!("  Loop iterations: {}", count);
    println!("  Time advances: {}", advances);
    println!("  Advancement ratio: {:.2}%", (advances as f64 / count as f64) * 100.0);
    
    // Test sampling logic
    println!("\n\nSimulating 100ms sampling for 1 second:\n");
    let interval_ns = 100_000_000u64; // 100ms
    let mut last_sample = current_time_ns();
    let mut sample_count = 0;
    
    let test_start = std::time::Instant::now();
    let mut loop_count = 0;
    while test_start.elapsed().as_secs_f64() < 1.0 {
        let now = current_time_ns();
        loop_count += 1;
        
        if now >= last_sample + interval_ns {
            sample_count += 1;
            if sample_count <= 5 || sample_count % 2 == 0 {
                println!("Sample #{}: elapsed={:.3}ms", sample_count, 
                         (now - (last_sample - interval_ns)) as f64 / 1_000_000.0);
            }
            last_sample = now;
        }
    }
    
    println!("\nIn 1 second:");
    println!("  Loop iterations: {}", loop_count);
    println!("  Samples: {} (expected ~10)", sample_count);
}
