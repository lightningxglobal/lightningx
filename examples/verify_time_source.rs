use std::time::Instant;

fn main() {
    #[cfg(target_os = "linux")]
    {
        let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
        let ret = unsafe {
            libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts as *mut libc::timespec)
        };
        println!("CLOCK_MONOTONIC test (target_os=linux):");
        if ret == 0 {
            let val = (ts.tv_sec as u64).saturating_mul(1_000_000_000)
                .saturating_add(ts.tv_nsec as u64);
            println!("  Return code: {} (success)", ret);
            println!("  tv_sec: {}, tv_nsec: {}", ts.tv_sec, ts.tv_nsec);
            println!("  Total ns: {}", val);
            
            // Read a few more times
            println!("\n  Sequential reads:");
            for i in 0..5 {
                let mut ts2: libc::timespec = unsafe { std::mem::zeroed() };
                let _ = unsafe {
                    libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts2 as *mut libc::timespec)
                };
                let val2 = (ts2.tv_sec as u64).saturating_mul(1_000_000_000)
                    .saturating_add(ts2.tv_nsec as u64);
                println!("    Read {}: {}", i + 1, val2);
            }
        } else {
            println!("  Return code: {} (failure!)", ret);
        }
    }
    
    #[cfg(not(target_os = "linux"))]
    {
        println!("Not on Linux, using Instant");
    }
    
    // Also test Instant
    println!("\nInstant.elapsed() test:");
    let start = Instant::now();
    for i in 0..5 {
        let e = start.elapsed().as_nanos() as u64;
        println!("  Read {}: {} ns", i + 1, e);
    }
}
