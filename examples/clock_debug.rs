/// 调试时钟实现

fn current_time_ns() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let mut ts: libc::timespec = unsafe { std::mem::zeroed() };
        unsafe {
            libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts as *mut libc::timespec);
        }
        (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
    }

    #[cfg(not(target_os = "linux"))]
    {
        use std::time::Instant;
        static INIT: std::sync::Once = std::sync::Once::new();
        static mut BASE: u64 = 0;

        unsafe {
            INIT.call_once(|| {
                BASE = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
            });

            BASE + Instant::now().elapsed().as_nanos() as u64
        }
    }
}

fn main() {
    println!("测试时钟实现...\n");

    // 测试1: 连续读取
    println!("【连续读取测试】");
    for i in 0..10 {
        let t = current_time_ns();
        println!("  读取 {}: {} ns", i, t);
    }

    // 测试2: 时间间隔
    println!("\n【时间推进测试】");
    let mut last = current_time_ns();
    for i in 0..10 {
        let now = current_time_ns();
        let diff = now.saturating_sub(last);
        println!("  间隔 {}: {} ns ({}μs)", i, diff, diff / 1000);
        last = now;
    }

    // 测试3: 检查采样逻辑
    println!("\n【采样逻辑模拟】");
    let interval_ns = 1_000_000; // 1ms
    let mut last_sample = 0u64;
    let mut sample_count = 0;

    for i in 0..100 {
        let now = current_time_ns();

        if i < 5 || i % 20 == 0 {
            println!("  订单 {}: now={} last_sample={} diff={}",
                     i, now, last_sample, now.saturating_sub(last_sample));
        }

        if now >= last_sample + interval_ns {
            sample_count += 1;
            if i < 5 || i % 20 == 0 {
                println!("    ✓ 采样触发! 累计: {}", sample_count);
            }
            last_sample = now;
        }
    }

    println!("\n100次订单，共采样 {} 次", sample_count);
    println!("预期采样次数: ~{}", 100 / 10); // 假设平均每订单10ns
}
