/// 调试Instant精度

use std::time::Instant;

fn main() {
    let start = Instant::now();

    println!("测试Instant.elapsed()精度...\n");

    // 测试1：连续读取
    println!("【连续读取】");
    let mut last = start.elapsed().as_nanos() as u64;
    for i in 0..20 {
        let now = start.elapsed().as_nanos() as u64;
        let diff = now - last;
        if diff > 0 {
            println!("  读取{}: diff={} ns", i, diff);
        }
        last = now;
    }

    // 测试2：在100M个操作中有多少次时间推进
    println!("\n【100M操作中的时间推进次数】");
    let mut last = start.elapsed().as_nanos() as u64;
    let mut push_count = 0;
    let mut total = 0;

    let loop_start = Instant::now();
    while loop_start.elapsed().as_secs_f64() < 0.1 {  // 只运行0.1秒
        total += 1;
        let now = loop_start.elapsed().as_nanos() as u64;
        if now != last {
            push_count += 1;
            last = now;
        }
    }

    println!("  0.1秒内: {} 操作, {} 次时间推进 (比率: {:.2}%)",
             total, push_count, (push_count as f64 / total as f64) * 100.0);

    // 测试3：采样逻辑模拟
    println!("\n【10ms采样间隔模拟】");
    let interval_ns = 10_000_000u64;
    let mut last_sample = 0u64;
    let mut sample_count = 0;

    let test_start = Instant::now();
    while test_start.elapsed().as_secs_f64() < 1.0 {
        let now = test_start.elapsed().as_nanos() as u64;

        if now >= last_sample + interval_ns {
            sample_count += 1;
            if sample_count <= 5 || sample_count % 20 == 0 {
                println!("  采样 #{}: now={} ms", sample_count, now / 1_000_000);
            }
            last_sample = now;
        }
    }

    println!("\n1秒内采样: {} 次 (预期: ~100)", sample_count);
}
