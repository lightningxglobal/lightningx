use std::sync::OnceLock;
/// 时间提供器 - 统一处理系统时间和单调时间
///
/// 注意：在macOS上，SystemTime.now()的精度为微秒(us)，无法达到纳秒(ns)精度。
/// 对于采样间隔计算，使用Instant::elapsed()获得单调时间，精度充足。
/// 对于时间戳生成，使用SystemTime但应意识到精度限制。
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// 全局单调时间参考点（用于采样间隔计算）
static MONOTONIC_START: OnceLock<Instant> = OnceLock::new();

/// 获取单调时间的纳秒值（精度高，用于采样间隔计算）
///
/// 这使用Instant::elapsed()，在所有平台上都能提供足够的精度。
/// 不受系统时钟调整的影响。
pub fn monotonic_nanos() -> u64 {
    let start = MONOTONIC_START.get_or_init(Instant::now);
    start.elapsed().as_nanos() as u64
}

/// 获取系统wall-clock时间的纳秒值（精度受限，用于时间戳）
///
/// 注意：在macOS上，这个精度实际上是微秒(us)，不是纳秒。
/// 不应该用于需要高精度的采样间隔计算。
pub fn wall_clock_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monotonic_nanos_increments() {
        let t1 = monotonic_nanos();
        let t2 = monotonic_nanos();
        assert!(t2 >= t1, "单调时间应该递增或保持不变");
    }

    #[test]
    fn test_monotonic_nanos_never_backwards() {
        for _ in 0..1000 {
            let t1 = monotonic_nanos();
            let t2 = monotonic_nanos();
            assert!(t2 >= t1, "单调时间不应该倒退");
        }
    }

    #[test]
    fn test_wall_clock_nanos_reasonable() {
        let wall = wall_clock_nanos();
        let expected_min = 1_700_000_000_000_000_000u64; // 2023年底之后
        assert!(wall > expected_min, "时间戳应该在合理范围内");
    }
}
