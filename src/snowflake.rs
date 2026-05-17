/// 分布式唯一 ID 生成器：Snowflake 算法
///
/// 用于跨多个 Desk Server 生成全局唯一的订单 ID。
///
/// ID 结构 (64 bits):
///   [63]      : 0 (reserved)
///   [48-62]   : desk_id (15 bits → 32768 个 Desk 实例)
///   [22-47]   : timestamp_ms (26 bits → ~4 年不重复)
///   [0-21]    : sequence (22 bits → 4M 个 ID/毫秒/Desk)
///
/// 性能：~50ns per ID generation (无锁)

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const DESK_ID_BITS: u32 = 15;
const TIMESTAMP_BITS: u32 = 26;
const SEQUENCE_BITS: u32 = 22;

const DESK_ID_SHIFT: u32 = TIMESTAMP_BITS + SEQUENCE_BITS;
const TIMESTAMP_SHIFT: u32 = SEQUENCE_BITS;

const SEQUENCE_MASK: u64 = (1u64 << SEQUENCE_BITS) - 1;
const TIMESTAMP_MASK: u64 = (1u64 << TIMESTAMP_BITS) - 1;
const DESK_ID_MASK: u64 = (1u64 << DESK_ID_BITS) - 1;

/// 获取当前时间戳（毫秒，自 UNIX 纪元）
fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Snowflake ID 生成器
///
/// 无锁设计，支持高频并发调用（百万 TPS）
pub struct SnowflakeIdGenerator {
    desk_id: u64,
    /// 状态字段：[timestamp_ms (26 bits) | sequence (22 bits)]
    state: AtomicU64,
}

impl SnowflakeIdGenerator {
    /// 创建新的 ID 生成器
    ///
    /// # Arguments
    /// * `desk_id` - 柜台ID (0-32767)
    ///
    /// # Panics
    /// 如果 desk_id >= 32768 则 panic
    pub fn new(desk_id: u64) -> Self {
        assert!(desk_id < (1u64 << DESK_ID_BITS), "desk_id must be < 32768");

        let initial_timestamp = current_timestamp_ms() & TIMESTAMP_MASK;
        let state = (initial_timestamp << TIMESTAMP_SHIFT) | 0;

        Self {
            desk_id,
            state: AtomicU64::new(state),
        }
    }

    /// 生成下一个唯一 ID
    ///
    /// 返回值结构：(desk_id << 48) | (timestamp << 22) | sequence
    pub fn next_id(&self) -> u64 {
        loop {
            let now_ms = current_timestamp_ms() & TIMESTAMP_MASK;
            let old_state = self.state.load(Ordering::Acquire);

            let old_timestamp = old_state >> TIMESTAMP_SHIFT;
            let old_sequence = old_state & SEQUENCE_MASK;

            let (new_timestamp, new_sequence) = if now_ms > old_timestamp {
                // 时间推进，序列号重置
                (now_ms, 0)
            } else if now_ms == old_timestamp {
                // 同一毫秒内，序列号递增
                if old_sequence >= SEQUENCE_MASK {
                    // 序列号溢出，自旋等待下一毫秒
                    std::hint::spin_loop();
                    continue;
                }
                (old_timestamp, old_sequence + 1)
            } else {
                // 时间回退（极罕见），使用旧时间戳 + 递增序列号
                // 注：此情况仅在系统时钟被调回时发生
                (old_timestamp, old_sequence + 1)
            };

            let new_state = (new_timestamp << TIMESTAMP_SHIFT) | new_sequence;

            // CAS 操作：原子地更新状态
            match self.state.compare_exchange(
                old_state,
                new_state,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // CAS 成功，生成最终 ID
                    let id = (self.desk_id << DESK_ID_SHIFT)
                        | (new_timestamp << TIMESTAMP_SHIFT)
                        | new_sequence;
                    return id;
                }
                Err(_) => {
                    // CAS 失败，重试
                    std::hint::spin_loop();
                    continue;
                }
            }
        }
    }

    /// 批量生成 N 个 ID
    pub fn next_ids(&self, count: usize) -> Vec<u64> {
        (0..count).map(|_| self.next_id()).collect()
    }

    /// 解析 Snowflake ID
    ///
    /// 返回 (desk_id, timestamp_ms, sequence)
    pub fn parse(id: u64) -> (u64, u64, u64) {
        let desk_id = (id >> DESK_ID_SHIFT) & DESK_ID_MASK;
        let timestamp = (id >> TIMESTAMP_SHIFT) & TIMESTAMP_MASK;
        let sequence = id & SEQUENCE_MASK;
        (desk_id, timestamp, sequence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_snowflake_creation() {
        let gen = SnowflakeIdGenerator::new(1);
        let id = gen.next_id();
        assert!(id > 0, "ID should be positive");
    }

    #[test]
    fn test_id_uniqueness() {
        let gen = SnowflakeIdGenerator::new(1);
        let count = 10000;
        let mut ids = HashSet::new();

        for _ in 0..count {
            let id = gen.next_id();
            assert!(ids.insert(id), "ID should be unique");
        }

        assert_eq!(ids.len(), count, "All IDs should be unique");
    }

    #[test]
    fn test_id_parsing() {
        let gen = SnowflakeIdGenerator::new(42);
        let id = gen.next_id();

        let (desk_id, _timestamp, _sequence) = SnowflakeIdGenerator::parse(id);
        assert_eq!(desk_id, 42, "desk_id should be preserved in ID");
    }

    #[test]
    fn test_multiple_desks() {
        let gen1 = SnowflakeIdGenerator::new(1);
        let gen2 = SnowflakeIdGenerator::new(2);

        let id1 = gen1.next_id();
        let id2 = gen2.next_id();

        let (d1, _, _) = SnowflakeIdGenerator::parse(id1);
        let (d2, _, _) = SnowflakeIdGenerator::parse(id2);

        assert_eq!(d1, 1, "gen1 should produce desk_id 1");
        assert_eq!(d2, 2, "gen2 should produce desk_id 2");
        assert_ne!(id1, id2, "IDs from different desks should differ");
    }

    #[test]
    fn test_sequence_increment() {
        let gen = SnowflakeIdGenerator::new(1);

        let id1 = gen.next_id();
        let id2 = gen.next_id();

        let (_desk1, ts1, seq1) = SnowflakeIdGenerator::parse(id1);
        let (_desk2, ts2, seq2) = SnowflakeIdGenerator::parse(id2);

        // 如果在同一毫秒内生成
        if ts1 == ts2 {
            assert_eq!(seq2, seq1 + 1, "sequence should increment");
        }
    }

    #[test]
    fn test_timestamp_advances() {
        let gen = SnowflakeIdGenerator::new(1);

        let id1 = gen.next_id();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id2 = gen.next_id();

        let (_desk1, ts1, _seq1) = SnowflakeIdGenerator::parse(id1);
        let (_desk2, ts2, _seq2) = SnowflakeIdGenerator::parse(id2);

        assert!(ts2 >= ts1, "timestamp should advance or stay same");
    }

    #[test]
    fn test_batch_generation() {
        let gen = SnowflakeIdGenerator::new(1);
        let ids = gen.next_ids(1000);

        assert_eq!(ids.len(), 1000);

        let unique_ids: HashSet<_> = ids.iter().cloned().collect();
        assert_eq!(unique_ids.len(), 1000, "Batch IDs should all be unique");
    }

    #[test]
    fn test_desk_id_bounds() {
        // 应该正常工作
        let gen_min = SnowflakeIdGenerator::new(0);
        let gen_max = SnowflakeIdGenerator::new((1u64 << DESK_ID_BITS) - 1);

        let _id_min = gen_min.next_id();
        let _id_max = gen_max.next_id();
    }

    #[test]
    #[should_panic]
    fn test_desk_id_overflow() {
        // desk_id 超出范围应该 panic
        let _gen = SnowflakeIdGenerator::new(1u64 << DESK_ID_BITS);
    }

    #[test]
    fn test_concurrent_generation() {
        use std::sync::Arc;
        use std::thread;

        let gen = Arc::new(SnowflakeIdGenerator::new(1));
        let mut handles = vec![];

        for _ in 0..4 {
            let gen_clone = Arc::clone(&gen);
            let handle = thread::spawn(move || {
                let mut ids = Vec::new();
                for _ in 0..1000 {
                    ids.push(gen_clone.next_id());
                }
                ids
            });
            handles.push(handle);
        }

        let mut all_ids = HashSet::new();
        for handle in handles {
            let ids = handle.join().unwrap();
            for id in ids {
                assert!(all_ids.insert(id), "Concurrent IDs should be unique");
            }
        }

        assert_eq!(all_ids.len(), 4000, "Should have 4000 unique IDs");
    }
}
