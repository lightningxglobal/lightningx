/// 限流模块：防止客户端滥用
///
/// 功能：
/// - Token Bucket算法实现
/// - 按客户端ID限流
/// - 支持配置每秒限额
/// - 检查和更新配额
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// 获取当前时间戳（秒）
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// 速率限制策略
#[derive(Clone, Copy, Debug)]
pub struct RateLimitPolicy {
    /// 每秒允许的请求数
    pub requests_per_second: u32,
    /// 令牌桶容量（突发处理能力）
    pub burst_capacity: u32,
}

impl RateLimitPolicy {
    /// 创建默认策略：100 req/s, 容量200
    pub fn default_trading() -> Self {
        Self {
            requests_per_second: 100,
            burst_capacity: 200,
        }
    }

    /// 严格策略：10 req/s, 容量20
    pub fn strict() -> Self {
        Self {
            requests_per_second: 10,
            burst_capacity: 20,
        }
    }

    /// 宽松策略：1000 req/s, 容量2000
    pub fn lenient() -> Self {
        Self {
            requests_per_second: 1000,
            burst_capacity: 2000,
        }
    }
}

/// 客户端的令牌桶状态
#[derive(Clone, Debug)]
struct TokenBucket {
    /// 当前令牌数
    tokens: f64,
    /// 上次更新时间
    last_update: u64,
    /// 配置策略
    policy: RateLimitPolicy,
}

impl TokenBucket {
    /// 创建新的令牌桶
    fn new(policy: RateLimitPolicy) -> Self {
        Self {
            tokens: policy.burst_capacity as f64,
            last_update: current_timestamp(),
            policy,
        }
    }

    /// 更新令牌数（基于经过的时间）
    fn refill(&mut self) {
        let now = current_timestamp();
        let elapsed = (now - self.last_update) as f64;
        let tokens_to_add = elapsed * self.policy.requests_per_second as f64;

        self.tokens = (self.tokens + tokens_to_add).min(self.policy.burst_capacity as f64);
        self.last_update = now;
    }

    /// 尝试消费令牌
    fn try_consume(&mut self, tokens: u32) -> bool {
        self.refill();

        if self.tokens >= tokens as f64 {
            self.tokens -= tokens as f64;
            true
        } else {
            false
        }
    }
}

/// 速率限制器
pub struct RateLimiter {
    /// 客户端 -> 令牌桶映射
    buckets: HashMap<String, TokenBucket>,
    /// 默认策略
    default_policy: RateLimitPolicy,
    /// 自定义策略 (client_id -> policy)
    custom_policies: HashMap<String, RateLimitPolicy>,
}

impl RateLimiter {
    /// 创建速率限制器
    pub fn new(default_policy: RateLimitPolicy) -> Self {
        Self {
            buckets: HashMap::new(),
            default_policy,
            custom_policies: HashMap::new(),
        }
    }

    /// 设置自定义策略
    pub fn set_policy(&mut self, client_id: &str, policy: RateLimitPolicy) {
        self.custom_policies.insert(client_id.to_string(), policy);
        // 清除旧的桶，下次访问会重新创建
        self.buckets.remove(client_id);
    }

    /// 检查限流（不消费令牌）
    pub fn check_limit(&mut self, client_id: &str) -> Result<(), String> {
        let policy = self
            .custom_policies
            .get(client_id)
            .copied()
            .unwrap_or(self.default_policy);

        let bucket = self
            .buckets
            .entry(client_id.to_string())
            .or_insert_with(|| TokenBucket::new(policy));

        bucket.refill();

        if bucket.tokens >= 1.0 {
            Ok(())
        } else {
            Err(format!(
                "Rate limit exceeded: {} req/sec (burst: {})",
                policy.requests_per_second, policy.burst_capacity
            ))
        }
    }

    /// 消费令牌（限流）
    pub fn consume(&mut self, client_id: &str, tokens: u32) -> Result<(), String> {
        let policy = self
            .custom_policies
            .get(client_id)
            .copied()
            .unwrap_or(self.default_policy);

        let bucket = self
            .buckets
            .entry(client_id.to_string())
            .or_insert_with(|| TokenBucket::new(policy));

        if bucket.try_consume(tokens) {
            Ok(())
        } else {
            Err(format!(
                "Rate limit exceeded: {} req/sec",
                policy.requests_per_second
            ))
        }
    }

    /// 获取客户端当前令牌数（用于监控）
    pub fn get_tokens(&mut self, client_id: &str) -> f64 {
        let policy = self
            .custom_policies
            .get(client_id)
            .copied()
            .unwrap_or(self.default_policy);

        let bucket = self
            .buckets
            .entry(client_id.to_string())
            .or_insert_with(|| TokenBucket::new(policy));

        bucket.refill();
        bucket.tokens
    }

    /// 重置客户端的令牌桶
    pub fn reset(&mut self, client_id: &str) {
        self.buckets.remove(client_id);
    }

    /// 重置所有客户端
    pub fn reset_all(&mut self) {
        self.buckets.clear();
    }
}

/// Operation class for per-user split buckets: order placement burns
/// matching capacity, cancels are cheap and must stay available even when
/// a user is place-limited (so they can always pull quotes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpClass {
    Place = 0,
    Cancel = 1,
}

/// Connection-independent per-user rate limiter for the order entry paths.
///
/// Buckets are keyed by (user_id, op-class) in a DashMap shared across all
/// WS connections and REST handlers of the process — reconnecting (or
/// opening parallel connections) does NOT mint fresh buckets. One DashMap
/// probe + a few float ops per request; never on the matching path.
///
/// Disabled by default; enable per class with env:
///   WS_RL_PLACE_RPS / WS_RL_PLACE_BURST
///   WS_RL_CANCEL_RPS / WS_RL_CANCEL_BURST
pub struct SharedRateLimiter {
    place: Option<RateLimitPolicy>,
    cancel: Option<RateLimitPolicy>,
    buckets: dashmap::DashMap<(i64, u8), TokenBucket>,
}

impl SharedRateLimiter {
    pub fn new(place: Option<RateLimitPolicy>, cancel: Option<RateLimitPolicy>) -> Self {
        Self {
            place,
            cancel,
            buckets: dashmap::DashMap::new(),
        }
    }

    pub fn from_env() -> Self {
        fn policy(rps_key: &str, burst_key: &str) -> Option<RateLimitPolicy> {
            let rps: u32 = std::env::var(rps_key).ok()?.parse().ok()?;
            if rps == 0 {
                return None;
            }
            let burst: u32 = std::env::var(burst_key)
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(rps * 2);
            Some(RateLimitPolicy {
                requests_per_second: rps,
                burst_capacity: burst.max(1),
            })
        }
        Self::new(
            policy("WS_RL_PLACE_RPS", "WS_RL_PLACE_BURST"),
            policy("WS_RL_CANCEL_RPS", "WS_RL_CANCEL_BURST"),
        )
    }

    pub fn is_enabled(&self) -> bool {
        self.place.is_some() || self.cancel.is_some()
    }

    /// Consume one token for the user's op-class bucket. Ok(()) when the
    /// class is disabled. O(1).
    pub fn try_consume(&self, user_id: i64, op: OpClass) -> Result<(), &'static str> {
        let policy = match op {
            OpClass::Place => self.place,
            OpClass::Cancel => self.cancel,
        };
        let Some(policy) = policy else {
            return Ok(());
        };
        let mut bucket = self
            .buckets
            .entry((user_id, op as u8))
            .or_insert_with(|| TokenBucket::new(policy));
        if bucket.try_consume(1) {
            Ok(())
        } else {
            Err(match op {
                OpClass::Place => "rate limit exceeded (place)",
                OpClass::Cancel => "rate limit exceeded (cancel)",
            })
        }
    }

    /// Snapshot all bucket states for persistence: (user_id, op, tokens,
    /// last_update). Refills first so the snapshot is current.
    pub fn snapshot(&self) -> Vec<(i64, u8, f64, u64)> {
        let mut out = Vec::with_capacity(self.buckets.len());
        for mut entry in self.buckets.iter_mut() {
            entry.value_mut().refill();
            let ((user_id, op), b) = (*entry.key(), entry.value());
            out.push((user_id, op, b.tokens, b.last_update));
        }
        out
    }

    /// Restore bucket states (process restart): tokens are clamped to the
    /// CURRENT policy's capacity and stale timestamps refill naturally on
    /// first use, so a policy change between runs can never grant more
    /// than the new burst.
    pub fn restore(&self, entries: &[(i64, u8, f64, u64)]) {
        for &(user_id, op, tokens, last_update) in entries {
            let policy = match op {
                0 => self.place,
                1 => self.cancel,
                _ => None,
            };
            let Some(policy) = policy else { continue };
            let mut bucket = TokenBucket::new(policy);
            bucket.tokens = tokens.clamp(0.0, policy.burst_capacity as f64);
            bucket.last_update = last_update;
            self.buckets.insert((user_id, op), bucket);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_token_bucket_consumption() {
        let policy = RateLimitPolicy {
            requests_per_second: 10,
            burst_capacity: 20,
        };

        let mut bucket = TokenBucket::new(policy);
        assert_eq!(bucket.tokens as u32, 20); // 初始满载

        // 消费令牌
        assert!(bucket.try_consume(15));
        assert_eq!(bucket.tokens as u32, 5);

        // 尝试消费超过剩余的
        assert!(!bucket.try_consume(10));
    }

    #[test]
    fn test_rate_limiter() {
        let mut limiter = RateLimiter::new(RateLimitPolicy::default_trading());

        // 可以进行多个请求（突发容量200）
        for _ in 0..100 {
            assert!(limiter.consume("client1", 1).is_ok());
        }

        // 但最终会超限
        let mut exceed = false;
        for _ in 0..200 {
            if limiter.consume("client1", 1).is_err() {
                exceed = true;
                break;
            }
        }
        assert!(exceed, "Should hit rate limit eventually");
    }

    #[test]
    fn test_rate_limit_with_refill() {
        let policy = RateLimitPolicy {
            requests_per_second: 10,
            burst_capacity: 10,
        };

        let mut limiter = RateLimiter::new(policy);

        // 消耗所有令牌
        for _ in 0..10 {
            assert!(limiter.consume("client1", 1).is_ok());
        }

        // 应该限流
        assert!(limiter.consume("client1", 1).is_err());

        // 等待令牌补充（1.5秒，预期恢复15个令牌）
        thread::sleep(Duration::from_millis(1500));

        // 应该能再消耗令牌（限流应该解除）
        assert!(limiter.consume("client1", 1).is_ok());
        assert!(limiter.consume("client1", 1).is_ok());
    }

    #[test]
    fn shared_limiter_splits_place_and_cancel_buckets() {
        let limiter = SharedRateLimiter::new(
            Some(RateLimitPolicy { requests_per_second: 10, burst_capacity: 3 }),
            Some(RateLimitPolicy { requests_per_second: 10, burst_capacity: 100 }),
        );
        // Exhaust the place bucket…
        for _ in 0..3 {
            assert!(limiter.try_consume(42, OpClass::Place).is_ok());
        }
        assert!(limiter.try_consume(42, OpClass::Place).is_err());
        // …cancels must still flow (pull-quotes guarantee).
        for _ in 0..50 {
            assert!(limiter.try_consume(42, OpClass::Cancel).is_ok());
        }
        // Other users unaffected.
        assert!(limiter.try_consume(43, OpClass::Place).is_ok());
    }

    #[test]
    fn shared_limiter_disabled_class_always_passes() {
        let limiter = SharedRateLimiter::new(None, None);
        assert!(!limiter.is_enabled());
        for _ in 0..10_000 {
            assert!(limiter.try_consume(1, OpClass::Place).is_ok());
            assert!(limiter.try_consume(1, OpClass::Cancel).is_ok());
        }
    }

    #[test]
    fn shared_limiter_snapshot_restore_roundtrip_clamps() {
        let limiter = SharedRateLimiter::new(
            Some(RateLimitPolicy { requests_per_second: 10, burst_capacity: 10 }),
            None,
        );
        for _ in 0..4 {
            limiter.try_consume(7, OpClass::Place).unwrap();
        }
        let snap = limiter.snapshot();
        assert!(snap.iter().any(|&(uid, op, tokens, _)| uid == 7
            && op == 0
            && (tokens - 6.0).abs() < 1.0));

        // Restore into a fresh limiter: remaining budget carries over —
        // a restart must NOT mint a fresh full bucket.
        let restored = SharedRateLimiter::new(
            Some(RateLimitPolicy { requests_per_second: 10, burst_capacity: 10 }),
            None,
        );
        restored.restore(&snap);
        let mut ok = 0;
        while restored.try_consume(7, OpClass::Place).is_ok() {
            ok += 1;
            assert!(ok < 100, "must exhaust");
        }
        assert!(ok <= 7, "restored bucket must reflect prior consumption, got {ok}");

        // Tokens beyond capacity (policy shrank between runs) are clamped.
        let shrunk = SharedRateLimiter::new(
            Some(RateLimitPolicy { requests_per_second: 1, burst_capacity: 2 }),
            None,
        );
        shrunk.restore(&[(9, 0, 999.0, snap[0].3)]);
        let mut ok = 0;
        while shrunk.try_consume(9, OpClass::Place).is_ok() {
            ok += 1;
            assert!(ok < 100);
        }
        assert!(ok <= 3, "clamped to new capacity, got {ok}");
    }

    #[test]
    fn test_custom_policy() {
        let mut limiter = RateLimiter::new(RateLimitPolicy::default_trading());

        // 为特定客户端设置严格策略
        limiter.set_policy("vip_client", RateLimitPolicy::lenient());

        // vip_client 应该有更高的容量
        let vip_tokens = limiter.get_tokens("vip_client");
        assert_eq!(vip_tokens, 2000.0);

        // 普通客户端使用默认策略
        let normal_tokens = limiter.get_tokens("normal_client");
        assert_eq!(normal_tokens, 200.0);
    }
}
