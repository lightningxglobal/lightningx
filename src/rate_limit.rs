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

        self.tokens = (self.tokens + tokens_to_add)
            .min(self.policy.burst_capacity as f64);
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
        let policy = self.custom_policies
            .get(client_id)
            .copied()
            .unwrap_or(self.default_policy);

        let bucket = self.buckets
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
        let policy = self.custom_policies
            .get(client_id)
            .copied()
            .unwrap_or(self.default_policy);

        let bucket = self.buckets
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
        let policy = self.custom_policies
            .get(client_id)
            .copied()
            .unwrap_or(self.default_policy);

        let bucket = self.buckets
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
