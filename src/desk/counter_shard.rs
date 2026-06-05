/// Number of counter/account shards in the current compact deployment model.
///
/// User IDs are PostgreSQL BIGSERIAL values, so they are deterministic and
/// dense. Routing by modulo keeps owner lookup O(1) without a database or
/// account-directory lookup on the order hot path.
pub const COUNTER_SHARD_COUNT: u16 = 4;

#[inline]
pub fn owner_shard_for_user_id(user_id: i64) -> u16 {
    if user_id <= 0 {
        return 0;
    }
    (user_id as u64 % COUNTER_SHARD_COUNT as u64) as u16
}

#[inline]
pub fn is_user_owned_by(user_id: i64, counter_shard_id: u16) -> bool {
    owner_shard_for_user_id(user_id) == counter_shard_id
}

#[inline]
pub fn counter_shard_for_desk_id(desk_id: u16) -> u16 {
    desk_id % COUNTER_SHARD_COUNT
}

#[inline]
pub fn is_user_owned_by_desk(user_id: i64, desk_id: u16) -> bool {
    is_user_owned_by(user_id, counter_shard_for_desk_id(desk_id))
}

/// Small-and-focused symbol routing plan:
/// desk 0 owns BTC, desk 1 owns ETH, desks 2/3 split the remaining symbols.
#[inline]
pub fn symbol_route_desk_id(symbol: &str) -> u16 {
    let base = symbol.split_once('_').map(|(base, _)| base).unwrap_or(symbol);
    match base {
        "BTC" => 0,
        "ETH" => 1,
        _ => 2 + (stable_symbol_bit(base) as u16),
    }
}

#[inline]
fn stable_symbol_bit(symbol: &str) -> u8 {
    let mut hash = 0u8;
    for byte in symbol.as_bytes() {
        hash ^= *byte;
    }
    hash & 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_users_across_4_counter_shards() {
        assert_eq!(COUNTER_SHARD_COUNT, 4);
        assert_eq!(owner_shard_for_user_id(1), 1);
        assert_eq!(owner_shard_for_user_id(3), 3);
        assert_eq!(owner_shard_for_user_id(4), 0);
        assert_eq!(owner_shard_for_user_id(5), 1);
    }

    #[test]
    fn invalid_user_ids_route_to_zero() {
        assert_eq!(owner_shard_for_user_id(0), 0);
        assert_eq!(owner_shard_for_user_id(-1), 0);
    }

    #[test]
    fn desk_id_wraps_to_counter_shard() {
        assert_eq!(counter_shard_for_desk_id(0), 0);
        assert_eq!(counter_shard_for_desk_id(3), 3);
        assert_eq!(counter_shard_for_desk_id(4), 0);
        assert!(is_user_owned_by_desk(5, 1));
        assert!(is_user_owned_by_desk(5, 5));
    }

    #[test]
    fn routes_core_symbols_to_dedicated_desks() {
        assert_eq!(symbol_route_desk_id("BTC_USDT"), 0);
        assert_eq!(symbol_route_desk_id("ETH_USDT"), 1);
        let sol = symbol_route_desk_id("SOL_USDT");
        assert!(sol == 2 || sol == 3);
        let xrp = symbol_route_desk_id("XRP_USDT");
        assert!(xrp == 2 || xrp == 3);
    }
}
