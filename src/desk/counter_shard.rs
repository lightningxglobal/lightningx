/// Number of counter/account shards in the current deployment model.
///
/// User IDs are PostgreSQL BIGSERIAL values, so they are deterministic and
/// dense. Routing by modulo keeps owner lookup O(1) without a database or
/// account-directory lookup on the order hot path.
pub const COUNTER_SHARD_COUNT: u16 = 16;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_users_across_16_counter_shards() {
        assert_eq!(COUNTER_SHARD_COUNT, 16);
        assert_eq!(owner_shard_for_user_id(1), 1);
        assert_eq!(owner_shard_for_user_id(15), 15);
        assert_eq!(owner_shard_for_user_id(16), 0);
        assert_eq!(owner_shard_for_user_id(17), 1);
    }

    #[test]
    fn invalid_user_ids_route_to_zero() {
        assert_eq!(owner_shard_for_user_id(0), 0);
        assert_eq!(owner_shard_for_user_id(-1), 0);
    }

    #[test]
    fn desk_id_wraps_to_counter_shard() {
        assert_eq!(counter_shard_for_desk_id(0), 0);
        assert_eq!(counter_shard_for_desk_id(15), 15);
        assert_eq!(counter_shard_for_desk_id(16), 0);
        assert!(is_user_owned_by_desk(17, 1));
        assert!(is_user_owned_by_desk(17, 17));
    }
}
