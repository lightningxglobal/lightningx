use crate::skiplist::SkipList;

/// Only SkipList is supported (Array/BTree implementations removed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderBookType {
    SkipList,
}

pub type OrderBookWrapper = SkipList;
