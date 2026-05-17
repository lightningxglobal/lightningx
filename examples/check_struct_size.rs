use matching_engine::*;
use std::mem::{size_of, align_of};

fn main() {
    println!("MatchingEngine size: {} bytes", size_of::<MatchingEngine>());
    println!("MatchingEngine align: {} bytes\n", align_of::<MatchingEngine>());
    
    println!("SmallVec<[u64; 64]> size: {}", size_of::<smallvec::SmallVec<[u64; 64]>>());
    println!("SmallVec<[usize; 64]> size: {}", size_of::<smallvec::SmallVec<[usize; 64]>>());
    println!("SmallVec<[TradeEvent; 128]> size: {}", size_of::<smallvec::SmallVec<[TradeEvent; 128]>>());
    println!("TradeEvent size: {}", size_of::<TradeEvent>());
}
