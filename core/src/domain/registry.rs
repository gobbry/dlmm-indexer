use std::collections::HashMap;

use crate::domain::amounts::Decimals;
use crate::domain::ids::{MintAddress, PoolAddress, Slot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolRecord {
    pub address: PoolAddress,
    pub mint_x: MintAddress,
    pub mint_y: MintAddress,
    pub first_seen_slot: Slot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenRecord {
    pub mint: MintAddress,
    pub decimals: Option<Decimals>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolCache(HashMap<PoolAddress, PoolRecord>);

impl PoolCache {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&self, address: &PoolAddress) -> Option<&PoolRecord> {
        self.0.get(address)
    }

    pub fn contains(&self, address: &PoolAddress) -> bool {
        self.0.contains_key(address)
    }

    pub fn insert(&mut self, record: PoolRecord) {
        self.0.insert(record.address, record);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TokenCache(HashMap<MintAddress, TokenRecord>);

impl TokenCache {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&self, mint: &MintAddress) -> Option<&TokenRecord> {
        self.0.get(mint)
    }

    pub fn contains(&self, mint: &MintAddress) -> bool {
        self.0.contains_key(mint)
    }

    pub fn insert(&mut self, record: TokenRecord) {
        self.0.insert(record.mint, record);
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
