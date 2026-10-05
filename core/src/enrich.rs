use std::collections::HashSet;

use crate::domain::amounts::{QuoteAllowlist, QuoteAsset, QuoteLeg, TokenAmountRaw};
use crate::domain::block::{DecodedBlock, EnrichedBlock};
use crate::domain::ids::{MintAddress, PoolAddress};
use crate::domain::registry::{PoolCache, PoolRecord, TokenCache};
use crate::domain::swap::{DecodedSwap, EnrichedSwap, SwapDirection};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PoolSide {
    X,
    Y,
}

pub fn enrich(
    decoded: DecodedBlock,
    pool_cache: &PoolCache,
    token_cache: &TokenCache,
    allowlist: &QuoteAllowlist,
) -> EnrichedBlock {
    let new_pools = new_pools(&decoded, pool_cache);
    let unknown_mints = unknown_mints(&decoded, token_cache);
    let swap_count = decoded.swaps.len();
    let swaps: Vec<EnrichedSwap> = decoded
        .swaps
        .into_iter()
        .map(|swap| enrich_swap(swap, allowlist))
        .collect();
    debug_assert_eq!(swaps.len(), swap_count);
    debug_assert!(new_pools.len() <= swap_count);
    EnrichedBlock {
        slot: decoded.slot,
        parent_slot: decoded.parent_slot,
        block_time: decoded.block_time,
        swaps,
        failures: decoded.failures,
        new_pools,
        unknown_mints,
    }
}

fn enrich_swap(swap: DecodedSwap, allowlist: &QuoteAllowlist) -> EnrichedSwap {
    let (mint_in, mint_out) = match swap.event.direction {
        SwapDirection::XToY => (swap.mint_x, swap.mint_y),
        SwapDirection::YToX => (swap.mint_y, swap.mint_x),
    };
    debug_assert_ne!(mint_in, mint_out);
    let quote_leg = quote_leg(&swap, allowlist);
    EnrichedSwap {
        decoded: swap,
        mint_in,
        mint_out,
        quote_leg,
    }
}

// First sighting in block order, so first_seen_slot and insert order are deterministic.
fn new_pools(decoded: &DecodedBlock, pool_cache: &PoolCache) -> Vec<PoolRecord> {
    let mut seen: HashSet<PoolAddress> = HashSet::new();
    decoded
        .swaps
        .iter()
        .filter(|swap| !pool_cache.contains(&swap.pool) && seen.insert(swap.pool))
        .map(|swap| PoolRecord {
            address: swap.pool,
            mint_x: swap.mint_x,
            mint_y: swap.mint_y,
            first_seen_slot: decoded.slot,
        })
        .collect()
}

fn unknown_mints(decoded: &DecodedBlock, token_cache: &TokenCache) -> Vec<MintAddress> {
    let mut seen: HashSet<MintAddress> = HashSet::new();
    decoded
        .swaps
        .iter()
        .flat_map(|swap| [swap.mint_x, swap.mint_y])
        .filter(|mint| !token_cache.contains(mint) && seen.insert(*mint))
        .collect()
}

// Exactly one allowlisted side is the quote; SOL-USDC picks the stable; neither is unpriced.
pub fn quote_leg(swap: &DecodedSwap, allowlist: &QuoteAllowlist) -> Option<QuoteLeg> {
    let asset_x = allowlist.asset_of(swap.mint_x);
    let asset_y = allowlist.asset_of(swap.mint_y);
    let (side, asset) = match (asset_x, asset_y) {
        (Some(x), Some(y)) if quote_rank(x) <= quote_rank(y) => (PoolSide::X, x),
        (Some(_), Some(y)) => (PoolSide::Y, y),
        (Some(x), None) => (PoolSide::X, x),
        (None, Some(y)) => (PoolSide::Y, y),
        (None, None) => return None,
    };
    Some(QuoteLeg {
        asset,
        amount: side_amount(swap, side),
        decimals: asset.decimals(),
    })
}

// Lower ranks are preferred; stables outrank SOL because their USD price barely moves.
const fn quote_rank(asset: QuoteAsset) -> u8 {
    match asset {
        QuoteAsset::Usdc => 0,
        QuoteAsset::Usdt => 1,
        QuoteAsset::Sol => 2,
    }
}

fn side_amount(swap: &DecodedSwap, side: PoolSide) -> TokenAmountRaw {
    let side_is_input = matches!(
        (side, swap.event.direction),
        (PoolSide::X, SwapDirection::XToY) | (PoolSide::Y, SwapDirection::YToX)
    );
    if side_is_input {
        swap.event.amount_in
    } else {
        swap.event.amount_out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::amounts::{Decimals, FeeRate1e9};
    use crate::domain::ids::{BinId, Signature, SwapOrdinal, TransactionIndex, UserAddress};
    use crate::domain::swap::SwapEvent;

    const AMOUNT_IN: u64 = 1_000;
    const AMOUNT_OUT: u64 = 2_000;

    fn other_mint(seed: u8) -> MintAddress {
        MintAddress::new([seed; 32])
    }

    fn swap(mint_x: MintAddress, mint_y: MintAddress, direction: SwapDirection) -> DecodedSwap {
        DecodedSwap {
            signature: Signature::new([1; 64]),
            transaction_index: TransactionIndex::new(0),
            ordinal: SwapOrdinal::new(0),
            pool: PoolAddress::new([2; 32]),
            mint_x,
            mint_y,
            user: UserAddress::new([3; 32]),
            event: SwapEvent {
                lb_pair: PoolAddress::new([2; 32]),
                from: UserAddress::new([3; 32]),
                start_bin_id: BinId::new(0),
                end_bin_id: BinId::new(0),
                amount_in: TokenAmountRaw::new(AMOUNT_IN),
                amount_out: TokenAmountRaw::new(AMOUNT_OUT),
                direction,
                fee: TokenAmountRaw::new(0),
                protocol_fee: TokenAmountRaw::new(0),
                fee_rate_1e9: FeeRate1e9::new(0),
                host_fee: TokenAmountRaw::new(0),
            },
            event2: None,
            swap2_event_payload: None,
        }
    }

    // The four allowlist cases: one side allowlisted (as input or output), both (the stable wins
    // on either side, a stable beats SOL), or neither.
    #[test]
    fn enrich_quote_leg_allowlist_cases() {
        let allowlist = QuoteAllowlist::mainnet();
        let (sol, usdc, usdt) = (allowlist.sol(), allowlist.usdc(), allowlist.usdt());
        let other = other_mint(9);
        // SOL has 9 decimals, both stables 6.
        let leg = |asset, amount, decimals| {
            Some(QuoteLeg {
                asset,
                amount: TokenAmountRaw::new(amount),
                decimals: Decimals::new(decimals),
            })
        };
        let cases = [
            (
                other,
                sol,
                SwapDirection::XToY,
                leg(QuoteAsset::Sol, AMOUNT_OUT, 9),
            ),
            (
                usdt,
                other,
                SwapDirection::XToY,
                leg(QuoteAsset::Usdt, AMOUNT_IN, 6),
            ),
            (
                sol,
                usdc,
                SwapDirection::YToX,
                leg(QuoteAsset::Usdc, AMOUNT_IN, 6),
            ),
            (
                usdc,
                sol,
                SwapDirection::YToX,
                leg(QuoteAsset::Usdc, AMOUNT_OUT, 6),
            ),
            (
                usdt,
                sol,
                SwapDirection::XToY,
                leg(QuoteAsset::Usdt, AMOUNT_IN, 6),
            ),
            (other_mint(8), other, SwapDirection::XToY, None),
        ];
        for (index, (mint_x, mint_y, direction, expected)) in cases.into_iter().enumerate() {
            let actual = quote_leg(&swap(mint_x, mint_y, direction), &allowlist);
            assert_eq!(actual, expected, "case {index}");
        }
    }

    // Pools and mints already cached are not reported again; repeats in a block appear once.
    #[test]
    fn enrich_reports_only_uncached_pools_and_mints_once() {
        let allowlist = QuoteAllowlist::mainnet();
        let known_mint = allowlist.sol();
        let unknown_mint = other_mint(9);
        let first = swap(unknown_mint, known_mint, SwapDirection::XToY);
        let second = swap(unknown_mint, known_mint, SwapDirection::YToX);
        let mut token_cache = TokenCache::new();
        token_cache.insert(crate::domain::registry::TokenRecord {
            mint: known_mint,
            decimals: Some(Decimals::new(9)),
        });
        let decoded = DecodedBlock {
            slot: crate::domain::ids::Slot::new(7),
            parent_slot: crate::domain::ids::Slot::new(6),
            block_time: crate::domain::ids::UnixSeconds::new(0),
            swaps: vec![first, second],
            failures: Vec::new(),
        };
        let enriched = enrich(decoded, &PoolCache::new(), &token_cache, &allowlist);
        assert_eq!(enriched.unknown_mints, vec![unknown_mint]);
        assert_eq!(enriched.new_pools.len(), 1);
        assert_eq!(enriched.new_pools[0].first_seen_slot.get(), 7);
        assert_eq!(enriched.swaps[0].mint_in, unknown_mint);
        assert_eq!(enriched.swaps[1].mint_in, known_mint);
    }
}
