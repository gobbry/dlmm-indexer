use rust_decimal::Decimal;

use crate::domain::ids::MintAddress;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TokenAmountRaw(u64);

impl TokenAmountRaw {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Decimals(u8);

impl Decimals {
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u8 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PriceUsd(Decimal);

// The swap's fee rate scaled by 1e9 (the IDL calls it fee_bps); stored on the row, never summed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FeeRate1e9(u128);

impl FeeRate1e9 {
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}

impl PriceUsd {
    pub const fn new(value: Decimal) -> Self {
        Self(value)
    }

    pub const fn get(self) -> Decimal {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum QuoteAsset {
    Sol,
    Usdc,
    Usdt,
}

impl QuoteAsset {
    pub const ALL: [Self; 3] = [Self::Sol, Self::Usdc, Self::Usdt];

    // The ticker stored as asset_symbol and logged. SQL keeps it as text, not an enum, so this
    // enum is the one allowlist and a new quote asset never needs a migration.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sol => "SOL",
            Self::Usdc => "USDC",
            Self::Usdt => "USDT",
        }
    }

    // The three quote mints have fixed decimals, so pricing never waits on a decimals fetch.
    pub const fn decimals(self) -> Decimals {
        match self {
            Self::Sol => Decimals::new(9),
            Self::Usdc => Decimals::new(6),
            Self::Usdt => Decimals::new(6),
        }
    }
}

// Mints, never symbols: a fake "USDT" pool exists on mainnet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteAllowlist {
    sol: MintAddress,
    usdc: MintAddress,
    usdt: MintAddress,
}

impl QuoteAllowlist {
    pub fn new(sol: MintAddress, usdc: MintAddress, usdt: MintAddress) -> Self {
        debug_assert_ne!(sol, usdc);
        debug_assert_ne!(sol, usdt);
        debug_assert_ne!(usdc, usdt);
        Self { sol, usdc, usdt }
    }

    pub fn mainnet() -> Self {
        Self::new(
            MintAddress::from_base58("So11111111111111111111111111111111111111112"),
            MintAddress::from_base58("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),
            MintAddress::from_base58("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"),
        )
    }

    pub const fn sol(&self) -> MintAddress {
        self.sol
    }

    pub const fn usdc(&self) -> MintAddress {
        self.usdc
    }

    pub const fn usdt(&self) -> MintAddress {
        self.usdt
    }

    pub fn asset_of(&self, mint: MintAddress) -> Option<QuoteAsset> {
        if mint == self.sol {
            Some(QuoteAsset::Sol)
        } else if mint == self.usdc {
            Some(QuoteAsset::Usdc)
        } else if mint == self.usdt {
            Some(QuoteAsset::Usdt)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteLeg {
    pub asset: QuoteAsset,
    pub amount: TokenAmountRaw,
    pub decimals: Decimals,
}
