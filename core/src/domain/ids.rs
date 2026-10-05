use std::fmt;
use std::str::FromStr;

use crate::domain::error::AddressParseError;

const BASE58_ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
// A 64-byte value in base58 needs at most 88 characters; the bound keeps the loop finite.
const BASE58_TEXT_LENGTH_MAX: usize = 88;

const fn base58_digit(character: u8) -> u32 {
    let mut position = 0;
    while position < BASE58_ALPHABET.len() {
        if BASE58_ALPHABET[position] == character {
            return position as u32;
        }
        position += 1;
    }
    panic!("invalid base58 character");
}

// bs58 is runtime-only; constants decode here so they can be written as base58 text.
const fn base58_decode_const<const LENGTH: usize>(text: &str) -> [u8; LENGTH] {
    let characters = text.as_bytes();
    assert!(
        characters.len() <= BASE58_TEXT_LENGTH_MAX,
        "base58 text too long"
    );
    let mut bytes = [0u8; LENGTH];
    let mut character_position = 0;
    while character_position < characters.len() {
        let mut carry = base58_digit(characters[character_position]);
        let mut byte_position = LENGTH;
        while byte_position > 0 {
            byte_position -= 1;
            carry += bytes[byte_position] as u32 * 58;
            bytes[byte_position] = (carry & 0xff) as u8;
            carry >>= 8;
        }
        assert!(carry == 0, "base58 value longer than the key");
        character_position += 1;
    }
    // Each leading '1' encodes one leading zero byte, so a mismatch means the decoded value
    // would be shorter or longer than LENGTH bytes.
    let mut leading_one_count = 0;
    while leading_one_count < characters.len() && characters[leading_one_count] == b'1' {
        leading_one_count += 1;
    }
    let mut leading_zero_count = 0;
    while leading_zero_count < LENGTH && bytes[leading_zero_count] == 0 {
        leading_zero_count += 1;
    }
    assert!(
        leading_one_count == leading_zero_count,
        "base58 text does not decode to the key length"
    );
    bytes
}

// An Anchor discriminator: the first 8 bytes of sha256("<namespace>:<name>").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Discriminator([u8; Discriminator::LENGTH_BYTES]);

impl Discriminator {
    pub const LENGTH_BYTES: usize = 8;

    pub const fn new(bytes: [u8; Self::LENGTH_BYTES]) -> Self {
        Self(bytes)
    }

    // For constants: written as the hex a block explorer shows, checked at compile time.
    pub const fn hex(text: &str) -> Self {
        let digits = text.as_bytes();
        assert!(
            digits.len() == Self::LENGTH_BYTES * 2,
            "discriminator hex must be 16 digits"
        );
        let mut bytes = [0u8; Self::LENGTH_BYTES];
        let mut byte_position = 0;
        while byte_position < Self::LENGTH_BYTES {
            let high = hex_digit(digits[byte_position * 2]);
            let low = hex_digit(digits[byte_position * 2 + 1]);
            bytes[byte_position] = (high << 4) | low;
            byte_position += 1;
        }
        Self(bytes)
    }

    pub const fn get(self) -> [u8; Self::LENGTH_BYTES] {
        self.0
    }

    pub const fn as_bytes(&self) -> &[u8; Self::LENGTH_BYTES] {
        &self.0
    }
}

impl PartialEq<[u8; Discriminator::LENGTH_BYTES]> for Discriminator {
    fn eq(&self, other: &[u8; Discriminator::LENGTH_BYTES]) -> bool {
        self.0 == *other
    }
}

const fn hex_digit(character: u8) -> u8 {
    match character {
        b'0'..=b'9' => character - b'0',
        b'a'..=b'f' => character - b'a' + 10,
        b'A'..=b'F' => character - b'A' + 10,
        _ => panic!("invalid hex digit"),
    }
}

// Signatures and addresses share one shape: fixed bytes on the wire, base58 for humans.
macro_rules! base58_bytes_newtype {
    ($name:ident, $length:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name([u8; $length]);

        impl $name {
            pub const LENGTH_BYTES: usize = $length;

            pub const fn new(bytes: [u8; $length]) -> Self {
                Self(bytes)
            }

            // For constants: an invalid key is a compile error at its definition site.
            pub const fn from_base58(text: &str) -> Self {
                Self(base58_decode_const::<$length>(text))
            }

            pub const fn get(self) -> [u8; $length] {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&bs58::encode(self.0).into_string())
            }
        }

        impl FromStr for $name {
            type Err = AddressParseError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                let decoded = bs58::decode(text)
                    .into_vec()
                    .map_err(|_| AddressParseError::InvalidBase58)?;
                let bytes: [u8; $length] =
                    decoded
                        .try_into()
                        .map_err(|rejected: Vec<u8>| AddressParseError::Length {
                            expected: $length,
                            actual: rejected.len(),
                        })?;
                Ok(Self(bytes))
            }
        }
    };
}

base58_bytes_newtype!(Signature, 64);
base58_bytes_newtype!(AccountAddress, 32);
base58_bytes_newtype!(PoolAddress, 32);
base58_bytes_newtype!(MintAddress, 32);
base58_bytes_newtype!(UserAddress, 32);

macro_rules! scalar_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name($inner);

        impl $name {
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            pub const fn get(self) -> $inner {
                self.0
            }
        }
    };
}

scalar_newtype!(Slot, u64);
scalar_newtype!(UnixSeconds, i64);
scalar_newtype!(SwapOrdinal, u16);
scalar_newtype!(StackHeight, u8);
scalar_newtype!(TransactionIndex, u16);
scalar_newtype!(JobId, i64);
scalar_newtype!(BinId, i32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotRange {
    pub start: Slot,
    pub end_inclusive: Slot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MinuteRange {
    pub start: UnixSeconds,
    pub end_inclusive: UnixSeconds,
}

#[cfg(test)]
mod tests {
    use super::*;

    // from_base58 is the compile-time twin of FromStr: same bytes, same Display text.
    #[test]
    fn from_base58_matches_bytes_and_round_trips() {
        const DLMM_PROGRAM_TEXT: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";
        let decoded = AccountAddress::from_base58(DLMM_PROGRAM_TEXT);
        let expected = AccountAddress::new([
            4, 233, 225, 47, 188, 132, 232, 38, 201, 50, 204, 233, 226, 100, 12, 206, 21, 89, 12,
            28, 98, 115, 176, 146, 87, 8, 186, 59, 133, 32, 176, 188,
        ]);
        assert_eq!(decoded, expected);
        assert_eq!(decoded.to_string(), DLMM_PROGRAM_TEXT);
        assert_eq!(DLMM_PROGRAM_TEXT.parse::<AccountAddress>(), Ok(decoded));
    }

    // Leading '1's are leading zero bytes, the edge where hand-rolled base58 usually breaks.
    #[test]
    fn from_base58_keeps_leading_zero_bytes() {
        const SYSTEM_PROGRAM_TEXT: &str = "11111111111111111111111111111111";
        let decoded = AccountAddress::from_base58(SYSTEM_PROGRAM_TEXT);
        assert_eq!(decoded, AccountAddress::new([0; 32]));
        assert_eq!(decoded.to_string(), SYSTEM_PROGRAM_TEXT);
    }

    // A 64-byte value exercises the other key length through the same decoder.
    #[test]
    fn from_base58_agrees_with_bs58_for_signatures() {
        let signature = Signature::new(std::array::from_fn(|position| position as u8));
        let text = signature.to_string();
        assert_eq!(Signature::from_base58(&text), signature);
    }
}
