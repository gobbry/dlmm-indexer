---
name: rust-conventions
description: "Project Rust rules for this repo: functional core, Newtype and Typestate always, TigerStyle naming applied to Rust, debug_assert policy, and the adopted idiomatic-rust references. Use whenever writing or reviewing Rust in this repo."
---

# Rust conventions

## Shape

- Functional core, imperative shell. Decisions are pure functions of plain
  data; IO lives in a thin async shell.
- Composition over inheritance. Traits only at real seams (an external data
  source, a price source, a store). No trait-object hierarchies, no builder
  ceremony.

## Types

- **Newtype always.** Every domain value gets its own type (`Signature`,
  `Slot`, `PoolAddress`, `Mint`, `TokenAmount`, `UnixSeconds`). A raw
  `u64`/`String` never crosses a function boundary as a domain value. Derive
  what is needed, no more.
- **Typestate always.** A lifecycle the compiler can enforce is encoded as
  distinct types, with transitions that consume the previous state. Invalid
  transitions do not compile.
- **Enums instead of booleans** for parameters and flags.
- **Immutability by default.** `mut` is a signal to look twice.

## Dependencies

- **No `solana-*` crates.** Pubkeys are `[u8; 32]` behind newtypes, base58 via `bs58`,
  events are parsed by hand from their fixed layouts, RPC JSON goes into our own `serde`
  structs. The rule covers direct dependencies only: `yellowstone-grpc-proto` pulls
  `solana-pubkey` transitively, and that is accepted.

## Assertions

- Preconditions, postconditions, and invariants use `debug_assert!`,
  `debug_assert_eq!`, `debug_assert_ne!`. At least two per non-trivial
  function. Split compound assertions.
- Release builds carry no asserts: the default release profile keeps
  `debug-assertions = false` and nobody changes that.
- Input validation that must hold in production is a `Result`, not an assert.

## Naming

- TigerStyle for everything domain-owned: `snake_case`, no abbreviations
  (`signature` not `sig`, `transaction` not `tx`), units and qualifiers as
  suffixes (`timeout_ms`, `amount_lamports`, `retry_count`, `batch_size_max`),
  related names share a prefix so they sort together.
- Standard-library and trait-mandated names follow Rust (`len`, `iter`, `fmt`,
  `from_str`). Clippy's naming lints win over TigerStyle where they conflict.

## Control flow

- Functions at most 70 lines.
- Push `if`s up and `for`s down.
- Every loop is bounded.
- Every `Result` is handled, or explicitly discarded with a comment saying why.

## Comments

- Only why. No doc comments that restate the signature.
- Tests get a one-line statement of the behavior they protect.

## Adopted idiomatic-rust references

From https://github.com/mre/idiomatic-rust:

- The Ultimate Guide to Rust Newtypes: https://www.howtocodeit.com/guides/ultimate-guide-rust-newtypes
- Pretty State Machine Patterns in Rust (2016): https://hoverbear.org/2016/10/12/rust-state-machine-pattern/
- Rust API Guidelines: https://rust-lang.github.io/api-guidelines/
- Rust Patterns: Enums Instead Of Booleans (2019): https://blakesmith.me/2019/05/07/rust-patterns-enums-instead-of-booleans.html
- Aim For Immutability in Rust (2023): https://corrode.dev/blog/immutability/

Any other idiom from that list or elsewhere: ask the author before adopting it.
